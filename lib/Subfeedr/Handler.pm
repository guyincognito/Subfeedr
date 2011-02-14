package Subfeedr::Handler;
use Moose;
extends 'Tatsumaki::Handler';
__PACKAGE__->asynchronous(1);

use Tatsumaki::Error;
use Tatsumaki::MessageQueue;
use Digest::SHA;
use Try::Tiny;
use URI;
use MIME::Base64;
use Subfeedr::Worker;
use Subfeedr::DataStore;
use Time::HiRes;
use AnyEvent;

my $publish_queue = {};
my $publish_cv = {};

sub get {
    my $self = shift;
    $self->finish("Welcome to subfeedr hub.");
}

sub post {
    my $self = shift;
    my $mode = $self->request->param('hub.mode') || '';
    if ($mode eq 'publish') {
        return $self->publish();
    } elsif ($mode eq 'subscribe' or $mode eq 'unsubscribe') {
        return $self->subscribe();
    } else {
        $self->response->code(400);
        $self->write("hub.mode is invalid");
        return $self->finish('');
    }
}

# from Google's hub
my %valid_ports = map { $_ => 1 } (80, 443, 4443, 8080 .. 8089, 8188, 8440, 8990);

sub is_valid_url {
    my $uri = URI->new(shift);

    return unless $uri->scheme =~ m|^https?$|;
    return unless $valid_ports{$uri->port};
    return if $uri->fragment;

    return 1;
}

sub publish {
    my $self = shift;

    my @urls = $self->request->param('hub.url');
    @urls or Tatsumaki::Error::HTTP->throw(400, "MUST supply at least one hub.url");

    for my $url (@urls) {
        unless (is_valid_url($url)) {
            Tatsumaki::Error::HTTP->throw(400, "hub.url invalid: $url");
        }
    }



    @urls = map URI->new($_)->canonical, @urls;

    for my $url (@urls) {
        my $sha1 = Digest::SHA::sha1_hex($url);
        
        Subfeedr::DataStore->new('known_feed')->sismember('set', $sha1, $self->async_cb(sub {
            return unless $_[0];
            my $pub_time = Time::HiRes::gettimeofday;
            unless ($publish_queue->{$sha1}) {
                $publish_queue->{$sha1} = [];
            }
            push @{$publish_queue->{$sha1}}, $pub_time;
            my $publish_queue_ct = @{$publish_queue->{$sha1}};
            unless ($publish_cv->{$sha1}) {
                #Throttle publish requests to once every 2 seconds
                $publish_cv->{$sha1} = AE::timer 0, 2, sub {
                    my $timer_time = Time::HiRes::gettimeofday;
                    my $idx = 0;
                    foreach my $pub (@{$publish_queue->{$sha1}}) {
                        if ($pub < $timer_time) {
                            ++$idx;
                        } else {
                            last;
                        }
                    }
                    unless ($idx) {
                        return;
                    }
                    splice @{$publish_queue->{$sha1}}, 0, $idx;
                    Tatsumaki::MessageQueue->instance('feed_fetch')->publish($url);
                };

            }
            #Tatsumaki::MessageQueue->instance('feed_fetch')->publish($url);
        }));
    }

    $self->response->code(204);
    $self->finish('');
}

sub subscribe {
    my $self = shift;

    my $input = $self->request->parameters;

    my $callback = $input->{'hub.callback'} || '';
    my $topic    = $input->{'hub.topic'} || '';
    my $token    = $input->{'hub.verify_token'} || '';
    my $secret   = $input->{'hub.secret'} || '';
    my $mode     = $input->{'hub.mode'} || '';
    my $lease_seconds = $input->{'hub.lease_seconds'} || 30 * 24 * 60 * 60;

    unless ($mode) {
        Tatsumaki::Error::HTTP->throw(400, "Invalid hub.mode");
    }
    unless ($callback && is_valid_url($callback)) {
        Tatsumaki::Error::HTTP->throw(400, "Invalid parameter hub.callback");
    }

    unless ($topic && is_valid_url($topic)) {
        Tatsumaki::Error::HTTP->throw(400, "Invalid parameter hub.topic");
    }

    $callback = URI->new($callback)->canonical;
    my $sha1_cb   = Digest::SHA::sha1_hex($callback);
    my $sha1_feed = Digest::SHA::sha1_hex($topic);

    Subfeedr::DataStore->new('subscription')->sismember($sha1_feed, $sha1_cb, $self->async_cb(sub {
        my $existent = shift;
        if ($mode eq 'unsubscribe' && !$existent) {
            #TODO: remove from datastore
            $self->response->code(204);
            return $self->finish('');
        }

        my $challenge = MIME::Base64::encode(join("", map chr(rand(256)), 1..64), "");
        $challenge =~ s/[\W]/X/g;

        my $uri = URI->new($callback);
        $uri->query_form(
            'hub.mode' => $mode,
            'hub.topic' => $topic,
            'hub.challenge' => $challenge,
            'hub.verify_token' => $token,
            'hub.lease_seconds' => $lease_seconds,
        );

        # NOTE only support sync for now
        Tatsumaki::HTTPClient->new->get($uri, $self->async_cb(sub {
            my $res = shift;
            my $time = Time::HiRes::gettimeofday;
            if ($res->is_success && $res->content eq $challenge) {
                Subfeedr::DataStore->new('subscriber')->set($sha1_cb, JSON::encode_json({
                    sha1 => $sha1_cb,
                    callback => $callback->as_string,
                    secret => $secret,
                    verify_token => $token,
                    lease_seconds => $lease_seconds,
                    time_updated => $time,
                }));
                Subfeedr::DataStore->new('subscription')->sadd($sha1_feed, $sha1_cb);
                Subfeedr::DataStore->new('known_feed')->sadd('set', $sha1_feed);
                Subfeedr::DataStore->new('feed')->set($sha1_feed, 
                    JSON::encode_json({ 
                        sha1 => $sha1_feed,
                        url => $topic,
                    }), sub { 
                        my $cv = AE::cv;
                        Tatsumaki::MessageQueue
                            ->instance('feed_fetch')
                            ->publish($topic);
                });

                $self->response->code(204);
                $self->finish('');
            } else {
                $self->response->code(409);
                $self->finish('Error trying to confirm subscription');
            }
        }));
    }));
}

1;
