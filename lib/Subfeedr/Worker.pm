package Subfeedr::Worker;
use Moose;
extends 'Tatsumaki::Service';

use Subfeedr::DataStore;
use Tatsumaki::HTTPClient;
use Tatsumaki::MessageQueue;
use Time::HiRes;
use Try::Tiny;
use AnyEvent;
use JSON;
use XML::Feed;
use XML::Smart;
use Digest::SHA;

our $FeedInterval = $ENV{SUBFEEDR_INTERVAL} || 60 * 15;

my %subscriber_timer;
my %etag_cv;

sub start {
    my $self = shift;

    my $t; $t = AE::timer 0, 15, sub {
        scalar $t;
        my $ds = Subfeedr::DataStore->new('known_feed');
        my $cv = $ds->sort('set', by => 'next_fetch.*', get => 'feed.*', limit => "0 20");
        $cv->cb(sub {
            # Use cv to catch errors ERR: no such key exist
            my $cv = shift;
            try {
                my $feeds = shift;
                for my $feed (map JSON::decode_json($_), @$feeds) {
                    next if $feed->{next_fetch} && $feed->{next_fetch} > time;
                    $self->work_url($feed->{url});
                }
            }
        });
    };

    my $mq = Tatsumaki::MessageQueue->instance('feed_fetch');
    $mq->poll("worker", sub {
        my $url = shift;
        my $sha1 = Digest::SHA::sha1_hex($url);

        $etag_cv{$sha1} = AE::cv;
        $etag_cv{$sha1}->begin ( sub {
            $self->work_url($url);
        });

        Subfeedr::DataStore->new('feed_etag')
            ->hexists($sha1, 'etag', sub {
            my $exists = shift;
            if (!$exists) {
                Subfeedr::DataStore->new('feed_etag')
                    ->hset($sha1, 'etag', '', sub { 
                    $etag_cv{$sha1}->end; 
                });
            } else {
                $etag_cv{$sha1}->end;
            }
        });
    });
}

sub work_url {
    my($self, $url) = @_;
    my $sha1_feed = Digest::SHA::sha1_hex($url);
    my $etag;
    my $etag_modified_cv = AE::cv;

    Subfeedr::DataStore->new('feed_etag')
        ->hget($sha1_feed, 'etag', sub {
        $etag = shift;
        $etag_modified_cv->end;
    });
    
    $etag_modified_cv->begin( sub {
        my $req = HTTP::Request->new(GET => $url);
        $req->header('If-None-Match' => $etag) if $etag;
        my $http_client = Tatsumaki::HTTPClient->new;
        $http_client->request($req, sub {
            my $res = shift;
            my $sha1 = Digest::SHA::sha1_hex($url);

            unless ($res->code() == 304) {

                try {
                    my $feed = XML::Feed->parse(\$res->content) or die "Parsing feed ($url) failed";
                    my $feed_etag = $res->header('ETag');
                    $feed_etag = '' unless $feed_etag;

                    my @new;

                    for my $entry ($feed->entries) {
                        next unless $entry->id;
                        push @new, $entry;
                    }
                    my $time = Time::HiRes::gettimeofday;
                    $self->notify($sha1, $url, $feed, $feed_etag, $time, \@new) if @new;
                } catch {
                    warn "Fetcher ERROR: $_";
                };
            }

            # TODO smart polling
            my $time = Time::HiRes::gettimeofday + $FeedInterval;
            $time += 60 * 60 if $res->is_error;
            warn "Scheduling next poll for $url on $time\n";

            Subfeedr::DataStore->new('next_fetch')->set($sha1, $time);
            Subfeedr::DataStore->new('feed')->set($sha1, JSON::encode_json({
                sha1 => $sha1,
                url  => "$url",
                next_fetch => $time,
            }));
        });
    });
}

sub notify {
    my($self, $sha1, $url, $feed, $feed_etag, $time, $new_entries) = @_;

    # assume that entries will only contain new updates from the feed
    my $how_many = @$new_entries;
    warn "Found $how_many entries for $url\n";
    my $payload = $self->post_payload($feed, $new_entries);
    #Generate payload JSON
    my $payload_json = JSON::encode_json({
        payload => $payload,
        etag => $feed_etag,
        time => $time,
    });
    my $mime_type = $feed->format =~ /RSS/ ? 'application/rss+xml' : 'application/atom+xml';

    Subfeedr::DataStore->new('subscription')->sort($sha1, get => 'subscriber.*', sub {
        my $subs = shift;
        my $etag_cv = AE::cv;

        $etag_cv->begin( sub {
            warn "setting feed etag $feed_etag";
            Subfeedr::DataStore->new('feed_etag')
                ->hset($sha1, 'etag', $feed_etag, sub { 
                Subfeedr::DataStore->new('')->bgsave();
            });
        });

        for my $subscriber (map JSON::decode_json($_), @$subs) {
            $etag_cv->begin;
            my $subname = $subscriber->{sha1};
            my $total_payload;
            my $list_length;

            #remove previous subscriber timer
            undef $subscriber_timer{$subname};

            my $payload_cv = AE::cv;

            #Generate payload to post and add to DB
            Subfeedr::DataStore
                ->new('subscriber_payload')
                ->rpush($subname, $payload_json, sub {
                #Update the time index for the subscriber itself
                $subscriber->{time_updated} = $time;
                Subfeedr::DataStore->new('subscriber')->set($subname, JSON::encode_json($subscriber), sub {
                    Subfeedr::DataStore->new('')->bgsave();
                    $etag_cv->end;
                    Subfeedr::DataStore
                        ->new('subscriber_payload')
                        ->llen($subname, sub {
                        $list_length = shift;
                        --$list_length;
                        Subfeedr::DataStore
                            ->new('subscriber_payload')
                            ->lrange($subname, 0, $list_length, sub {
                                my $payloads = shift;
                                my $payload_json = JSON::decode_json($payloads->[0]);
                                my $payload_object = XML::Smart
                                    ->new($payload_json->{payload});
                                #Assuming ATOM format
                                #TODO: handle RSS format: probably ->{channel}{item}
                                my $entries = $payload_object->{feed}{entry};
                                for my $idx (1 .. $list_length) {
                                    my $next_payload_json = JSON::decode_json($payloads->[$idx]);
                                    my $next_payload_object 
                                        = XML::Smart->new($next_payload_json->{payload});
                                    push @$entries, @{$next_payload_object->{feed}{entry}};
                                }
                                $total_payload = $payload_object->data;
                                $payload_cv->end;
                        }); 
                    });
                });
            });

            #Try to post payload to subscriber
            $payload_cv->begin( sub {
                my $hmac = Digest::SHA::hmac_sha1_hex(
                $total_payload, $subscriber->{secret});
                my $req = HTTP::Request->new(POST => $subscriber->{callback});
                $req->content_type($mime_type);
                $req->header('X-Hub-Signature' => "sha1=$hmac");
                $req->content_length(length $total_payload);
                $req->content($total_payload);

                my $http_client = Tatsumaki::HTTPClient->new;

                #set a timer to post to the subscriber callback.  Try every 30
                #seconds until we can successfully post to it.
                $subscriber_timer{$subname} = AE::timer 0, 30, sub {
                    $http_client->request($req, sub {
                        my $res = shift;
                        if ($res->is_error) {
                            warn $res->status_line;
                            warn $res->content;
                        } else {
                            #cancel the timer and empty the entry list for
                            #this subscriber
                            my $list_cv = AE::cv;
                            $list_cv->begin(sub {
                                undef $subscriber_timer{$subname};
                            });
                            for (0 .. $list_length) {
                                $list_cv->begin;
                                Subfeedr::DataStore
                                    ->new('subscriber_payload')
                                    ->lpop($subname, sub { 
                                    $list_cv->end;
                                });
                            }
                            $list_cv->end;
                        }
                    });
                };
            });
        }
        $etag_cv->end;
    });
}

sub post_payload {
    my($self, $feed, $entries) = @_;

    local $XML::Atom::ForceUnicode = 1;

    # TODO create XML::Feed::Diff or something to do this
    my $format = (split / /, $feed->format)[0];

    my $new = XML::Feed->new($format);
    for my $field (qw( title link description language author copyright modified generator )) {
        my $val = $feed->$field();
        next unless defined $val;
        $new->$field($val);
    }

    for my $entry (@$entries) {
        $new->add_entry($entry->convert($format));
    }

    my $payload = $new->as_xml;
    utf8::encode($payload) if utf8::is_utf8($payload); # Ugh

    return $payload;
}

1;
