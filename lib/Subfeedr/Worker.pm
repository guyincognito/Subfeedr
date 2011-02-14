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

my $subscriber_timer;
my %etag_cv;
my $subscriber_payload_list = {};
my $etag_list = [];

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

    my $etag = '';
    if (@$etag_list) {
        $etag = $etag_list->[-1]{end_etag}
    }

    my $req = HTTP::Request->new(GET => $url);
    
    #Need to develop start up routine to repopulate the package level variables from the db
    #(We'll have to accept the possibility of posting repeated information if the hub goes down)
    $req->header('If-None-Match' => $etag) if $etag;
    $self->log("Setting If-None-Match to $etag");
    my $http_client = Tatsumaki::HTTPClient->new;
    $http_client->request($req, sub {
        my $res = shift;
        my $sha1 = Digest::SHA::sha1_hex($url);

        unless ($res->code() == 304) {
            try {
                my $time = Time::HiRes::gettimeofday;
                $self->log("Time in work_url is $time");
                my $feed = XML::Feed->parse(\$res->content) or die "Parsing feed ($url) failed";
                my $feed_etag = $res->header('ETag');
                $feed_etag = '' unless $feed_etag;
                #my $key  = $etag . "_" . $feed_etag;
                push @$etag_list, {
                    start_etag => $etag,
                    end_etag => $feed_etag,
                };
                #TODO: Need to keep equivalent values in DB
                if (@$etag_list > 1) {
                    if ($etag_list->[-2]{end_etag} ne $etag_list->[-1]{start_etag}) {
                        $self->log("INTEGRITY ERROR, original etag " . $etag_list->[-2]{end_etag} . " retrieved etag " . $etag_list->[-1]{start_etag});
                        #pop the element off the end of the list and return
                        pop @$etag_list;
                        return;
                    } else {
                        shift @$etag_list;
                    }
                }
                
                my @new;
                for my $entry ($feed->entries) {
                    next unless $entry->id;
                    push @new, $entry;
                }

                my $entry_count = @new;
                $self->log("In work_url, got $entry_count new entries");
                my $payload = $self->post_payload($feed, \@new);

                $self->notify($sha1, $url, $feed, $feed_etag, $etag, $time, $payload, \@new) if @new;
            } catch {
                warn "Fetcher ERROR: $_";
            };
        } else {
            $self->log("Feed not modified");
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
}

sub notify {
    my($self, $sha1, $url, $feed, $feed_etag, $old_etag, $time, $payload, $new_entries) = @_;

    # assume that entries will only contain new updates from the feed
    my $how_many = @$new_entries;
    $self->log("Found $how_many entries for $url If-None-Match:$old_etag and ETag:$feed_etag");

    $self->log("Time in notify is $time");
    my $payload_info = {
        prev_etag => $old_etag,
        etag => $feed_etag,
        payload => $payload,
        set_time => $time,
    };
    $self->log("Time in payload_info->{set_time} is " . $payload_info->{set_time});

    my $payload_info_json = JSON::encode_json($payload_info);
    my $mime_type = $feed->format =~ /RSS/ ? 'application/rss+xml' : 'application/atom+xml';

    Subfeedr::DataStore->new('subscription')->sort($sha1, get => 'subscriber.*', sub {
        my $subs = shift;

        for my $subscriber (map JSON::decode_json($_), @$subs) {
            my $subname = $subscriber->{sha1};
            
            unless ($subscriber_payload_list->{$subname}) {
                $subscriber_payload_list->{$subname} = [];
            }

            #Add payload to package level variable and db
            push @{$subscriber_payload_list->{$subname}}, $payload_info;
            my $pushed_payload_ct = @{$subscriber_payload_list->{$subname}};
            $self->log("After pushing, now have $pushed_payload_ct for $subname\n");

            Subfeedr::DataStore
                ->new('subscriber_payload')
                ->rpush($subname, $payload_info_json);

            #TODO: Need to determine minimum safe interval time
            unless ($subscriber_timer->{$subname}) {
                $subscriber_timer->{$subname} = AE::timer 0, 2, sub {
                    my $time = Time::HiRes::gettimeofday;
                    my $idx = 0;
                    foreach my $payload_info (@{$subscriber_payload_list->{$subname}}) {
                        if ($payload_info->{set_time} < $time) {
                            ++$idx;
                        } else {
                            last;
                        }
                    }
                    unless ($idx) {
                        return;
                    }
                    my $payload_object 
                        = XML::Smart->new($subscriber_payload_list->{$subname}[0]{payload});
                    #TODO: handle RSS format: probably ->{channel}{item}
                    my $entries = $payload_object->{feed}{entry};
                    for my $i (1 .. $idx - 1) {
                        my $next_payload_object 
                            = XML::Smart
                                ->new($subscriber_payload_list->{$subname}[$i]{payload});
                        push @$entries, @{$next_payload_object->{feed}{entry}};
                    }
                    my $entry_ct = @$entries;
                    $self->log("Get $entry_ct entries to post");

                    #TODO: Need to add entry ids to a package level hash to detemrine whether
                    #an entry has already been posted.  If so, return from the
                    #function.  Will also need to expire these ids after some
                    #time
                    my $payload_data = $payload_object->data;
                    my $hmac = Digest::SHA::hmac_sha1_hex(
                        $payload_data, $subscriber->{secret});
                    my $req = HTTP::Request
                        ->new(POST => $subscriber->{callback});
                    $req->content_type($mime_type);
                    $req->header('X-Hub-Signature' => "sha1=$hmac");
                    $req->content_length(length $payload_data);
                    $req->content($payload_data);

                    my $http_client = Tatsumaki::HTTPClient->new;
                    $http_client->request($req, sub {
                        my $res = shift;
                        if ($res->is_error) {
                            #TODO: Implement a backoff routine (say a package
                            #level counter variable that returns from this
                            #function until a certain period of time has passed
                            $self->log("For " . $subscriber->{callback});
                            $self->log($res->status_line . $res->content);
                        } else {
                            #remove payload(s) from package level list
                            splice @{$subscriber_payload_list->{$subname}}, 0, $idx;
                            my $spliced_payload_ct 
                                = @{$subscriber_payload_list->{$subname}};
                            $self->log("After splicing $idx elements, now have $spliced_payload_ct payloads for $subname\n");
                            #Do the equivalent splice in backend
                            my $list_cv = AE::cv;
                            Subfeedr::DataStore
                                ->new('subscriber_payload')
                                ->ltrim($subname, $idx, -1, sub { 
                                $self->log("Removed $idx elements from payload list");
                            });
                            Subfeedr::DataStore
                                ->new('subscriber_payload')
                                ->llen($subname, sub { 
                                my $length = shift;
                                $self->log("Backend payload list has $length elements");
                            });
                        }
                    });
                };
            }
        }
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

sub log {
    my $self = shift;
    my $message = shift;
    open my $fh, ">>/tmp/subfeedr.log";
    print $fh localtime() . ": $message\n";
}

1;
