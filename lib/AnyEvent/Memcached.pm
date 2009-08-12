package AnyEvent::Memcached;

use warnings;
use strict;

=head1 NAME

AnyEvent::Memcached - AnyEvent memcached client

=head1 VERSION

Version 0.01_3

=head1 NOTICE

This is a B<developer release>. Interface is subject to change.

If you want to rely on some features, please, notify me about them

=cut

our $VERSION = '0.01_3';

=head1 SYNOPSIS

    use AnyEvent::Memcached;

    my $memd = AnyEvent::Memcached->new(
        servers => [ "10.0.0.15:11211", "10.0.0.15:11212" ], # same as in Cache::Memcached
        debug   => 1,
        compress_threshold => 10000,
        namespace => 'my-namespace:',

        cv      => $cv, # AnyEvent->condvar: group callback
    );
    
    $memd->set_servers([ "10.0.0.15:11211", "10.0.0.15:11212" ]);
    
    # Basic methods are like in Cache::Memcached, but with additional cb => sub { ... };
    # first argument to cb is return value, second is the error(s)
    
    $memd->set( key => $value, cb => sub {
        shift or warn "Set failed: @_"
    } );

    $memd->get( 'key', cb => sub {
        my ($value,$err) = shift;
        $err and return warn "Get failed: @_";
        warn "Value for key is $value";
    } );

    $memd->mget( [ 'key1', 'key2' ], cb => sub {
        my ($values,$err) = shift;
        $err and return warn "Get failed: @_";
        warn "Value for key1 is $values->{key1} and value for key2 is $values->{key2}"
    } );

    # Additionally there is rget (see memcachedb-1.2.1-beta)

    $memd->rget( 'fromkey', 'tokey', cb => sub {
        my ($value,$err) = shift;
        $err and warn "Get failed: @_";
    } );

=cut

use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;
use AnyEvent::Memcached::Conn;
use base 'Object::Event';
use String::CRC32;
use Storable ();

# flag definitions
use constant F_STORABLE => 1;
use constant F_COMPRESS => 2;

# size savings required before saving compressed value
use constant COMPRESS_SAVINGS => 0.20; # percent

our $HAVE_ZLIB;
BEGIN {
	$HAVE_ZLIB = eval "use Compress::Zlib (); 1;";
}

sub _hash($) {
	return (crc32($_[0]) >> 16) & 0x7fff;
}

=head1 METHODS

=head2 new %args

Currently supported options:

=over 4

=item servers
=item namespace
=item debug
=item cv
=item compress_threshold
=item compress_enable

=back

=cut

sub new {
	my $self = bless {}, shift;
	my %args = @_;
	$self->set_servers(delete $args{servers});
	$self->{namespace} = exists $args{namespace} ? delete $args{namespace} : '';
	for (qw( debug cv compress_threshold compress_enable )) {
		$self->{$_} = exists $args{$_} ? delete $args{$_} : 0;
	}
	require Carp; Carp::carp "@{[ keys %args ]} options are not supported yet" if %args;
	$self;
}

=head2 set_servers

    Setup server list

=cut

sub set_servers {
	my $self = shift;
	my $list = shift;
	$list = [$list] unless ref $list eq 'ARRAY';
	$self->{servers} = $list || [];
	$self->{active}  = 0+@{$self->{servers}};
	$self->{buckets} = undef;
	$self->{bucketcount} = 0;
	$self->_init_buckets;
	@{$self->{buck2sock}} = ();

	$self->{'_single_sock'} = undef;
	if (@{$self->{'servers'}} == 1) {
		$self->{'_single_sock'} = $self->{'servers'}[0];
	}

	return $self;
}

sub _init_buckets {
	my $self = shift;
	return if $self->{buckets};
	my $bu = $self->{buckets} = [];
	foreach my $v (@{$self->{servers}}) {
		my $peer;
		if (ref $v eq "ARRAY") {
			$peer = $v->[0];
			for (1..$v->[1]) { push @$bu, $v->[0]; }
		} else {
			push @$bu, $peer = $v;
		}
		my ($host,$port) = $peer =~ /^(.+?)(?:|:(\d+))$/;
		$self->{peers}{$peer} = {
			host => $host,
			port => $port,
		};
	}
	$self->{bucketcount} = scalar @{$self->{buckets}};
}

=head2 connect

    Establish connection to all servers and invoke event C<connected>, when ready

=cut

sub connect {
	my $self = shift;
	if (@_) {
		#warn "@_; $_[-1]{cv}";
		$_[-1]{cv}->begin if $_[-1]{cv};
		$self->{cv}->begin if $self->{cv};
		push @{ $self->{connqueue} ||= [] }, \@_;
	}
	return if $self->{connecting};
	my $cv = AnyEvent->condvar;
	$self->{connecting} = 1;
	$cv->begin(sub {
		undef $cv;
		$self->{connected} = 1;
		$self->{connecting} = 0;
		$self->event( connected => ());
		for (@{ $self->{connqueue} }) {
			my $method = shift @$_;
			my $args = pop @$_;
			$self->$method(@$_,%$args);
			$args->{cv}->end if $args->{cv};
			$self->{cv}->end if $self->{cv};
			undef $args;
		}
	});
	for my $peer ( values %{ $self->{peers} }) {
		warn "Connecting to $peer->{host}, $peer->{port}" if $self->{debug} > 1;
		$cv->begin;
		AnyEvent::Socket::tcp_connect( $peer->{host}, $peer->{port}, sub {
			if ( my $fh = shift ) {
				warn "$peer->{host}:$peer->{port} connected" if $self->{debug} > 1;
				$peer->{con} = AnyEvent::Memcached::Conn->new( fh => $fh, debug => $self->{debug} );
				$cv->end;
			} else {
				warn "$peer->{host}:$peer->{port} not connected: $!";
				$cv->end;
			}
		});
	}
	$cv->end;
}

sub _handle_errors {
	my $self = shift;
	my $peer = shift;
	local $_ = shift;
	if ($_ eq 'ERROR') {
		warn "Error";
	}
	elsif (/(CLIENT|SERVER)_ERROR (.*)/) {
		warn ucfirst(lc $1)." error: $2";
	}
	else {
		warn "Bad response from $peer->{host}:$peer->{port}: $_";
	}
}

sub _p4k {
	my $self = shift;
	my $key = shift;
	my ($hv, $real_key) = ref $key ?
		(int($key->[0]), $key->[1]) :
		(_hash($key),    $key);
	my $bucket = $hv % $self->{bucketcount};
	return wantarray ? ( $self->{buckets}[$bucket], $real_key ) : $self->{buckets}[$bucket];
}

sub _set {
	my $self = shift;
	my $cmd = shift;
	my $key = shift;
	my $val = shift;
	my %args = @_;
	$self->{connected} or return $self->connect( $cmd => $key,$val,\%args );
	return $args{cb}(undef, "Readonly") if $self->{readonly};
	$_ and $_->begin for $self->{cv}, $args{cv};
	(my $peer,$key) = $self->_p4k($key) or return $args{cb}(undef, "Peer dead???");

	use bytes; # return bytes from length()

	warn "value for memkey:$key is not defined" unless defined $val;
	my $flags = 0;
	if (ref $val) {
		local $Carp::CarpLevel = 2;
		$val = Storable::nfreeze($val);
		$flags |= F_STORABLE;
	}
	my $len = length($val);

	if ( $self->{compress_threshold} and $HAVE_ZLIB
	and $self->{compress_enable} and $len >= $self->{compress_threshold}) {

		my $c_val = Compress::Zlib::memGzip($val);
		my $c_len = length($c_val);

		# do we want to keep it?
		if ($c_len < $len*(1 - COMPRESS_SAVINGS)) {
			$val = $c_val;
			$len = $c_len;
			$flags |= F_COMPRESS;
		}
	}

	my $expire = int($args{expire} || 0);

	$self->{peers}{$peer}{con}->command(
		"$cmd $self->{namespace}$key $flags $expire $len\015\012$val",
		cb => sub {
			local $_ = shift;
			if ($_ eq 'STORED') {
				$args{cb}(1);
			}
			elsif ($_ eq 'NOT_STORED') {
				$args{cb}(0);
			}
			elsif ($_ eq 'EXISTS') {
				$args{cb}(0);
			}
			else {
				$args{cb}(undef,$_);
			}
			$_ and $_->end for $args{cv}, $self->{cv};
		}
	);
	return;
}

=head2 set( $key, $value, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Unconditionally sets a key to a given value in the memcache.

C<$rc> is

=over 4

=item '1'

Successfully stored

=item '0'

Item was not stored

=item undef

Error happens, see C<$err>

=back

=head2 add( $key, $value, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Like C<set>, but only stores in memcache if the key doesn't already exist.

=head2 replace( $key, $value, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Like C<set>, but only stores in memcache if the key already exists. The opposite of add.

=head2 append( $key, $value, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Append the $value to the current value on the server under the $key.

B<append> command first appeared in memcached 1.2.4.

=head2 prepend( $key, $value, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Prepend the $value to the current value on the server under the $key.

B<prepend> command first appeared in memcached 1.2.4.

=cut

sub set     { shift->_set( set => @_) }
sub add     { shift->_set( add => @_) }
sub replace { shift->_set( replace => @_) }
sub append  { shift->_set( append => @_) }
sub prepend { shift->_set( prepend => @_) }

=head2 get( $key, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Retrieve the value for a $key. $key should be a scalar

=head2 mget( $keys : ARRAYREF, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

B<NOT IMPLEMENTED YET>

Retrieve the values for a $keys. 

=head2 get_multi : Alias for mget.

B<NOT IMPLEMENTED YET>

=head2 gets( $keys : ARRAYREF, [cv => $cv], [ expire => $expire ], cb => $cb->( $rc, $err ) )

Retrieve the value and its CAS for a $key. $key should be a scalar.

B<NOT IMPLEMENTED YET>

=cut

sub get {
	my $self = shift;
	my ($cmd) = (caller(0))[3] =~ /([^:]+)$/;
	my $keys = shift;
	my %args = @_;
	$self->{connected} or return $self->connect( $cmd => $keys, \%args );
	my $array;
	if (ref $keys and ref $keys eq 'ARRAY') {
		$array = 1;
	} else {
		$keys = [$keys];
	}
	my $bcount = $self->{bucketcount};

	my %peers;
	for my $key (@$keys) {
		my ($peer,$real_key) = $self->_p4k($key);
		#warn "$peer, $real_key | $self->{peers}{$peer}";
		push @{ $peers{$peer} ||= [] }, $real_key;
	}
	
	my %result;
	my $cv = AnyEvent->condvar;
	$_ and $_->begin for $self->{cv}, $args{cv};
	$cv->begin(sub {
		undef $cv;
		$args{cb}( $array ? \%result :  $result{ $keys->[0]} );
		%result = ();
		$_ and $_->end for $args{cv}, $self->{cv};
	});
	for my $peer (keys %peers) {
		$cv->begin;
		$self->{peers}{$peer}{con}->request( "get ".join(' ',map "$self->{namespace}$_", @{ $peers{$peer} }));
		$self->{peers}{$peer}{con}->reader( id => $peer, res => \%result, namespace => $self->{namespace}, cb => sub {
			$cv->end;
		});
	}
	$cv->end;
	return;
}

=head2 delete( $key, [cv => $cv], [ noreply => 1 ], cb => $cb->( $rc, $err ) )

Delete $key and its value from the cache.

If C<noreply> is true, cb doesn't required

=head2 del

Alias for "delete"

=head2 remove

Alias for "delete"

=cut

sub delete {
	my $self = shift;
	my ($cmd) = (caller(0))[3] =~ /([^:]+)$/;
	my $key = shift;
	my %args = @_;
	$self->{connected} or return $self->connect( $cmd => $key,\%args );
	return $args{cb}(undef, "Readonly") if $self->{readonly};
	(my $peer,$key) = $self->_p4k($key) or return $args{cb}(undef, "Peer dead???");
	my $time = $args{delay} ? " $args{delay}" : '';
	if ($args{noreply}) {
		$self->{peers}{$peer}{con}->request("delete $self->{namespace}$key noreply");
		$args{cb}(1) if $args{cb};
	} else {
		$_ and $_->begin for $self->{cv}, $args{cv};
		$self->{peers}{$peer}{con}->command(
			"delete $self->{namespace}$key$time",
			cb => sub {
				local $_ = shift;
				if ($_ eq 'DELETED') {
					$args{cb}(1);
				} elsif ($_ eq 'NOT_FOUND') {
					$args{cb}(0);
				} else {
					$args{cb}(undef,$_);
				}
				$_ and $_->end for $args{cv}, $self->{cv};
			}
		);
	}
	return;
}
*del   =  \&delete;
*remove = \&delete;

=head2 incr( $key, $increment, [cv => $cv], [ noreply => 1 ], cb => $cb->( $rc, $err ) )

Increment the value for the $key by $delta. Starting with memcached 1.3.3 $key should be set to a number or the command will fail.
Note that the server doesn't check for overflow.

If C<noreply> is true, cb doesn't required, and if passed, simply called with rc = 1

Similar to DBI, zero is returned as "0E0", and evaluates to true in a boolean context.

=head2 decr( $key, $decrement, [cv => $cv], [ noreply => 1 ], cb => $cb->( $rc, $err ) )

Opposite to C<incr>

=cut

sub incr {
	my $self = shift;
	my ($cmd) = (caller(0))[3] =~ /([^:]+)$/;
	my $key = shift;
	my $val = shift;
	my %args = @_;
	$self->{connected} or return $self->connect( $cmd => $key,$val,\%args );
	return $args{cb}(undef, "Readonly") if $self->{readonly};
	(my $peer,$key) = $self->_p4k($key) or return $args{cb}(undef, "Peer dead???");
	if ($args{noreply}) {
		$self->{peers}{$peer}{con}->request("$cmd $self->{namespace}$key $val noreply");
		$args{cb}(1) if $args{cb};
	} else {
		$_ and $_->begin for $self->{cv}, $args{cv};
		$self->{peers}{$peer}{con}->command(
			"$cmd $self->{namespace}$key $val",
			cb => sub {
				local $_ = shift;
				if ($_ eq 'NOT_FOUND') {
					$args{cb}(undef);
				}
				elsif( /^(\d+)$/ ) {
					$args{cb}($1 eq '0' ? '0E0' : $1);
				}
				else {
					$args{cb}(undef,$_);
				}
				$_ and $_->end for $args{cv}, $self->{cv};
			}
		);
	}
	return;
}
*decr = \&incr;

#rget <start key> <end key> <left openness flag> <right openness flag> <max items>\r\n
#
#- <start key> where the query starts.
#- <end key>   where the query ends.
#- <left openness flag> indicates the openness of left side, 0 means the result includes <start key>, while 1 means not.
#- <right openness flag> indicates the openness of right side, 0 means the result includes <end key>, while 1 means not.
#- <max items> how many items at most return, max is 100.

# rget ($from,$till, '+left' => 1, '+right' => 0, max => 10, cb => sub { ... } );

=head2 rget( $from, $till, [ max => 100 ], [ '+left' => 1 ], [ '+right' => 1 ], [cv => $cv], cb => $cb->( $rc, $err ) )

Memcachedb 1.2.1-beta implements rget method, that allows to look through the whole storage

=over 4

=item $from

the starting key

=item $till

finishing key

=item +left

If true, then starting key will be included in results. true by default

=item +right

If true, then finishing key will be included in results. true by default

=item max

Maximum number of results to fetch. 100 is the maximum and is the default

=back

=cut

sub rget {
	my $self = shift;
	#my ($cmd) = (caller(0))[3] =~ /([^:]+)$/;
	my $cmd = 'rget';
	my $from = shift;
	my $till = shift;
	my %args = @_;
	$self->{connected} or return $self->connect( $cmd => $from,$till,\%args );
	my ($lkey,$rkey);
	$lkey = exists $args{'+left'}  ? $args{'+left'}  ? 0 : 1 : 0;
	$rkey = exists $args{'+right'} ? $args{'+right'} ? 0 : 1 : 0;
	$args{max} ||= 100;
	#return $args{cb}(undef, "Readonly") if $self->{readonly};

	my %result;
	my $err;
	my $cv = AnyEvent->condvar;
	$_ and $_->begin for $self->{cv}, $args{cv};
	$cv->begin(sub {
		undef $cv;
		$args{cb}( $err ? (undef,$err) : \%result );
		%result = ();
		$_ and $_->end for $args{cv}, $self->{cv};
	});

	# TODO: peers ?

	for my $peer (keys %{$self->{peers}}) {
		$cv->begin;
		my $do;$do = sub {
			undef $do;
			$self->{peers}{$peer}{con}->request( "$cmd $self->{namespace}$from $self->{namespace}$till $lkey $rkey $args{max}" );
			$self->{peers}{$peer}{con}->reader( id => $peer, res => \%result, namespace => $self->{namespace}, cb => sub {
				#warn "rget from: $peer";
				$cv->end;
			});
		};
		if (exists $self->{peers}{$peer}{rget_ok}) {
			if ($self->{peers}{$peer}{rget_ok}) {
				$do->();
			} else {
				#warn
					$err = "rget not supported on peer $peer";
				$cv->end;
			}
		} else {
			$self->{peers}{$peer}{con}->command( "$cmd 1 0 0 0 1", cb => sub {
				local $_ = shift;
				if ($_ eq 'END') {
					$self->{peers}{$peer}{rget_ok} = 1;
					$do->();
				} else {
					#warn
						$err = "rget not supported on peer $peer";
					$self->{peers}{$peer}{rget_ok} = 0;
					$cv->end;
				}
			} );
			
		}
	}
	$cv->end;
	return;
}

=head1 BUGS

Since there is developer release, there may be a lot of bugs

Feature requests are welcome

Bug reports are welcome

=head1 AUTHOR

Mons Anderson, C<< <mons at cpan.org> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Mons Anderson, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of AnyEvent::Memcached
