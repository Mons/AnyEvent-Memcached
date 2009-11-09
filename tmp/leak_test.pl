#!/usr/bin/env perl

use strict;
use lib::abs '../..', '../../../*/lib';
use AnyEvent;
BEGIN { $ENV{DEBUG_CB} = 1;$ENV{DEBUG_MEM} = 1 }
BEGIN {
	eval { require Devel::FindRef; *findref = \&Devel::FindRef::track;   1 } or *findref  = sub { "No Devel::FindRef installed\n" };
}
use Scalar::Util qw(weaken);

use AnyEvent::Memcached;
use Sub::Name;
use Devel::Leak::Cb;
BEGIN {
	warn "AnyEvent::Memcached  : $AnyEvent::Memcached::VERSION / $INC{'AnyEvent/Memcached.pm'}\n";
	warn "AnyEvent::Connection : $AnyEvent::Connection::VERSION / $INC{'AnyEvent/Connection.pm'}\n";
}

my $cv = AnyEvent->condvar;
$SIG{INT} = sub { $cv->send };
$cv->begin(sub { $cv->send });
my %use = ();
my %get = ();

my $memd = AnyEvent::Memcached->new(
	servers   => [
		'127.0.0.1:11221',
		'127.0.0.1:11222',
	],
	cv        => $cv,
	debug     => 0,
	namespace => "test:",
	timeout   => 1,
);

my $done;
my $key = 'aaaaa';
my $t;$t = AnyEvent->timer(
	interval => 0.01,
	cb => sub {
		$t;
		my $set = $key++;
		$use{$set}++;
		$memd->set($set => 'val1', cb => subname 'cb1.'.$set => cb {
			my $val;
			delete $use{$set};
			#$val = shift and warn("Set $set ok ($val)\n") or warn("Set $set failed: @_"),return;
			$get{$set}++;
			$memd->get($set, cb => subname 'cb2.'.$set => cb {
				delete $get{$set};
				#$val = shift and warn("Get $set ok ($val)\n") or warn("Get $set failed: @_"),return;
			});
		})
	},
);

END {
	weaken(my $check = $memd);
	undef $t;
	undef $memd;
	if ($check) {
		warn "memd not destroyed";
		print findref($check);
		#warn Dump $check;
	}
	#warn Dump \%use,\%get;
}

$cv->recv;
