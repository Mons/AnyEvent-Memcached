#!/usr/bin/env perl

use lib::abs '../..';
use strict;
use AnyEvent;
use AnyEvent::Cache::Memcached;
use R::Dump;
use Data::Dumper;
use Devel::Size 'total_size';
use Cache::Memcached;
#use Devel::Leak::Object 'GLOBAL_bless';
#use Devel::LeakTrace;

$0 = 'memcached test client using anyevent';

my $cv = AnyEvent->condvar;
$SIG{INT} = sub { $cv->send };

sub mem () {
	my $ps = (split /\n/,`ps auxwp $$`)[1];
	my ($mem) = $ps =~ /\w+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+(\d+)/;
	return $mem;
}

our $cmem = 0;
sub measure ($) {
	my $op = shift;
	my $mem = mem;
	my $delta = $mem - $cmem;
	if ($delta != 0) {
		$cmem = $mem;
		warn sprintf "%s: %+d\n",$op,$delta;
	}
}
measure('start');

my @clargs = (
	servers => [ 'localhost:21211', 'localhost:21212' ],

);

my $mc = Cache::Memcached->new( @clargs );
$mc->set(test => 'test ok');
$mc->set(a => "a ok\n");

my $cl = AnyEvent::Cache::Memcached->new( @clargs );
measure('client');
my $maxsize = 0;
my $do_sleep = 0;
my $count = 0;
my $cyr = 0;
my $start = time;

$cl->reg_cb(
	connected => sub {
		warn "Got connected";
		$cl->set(key => 'test', cb => sub {
			warn "set: ".Dump +\@_;
			$cl->replace( key => 'another', cb => sub {
				warn "replace: ".Dump +\@_;
			} )
		});
		$cl->get([qw(test a x y)], cb => sub {
			warn Dump +$_[0];
		});
		$cl->get(qw(test), cb => sub {
			warn "GET = ".Dump +$_[0];
		});
	},
);

$cl->connect;

$cv->recv;
warn "Finishing";
__END__
$cl->reg_cb(
	connected => sub {
		my $t;$t = AnyEvent->timer(after=>10, cb => sub {
			undef $t;
			#$do_sleep = 1;
			#warn "reconnect after "
			#$cl->reconnect;
		});
	},
	entry => sub {
		shift;
		my $h = shift;
		measure('entry');
		$count++;
		my $size = total_size $h;
		my $delta = $size - $maxsize;
		if ($delta > 0) {
			$maxsize = $size;
			warn sprintf "%s maxsize grow by %+d (size=%d)\n",~~localtime, $delta,$size;
		}
		warn Dump + [ $h->{author} ];#$cv->send;
		m{()};
		my $is_cyr = 1,$cyr++ if ($h->{entry}{title}.$h->{entry}{content}) =~ /(\p{IsCyrillic})/;
		warn sprintf "[%s] [%d/%d] [%0.1f/s:%0.1f/s] [%s] %s: %s\n",
			~~localtime, $count, $cyr, $count/((time-$start)||1), $cyr/((time-$start)||1), $is_cyr ? 'Y' : 'N',
			$h->{entry}{poster}{name},$h->{entry}{title};
	},
	time  => sub {
		shift;
		my $time = shift;
		warn "time: $time, delta=".(time-$time)."\n";
		measure('time');
	},
	slow  => sub { shift;warn "slow:". R::Dump (@_); },
	error => sub { shift;warn "error:". R::Dump (@_); },
	parser_error => sub {
		shift;
		my ($data,$err) = @_;
		warn "Parsing failed: $err\n$data";
	}
);
