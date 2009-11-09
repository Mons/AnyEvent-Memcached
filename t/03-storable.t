#!/usr/bin/env perl -w

our $testaddr = $ENV{MEMCACHED_SERVER} || "127.0.0.1:21201"; # Default memcachedb port

use strict;
use lib::abs '../lib';
use Test::More;
use AnyEvent::Impl::Perl;
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Memcached;

my ($host,$port) = split ':',$testaddr;$host ||= '127.0.0.1'; # allow *_SERVER=:port
$testaddr = join ':', $host,$port;

alarm 10;
my $cv = AnyEvent->condvar;
$cv->begin(sub { $cv->send });

$cv->begin;
my $cg;$cg = tcp_connect $host,$port, sub {
	undef $cg;
	@_ or plan skip_all => "No memcached instance running at $testaddr\n";
	diag "testing $testaddr";
	plan tests => 3;

	my $memd = AnyEvent::Memcached->new(
		servers   => [ $testaddr ],
		cv        => $cv,
		debug     => 0,
		namespace => "AE::Memd::t/$$/" . (time() % 100) . "/",
		compress_enable    => 1,
		compress_threshold => 1, # Almost everything is greater than 1
	);

	isa_ok($memd, 'AnyEvent::Memcached');
	# Repeated structures will be compressed
	$memd->set("key1", { some => 'struct'x10 }, cb => sub {
		ok(shift,"set key1") or diag "  Error: @_";
		$memd->get("key1", cb => sub {
			is_deeply(shift, { some => 'struct'x10 }, "get key1") or diag "  Error: @_";
		});
	});
	$cv->end; #connect
}, sub { 1 };

$cv->end;
$cv->recv;
