#!/usr/bin/env perl -w

use strict;
use Test::More;
use lib::abs "../lib";

# Ensure a recent version of Test::Pod::Coverage
eval "use Test::Pod::Coverage 1.08; 1"
	or plan skip_all => "Test::Pod::Coverage 1.08 required for testing POD coverage";
eval "use Pod::Coverage 0.18; 1"
	or plan skip_all => "Pod::Coverage 0.18 required for testing POD coverage";

my $lib = lib::abs::path( "../lib" );
my $blib = lib::abs::path( "../blib" );
#local *Test::Pod::Coverage::_starting_points = sub { -e $blib ? $blib : $lib };
#my @mods = all_modules( lib::abs::path( "../lib" ) );

plan tests => 1;

pod_coverage_ok(
	'AnyEvent::Memcached',
	#{ also_private => [ qr/^(?:accept_connection|eventif|eventcan|handle)$/ ], },
);

exit 0;
require Test::Pod::Coverage; # ;)
