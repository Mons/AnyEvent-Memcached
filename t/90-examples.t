#!/usr/bin/env perl -w

use strict;
no warnings 'once';
use Test::More;
use lib::abs "../lib";

$ENV{TEST_AUTHOR} or plan skip_all => '$ENV{TEST_AUTHOR} not set';
our $dist = lib::abs::path('..');
eval "use File::Find";
$@ and plan skip_all => "File::Find required for testing POD";

plan tests => 1;

my $found = 0;
my $dir;
opendir $dir, $dist;
while (defined ( $_ = readdir $dir )) {
	$found='d', last if -d "$dist/$_" and /^(bin|scripts?|ex|eg|examples?|samples?|demos?)$/;
	$found='f', last if -f "$dist/$_" and /^(examples?|samples?|demos?)\.p(m|od)$/i;
}
ok($found, 'have example: '.$found.':'.$_);
closedir $dir;
