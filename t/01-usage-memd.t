#!/usr/bin/env perl -w

use lib::abs;
our $noreply = 1;
our $testaddr = $ENV{MEMCACHED_SERVER} || "127.0.0.1:11211";
do + lib::abs::path('.').'/check.pl'; $@ and die;
exit;
require Test::NoWarnings;
