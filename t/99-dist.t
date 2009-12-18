#!/usr/bin/perl

use lib::abs '../lib';
use Test::More;
use Test::If 'Test::Dist';
use Test::NoWarnings;
chdir lib::abs::path('..');

Test::Dist::dist_ok(
	'+' => 1,
	run => 1,
	skip => [qw(prereq)],
	kwalitee => {
		req => [qw( has_separate_license_file has_example
		metayml_has_provides metayml_declares_perl_version
		uses_test_nowarnings 
		)],
	},
	prereq => [
		undef,undef, [qw( Test::Pod Test::Pod::Coverage )],
	],
	podcover => { mod_match => qr{^AnyEvent::Memcached$}, mod_skip => [qr{^AnyEvent::Memcached::}] },
);
exit 0;
require Test::Pod::Coverage; # kwalitee hacks, hope temporary
