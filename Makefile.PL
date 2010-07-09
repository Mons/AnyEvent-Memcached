use inc::Module::Install;

name     'AnyEvent-Memcached';
author   'Mons Anderson <mons@cpan.org>';
all_from 'lib/AnyEvent/Memcached.pm';
version   my $v = '0.03';
license  'perl';

test_requires 'Test::More';
test_requires 'Test::NoWarnings';
test_requires 'lib::abs', '0.90';

requires 'common::sense',        '2';
requires 'Storable';
requires 'AnyEvent',             '5.0';
requires 'AnyEvent::Connection', '0.05';
requires 'String::CRC32';
#requires 'Devel::Leak::Cb';

#auto_include;
#auto_include_deps;

provides 'AnyEvent::Memcached' => {
	version => $v,
	file    => 'lib/AnyEvent/Memcached.pm',
};

auto_install;

WriteAll;

