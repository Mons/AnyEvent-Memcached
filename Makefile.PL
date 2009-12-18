use inc::Module::Install;

name     'AnyEvent-Memcached';
all_from 'lib/AnyEvent/Memcached.pm';
author   'Mons Anderson <mons@cpan.org>';
version   my $v = '0.02';
license  'perl';

build_requires 'Test::More';
build_requires 'Test::If';
build_requires 'Test::NoWarnings';
build_requires 'lib::abs',       '0.90';

requires 'common::sense',        '2';
requires 'Storable';
requires 'Object::Event',        '1.101';
requires 'AnyEvent',             '5.0';
requires 'AnyEvent::Connection', '0.02';
requires 'String::CRC32';
requires 'Devel::Leak::Cb';

provides 'AnyEvent::Memcached' => {
	version => $v,
	file    => 'lib/AnyEvent/Memcached.pm',
};

auto_install;

WriteAll;

