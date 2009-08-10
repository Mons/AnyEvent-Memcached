use inc::Module::Install;

name     'AnyEvent-Memcached';
all_from 'lib/AnyEvent/Memcached.pm';
author   'Mons Anderson <mons@cpan.org>';
license  'perl';

build_requires 'Test::More';
build_requires 'lib::abs', '0.90';

requires 'Object::Event', '1.0';
requires 'AnyEvent',      '4.82';
requires 'String::CRC32';

auto_install;

WriteAll;

