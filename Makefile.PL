use inc::Module::Install;

name     'AnyEvent-Memcached';
all_from 'lib/AnyEvent/Memcached.pm';
author   'Mons Anderson <mons@cpan.org>';
license  'perl';

build_requires 'Test::More';

auto_install;

WriteAll;

