#!/usr/bin/env bash

MODULE=`perl -ne 'print $1 if m{all_from.+?([\w/.]+)}' Makefile.PL`;
perl=perl
$perl -v

rm -rf MANIFEST.bak MANIFEST Makefile.old && \
echo > '_.tar.gz' && \
pod2text $MODULE > README && \
$perl -i -lpne 's{^\s+$}{};s{^    ((?: {8})+)}{" "x(4+length($1)/2)}se;' README && \
$perl Makefile.PL && \
rm *.tar.gz && \
make manifest && \
$perl -i -lne 'print unless /(?:\.tar\.gz$|^dist|^tmp)/' MANIFEST && \
make clean && \
$perl Makefile.PL && \
make && \
make test && \
make disttest && \
make dist && \
cp -f *.tar.gz dist/ && \
make clean && \
echo "All is OK"
