package AnyEvent::Memcached::Hash::WithNext;

=head1 DESCRIPTION

Uses the same hashing, as default, but always put key to server, next after choosen. Rusult is twice-replicated data. Useful for usage with memcachdb

=cut

use common::sense 2;m{
use strict;
use warnings;
}x;
use Carp;
use base 'AnyEvent::Memcached::Hash';

sub peers {
	my $self = shift;
	my ($hash,$real,$peers) = @_;
	$peers ||= {};
	my $peer = $self->{buckets}->peer( $hash );
	my $next = $self->{buckets}->next( $peer );
	push @{ $peers->{$peer} ||= [] }, $real;
	push @{ $peers->{$next} ||= [] }, $real;
	return $peers;
}

1;