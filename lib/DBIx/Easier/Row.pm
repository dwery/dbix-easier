package DBIx::Easier::Row;

use strict;
use warnings;

use vars qw($AUTOLOAD);
use base qw/ Class::Accessor /;

use Carp;

__PACKAGE__->mk_accessors(qw/ dbix catalog schema table primary_key tuple resultset /);

sub get_column
{
	my ($self, $column) = @_;
	return $self->tuple->{$column};
}

sub columns
{
	return keys %{(shift)->tuple};
}

sub update
{
        my ($self, $values) = @_;

	my ($stmt, @bind) = $self->dbix->sql->update(
			$self->table,
			$values,
                        { map { $_ => $self->tuple->{$_} } @{$self->primary_key} }
	);

	$self->dbix->execute($stmt, \@bind);
}

sub delete
{
        my ($self, $where) = @_;

	my ($stmt, @bind) = $self->dbix->sql->delete($self->table, { map { $_ => $self->tuple->{$_} } @{$self->primary_key} } );

	$self->dbix->execute($stmt, \@bind);
}


sub AUTOLOAD
{
	my $self = shift or return undef;
    
	(my $method = $AUTOLOAD) =~ s/.*:://;
	
	if (exists $self->tuple->{$method}) {
		return $self->tuple->{$method};
	}
    
	return undef;
}


1;
