package DBIx::Easier::ResultSet;

use strict;
use warnings;
use common::sense;

use base qw/ Class::Accessor::Fast /;

use Carp qw( carp croak );
use DBIx::Easier::Row;

__PACKAGE__->mk_accessors(qw/ sth  _table _rows _pk /);
__PACKAGE__->mk_ro_accessors(qw/ sql dbix catalog schema table /);

sub _inflate_row
{
	my ($self, $tuple) = @_;

	return DBIx::Easier::Row->new({
		'dbix'		=> $self->dbix,
		'catalog'	=> $self->catalog,
		'schema'	=> $self->schema,
		'table'		=> $self->table,
		'_table'	=> $self->_table,
		'primary_key'	=> $self->primary_key,
		'tuple'		=> $tuple,
		'resultset'	=> $self,
	});
}

sub quoted_table
{
	my ($self) = @_;

	$self->_table($self->dbix->dbh->quote_identifier($self->catalog, $self->schema, $self->table))
		unless defined $self->_table;

	return $self->_table;
}

sub primary_key
{
	my ($self) = @_;

	unless (defined $self->_pk) {
		$self->_pk([ $self->dbix->dbh->primary_key($self->catalog, $self->schema, $self->table) ]);
	}

        croak "unable to get primary keys for table " . $self->_table
                unless scalar @{$self->_pk} > 0;

	return $self->_pk || [];
}

sub execute
{
	my ($self, $stmt, $bind) = @_;

	$self->sth(undef);
	$self->_rows(undef);

	my $sth = $self->dbix->execute($stmt, $bind)
		or return undef;

	$self->sth($sth);

	return $self;
}

sub select
{
	my ($self, $cols, $where, $order) = @_;

	my ($stmt, @bind) = $self->sql->select(\$self->quoted_table, $cols, $where, $order);

	return $self->execute($stmt, \@bind);
}

sub search
{
	my ($self, $where, $order) = @_;

	return $self->select('*', $where, $order);
}

sub find
{
	my ($self, $where, $order) = @_;

	foreach my $pk (@{$self->primary_key}) {
		croak "primary key \"$pk\" missing in search query"
			unless defined $where->{$pk};
	}

	my $rs = $self->select('*', $where, $order);
	if ($rs->count > 1) {
		carp "too many rows returned for this query, returning first one";	
	}

	if ($rs->count) {
		return $rs->first;
	} else {
		return undef;
	}
}

sub rows
{
	my ($self) = @_;
	unless (defined $self->_rows) {
		$self->_rows($self->sth->fetchall_arrayref({}));
	}

	return $self->_rows;
}

sub fetch
{
	my ($self) = @_;

	my $row = $self->sth->fetchrow_hashref;
	return (defined $row) ? ($self->_inflate_row($row)) : undef;
}

sub count
{
	my ($self) = @_;
	return scalar @{$self->rows};
}

sub all
{
	my ($self) = @_;
	return map { $self->_inflate_row($_) } @{$self->rows};
}

sub first
{
	my ($self) = @_;
	return scalar @{$self->rows} ? $self->_inflate_row($self->rows->[0]) : undef;
}

sub columns
{
	return @{(shift)->sth->{'NAME_lc'}};
}

sub insert
{
	my ($self, $values, $options) = @_;

	my ($stmt, @bind) = $self->sql->insert(\$self->quoted_table, $values, $options);

	my $sth = $self->dbix->execute($stmt, \@bind)
		or return undef;

	$self->sth($sth);

	if (defined $sth->{'NUM_OF_FIELDS'} && $sth->{'NUM_OF_FIELDS'}) {
		$self->_rows($sth->fetchall_arrayref({}));
	}

	return $self;
}

sub last_insert_id
{
	my ($self) = @_;

	return $self->dbix->dbh->last_insert_id($self->catalog, $self->schema, $self->table, undef);
}

sub update
{
	my ($self, $values) = @_;

	return unless $self->rows;

	croak "no primary key for ", $self->table
		unless defined $self->primary_key;

	foreach my $row (@{$self->rows}) {

		my ($stmt, @bind) = $self->sql->update($self->table, $values,
			{ map { $_ => $row->{$_} } @{$self->primary_key} } );

		$self->dbix->execute($stmt, \@bind);
	}

	return 1;
}

sub delete
{
	my ($self) = @_;

	return unless $self->rows;

	croak "no primary key for ", $self->table
		unless defined $self->primary_key;

	foreach my $row (@{$self->rows}) {

		my ($stmt, @bind) = $self->sql->delete($self->table, { map { $_ => $row->{$_} } @{$self->primary_key} } );
		$self->dbix->execute($stmt, \@bind);
	}
}

sub DESTROY
{
	my ($self) = @_;

	$self->sth->finish
		if defined $self->sth;
}

1;
