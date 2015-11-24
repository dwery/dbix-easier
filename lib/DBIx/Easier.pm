package DBIx::Easier;

use strict;
use warnings;
use common::sense;

use DBI;
use SQL::Abstract;
use Carp;

use DBIx::Easier::ResultSet;

use base qw( Class::Accessor );

__PACKAGE__->mk_accessors(qw/ dbh sql debug catalog schema pk cache_statements /);

our $VERSION = '1.1';

# $DBI::err and $DBI::errstr

sub connect
{
	my ($self, $opts) = @_;

	$self = $self->SUPER::new
		unless ref($self);

	$self->pk({})
		unless defined $self->pk;

	croak "missing dsn"
		unless defined $opts->{'dsn'};

	if ($opts->{'dsn'} =~ /^dbi:Pg/i) {

		carp "please set client_encoding=utf8"
			unless $opts->{'dsn'} =~ /client_encoding=/;

		$opts->{'dsn'} .= ';fallback_application_name=' . $0;
	}

	$self->dbh(DBI->connect($opts->{'dsn'}, $opts->{'user'}, $opts->{'pass'}, $opts->{'attr'}));

	if (defined $self->dbh) {

		$self->sql(SQL::Abstract->new(
			'quote_char'	=> $self->dbh->get_info(29),
			'name_sep'	=> $self->dbh->get_info(41),
		));

		return $self;
	}

	return undef;
}

sub DESTROY
{
	my ($self) = @_;

	$self->disconnect
		if defined $self->dbh;
}

sub execute
{
	my ($self, $stmt, $bind) = @_;

	$self->dump($stmt, $bind)
		if $self->{'debug'};

	my $sth = $self->{'cache_statements'}
			? $self->dbh->prepare_cached($stmt)
			: $self->dbh->prepare($stmt);

	return undef
		unless $sth->execute(@$bind);

	return $sth;
}

sub resultset
{
	my ($self, $table) = @_;

	# fetch primary keys for table
	unless (defined $self->pk->{$table}) {
		$self->pk->{$table} = [ $self->dbh->primary_key($self->catalog, $self->schema, $table) ];
	}

	croak "unable to get primary keys from DBI for " . $table
		unless scalar @{$self->pk->{$table}} > 0;

	return DBIx::Easier::ResultSet->new({
		'dbix'		=> $self,
		'catalog'	=> $self->catalog,
		'schema'	=> $self->schema,
		'table'		=> $table,
		'sql'		=> $self->sql,
		'primary_key'	=> $self->pk->{$table},
	});
}

sub dump
{
	my ($self, $stmt, $bind) = @_;

	print STDERR $stmt, ': ', join(',', map { $_ ? $_ : 'NULL' } @$bind), "\n";
}

sub do { (shift)->dbh->do(@_); }
sub commit { (shift)->dbh->commit; }
sub rollback { (shift)->dbh->rollback; }
sub begin_work { (shift)->dbh->begin_work; }
sub disconnect { (shift)->dbh->disconnect; }
sub error { return (shift)->dbh->errstr; }
sub qerror { return (shift)->dbh->errstr; }

1;
