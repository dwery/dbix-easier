package DBIx::Easier;

use strict;
use warnings;
use common::sense;

use DBI;
use SQL::Abstract;
use Carp;
use Config::Any;

use DBIx::Easier::ResultSet;

use base qw( Class::Accessor );

__PACKAGE__->mk_accessors(qw/ dbh sql debug cache_statements _schema /);
__PACKAGE__->mk_ro_accessors(qw/ catalog schema /);

our $VERSION = '2.0.1';

# $DBI::err and $DBI::errstr

sub connect
{
	my ($self, $opts) = @_;

	$self = $self->SUPER::new
		unless ref($self);

	$self->_schema($opts->{'definition'} || {});

	# cache by default, unless overridden
	$self->cache_statements(1)
		unless defined $self->cache_statements;

	croak "missing dsn"
		unless defined $opts->{'dsn'};

	if (ref($opts->{'dsn'}) eq 'ARRAY') {
		$opts->{'dsn'} = join(';', @{$opts->{'dsn'}});
	}

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

sub connect_with_config
{
	my ($self, $file) = @_;

	my $cfg = Config::Any->load_files({
		'files' => [ $file ],
		'use_ext' => 1,
		'flatten_to_hash' => 1
	});

	croak "cannot load configuration from $file"
		unless defined $cfg
		and defined $cfg->{$file};

	return $self->connect($cfg->{$file});
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

	return DBIx::Easier::ResultSet->new({
		'dbix'		=> $self,
		'catalog'	=> $self->catalog,
		'schema'	=> $self->schema,
		'table'		=> $table,
		'sql'		=> $self->sql,
		'_pk'		=> $self->_schema->{$self->key_for_table($self->catalog, $self->catalog, $table)},
	});
}

sub key_for_table
{
	my ($self, $catalog, $schema, $table) = @_;

	return join(';', $catalog, $schema, $table);
}

sub dump_schema
{
	my ($self, $file) = @_;

	my $sth = $self->dbh->table_info($self->catalog, $self->schema, '', 'TABLE');

	my $dump = {};

	foreach my $table (@{$sth->fetchall_arrayref}) {

		my ($catalog, $schema, $table) = @$table;

		next if $schema eq 'information_schema';

		my $key = $self->key_for_table($catalog, $schema, $table);
		my @pk = $self->dbh->primary_key($self->catalog, $self->schema, $table);

		my $quote = $self->dbh->get_info(29);

		$dump->{$key} = {
			'pk' => [ map { $_ =~ s/$quote//g; $_ } @pk ],
		};
	}

	return $dump;
}

sub load_schema
{
	my ($self, $file) = @_;

	my $schema = Config::Any->load_files({
		'files' => [ $file ],
		'use_ext' => 1,
		'flatten_to_hash' => 1
	});

	croak "cannot load schema from $file"
		unless defined $schema
		and defined $schema->{$file};

	$self->_schema($schema);
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
