use 5.008000;
use ExtUtils::MakeMaker;

WriteMakefile(
	NAME		=> 'DBIx::Easier',
	VERSION_FROM	=> 'lib/DBIx/Easier.pm',
	ABSTRACT	=> 'DBIx made easier',
	AUTHOR		=> 'Alessandro Zummo <a.zummo@towertech.it>',
	LICENSE		=> 'Artistic',
	PREREQ_PM	=> {
				'DBI'			=> 0,
				'SQL::Abstract'		=> 0,
				'Class::Accessor'	=> 0,
				'Class::Accessor::Fast'	=> 0,
				'common::sense'		=> 0,
			},			
);

