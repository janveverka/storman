use strict;
use warnings;
package T0::Iterator::Iterator;
use POE;
use T0::Iterator::Rfdir;
use DB_File;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, @_ ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   @_ ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   @_ ); }

#select STDERR; $| = 1;	# make unbuffered
#select STDOUT; $| = 1;	# make unbuffered

# files are keys, entries are
#
#   for fileStatusList :
#     0 for file ready to be inserted
#     1 for already injected file
#
#   for fileSizeList :
#     size of file
#
#   for fileDateList
#     file timestamp in seconds after epoch
#
my %fileStatusList;
my %fileSizeList;
my %fileDateList;

#
# database to make fileStatusList persistent
#
my $database = undef;


sub new {
  my $class = shift;
  my $self = {};
  bless($self, $class);

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;

  $self->ReadConfig();

  $self->{Session} = POE::Session->create(
					  inline_states => {
							    _start => \&start_task,
							    start_rfdir => \&start_rfdir,
							    task_done => \&task_done,
							    config_changed => \&config_changed,
							    inject_file => \&inject_file
							   },
					  args => [ $self ]
					 );

  # if persistent, tie file to hash and clear hash of not injected files
  if ( defined $self->{Persistent} )
    {
      $database = tie(%fileStatusList, 'DB_File', $self->{Persistent}) || die "can't open $self->{Persistent}: $!";

      while ( my ($filename, $status) = each %fileStatusList )
	{
	  if ( defined $self->{Restart} or 0 == $status )
	    {
	      delete($fileStatusList{$filename});
	    }
	}

      $database->sync();
    }

  # add file watcher for config file
  my $watcher = T0::FileWatcher->new(
				     File            => $self->{Config},
				     Client          => $self->{Session},
				     Event           => 'config_changed',
				     Interval        => $self->{ConfigRefresh},
				    );

  return $self;
}

sub start_task {
  my ( $kernel, $heap, $self ) = @_[ KERNEL, HEAP, ARG0 ];

  # save parameters
  $heap->{Self} = $self;
  $heap->{Directory} = $self->{Directory};
  $heap->{MinAge} = $self->{MinAge};
  $heap->{SleepTime} = $self->{SleepTime};
  $heap->{Rate} = $self->{Rate};
  $heap->{MaxFiles} = $self->{MaxFiles};
  $heap->{Interval} = $self->{Interval};

  $heap->{WaitForData} = 1;
  $heap->{WaitForDataInterval} = 1;

  $kernel->yield('start_rfdir');
}

sub start_rfdir {
  my ( $heap, $session ) = @_[ HEAP, SESSION ];

  my %inputhash = (
		   Session => $session,
		   Callback => 'task_done',
		   Directory => $heap->{Directory},
		   MinAge => $heap->{MinAge},
		   Files => {
			     Status => \%fileStatusList,
			     Size => \%fileSizeList,
			     Date => \%fileDateList
			    }
		  );

  my $rfdir = T0::Iterator::Rfdir->new(\%inputhash);
}

sub task_done {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  if ( defined $heap->{SleepTime} and $heap->{SleepTime} > 0 )
    {
      $kernel->delay_set('start_rfdir',$heap->{SleepTime}*60);
    }
  else
    {
      $heap->{WaitForData} = 0;
    }
}

sub config_changed {
  my ( $kernel, $heap, $file ) = @_[ KERNEL, HEAP, ARG0 ];

  if ( $file eq $heap->{Self}->{Config} )
    {
      $heap->{Self}->Quiet("Configuration file \"$file\" has changed.\n");

      T0::Util::ReadConfig( $heap->{Self} );

      $heap->{Directory} = $heap->{Self}->{Directory};
      $heap->{MinAge} = $heap->{Self}->{MinAge};
      $heap->{SleepTime} = $heap->{Self}->{SleepTime};
      $heap->{Rate} = $heap->{Self}->{Rate};
      $heap->{MaxFiles} = $heap->{Self}->{MaxFiles};
      $heap->{Interval} = $heap->{Self}->{Interval};
    }
}

sub ReadConfig {
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;

  T0::Util::ReadConfig( $self );
}

sub inject_file {
  my ( $kernel, $heap, $sender ) = @_[ KERNEL, HEAP, ARG0 ];

  if ( defined $heap->{Maxfiles} and $heap->{Maxfiles} < 1 )
    {
      return 1;
    }

  # loop over files, find first uninjected
  my $file = undef;
  my $size = 0;
  my $date = 0;
  while ( my ($filename, $status) = each %fileStatusList )
    {
      if ( 0 == $status )
	{
	  $fileStatusList{$filename} = 1;
	  if ( defined $database ) { $database->sync(); }

	  $file = $filename;
	  $size = $fileSizeList{$filename};
	  $date = $fileDateList{$filename};
	  last;
	}
    }

  if ( not defined $file )
    {
      # wait the prescribed interval if required
      if ( $heap->{WaitForData} == 1 )
	{
	  $heap->{Self}->Quiet("Wait $heap->{WaitForDataInterval} seconds for data\n");

	  $kernel->delay_set( 'inject_file', $heap->{WaitForDataInterval}, ($sender)  );

	  my $newWaitTime = $heap->{WaitForDataInterval} * 2;

	  # if no SleepTime is defined, make the maximum 1 minute
	  my $longestWait = defined $heap->{SleepTime} ? $heap->{SleepTime}*60 : 60;

	  if ( $newWaitTime > $longestWait )
	    {
	      $heap->{WaitForDataInterval} = $longestWait;
	    }
	  else
	    {
	      $heap->{WaitForDataInterval} = $newWaitTime;
	    }

	  return 1;
	}
      else
	{
	  $heap->{Self}->Quiet("No more data, not waiting anymore\n");
	  return 1;
	}
    }

  # reset time interval
  $heap->{WaitForDataInterval} = 1;

  my $prefix = '';
  if ( defined $heap->{Maxfiles} )
    {
      $heap->{Maxfiles}--;
      if ( $heap->{Maxfiles} > 0 ) { $prefix = "$heap->{Maxfiles}: "; }
    }
  $sender->Quiet("$prefix $file, $size, $date\n");

  my %t = (  $heap->{Self}->{Notify} => $file, Size => $size, Date => $date );
  $sender->Send( \%t );

  if ( defined $heap->{Rate} )
  {
    $heap->{Interval} = $size / (1024*1024) / $heap->{Rate};
    $heap->{Interval} = int( 1000 * $heap->{Interval} ) / 1000;
    $sender->Verbose("Set interval=",$heap->{Interval}," for ",$heap->{Rate}," MB/sec\n");
  }

  if ( defined $heap->{Interval} ) { $kernel->delay_set( 'inject_file', $heap->{Interval}, ($sender) ); }
  else { $kernel->yield( 'inject_file', ($sender) ); }
  return 1;
}

1;
