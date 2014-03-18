use strict;
use warnings;
package T0::Iterator::Iterator;
use POE;
use T0::Logger::Sender;
use T0::FileWatcher;
use T0::Iterator::LFNList;
use T0::Iterator::Rfdir;
use T0::Util;
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
							    start_lfnlist => \&start_lfnlist,
							    start_rfdir => \&start_rfdir,
							    lfnlist_done => \&lfnlist_done,
							    rfdir_done => \&rfdir_done,
							    config_changed => \&config_changed,
							    inject_file => \&inject_file
							   },
					  args => [ $self ]
					 );
  return $self;
}

sub start_task {
  my ( $kernel, $heap, $session, $self ) = @_[ KERNEL, HEAP, SESSION, ARG0 ];

  # save parameters
  $heap->{Self} = $self;
  $heap->{Channel} = $self->{Channel};
  $heap->{Directory} = $self->{Directory};
  $heap->{LFNList} = $self->{LFNList};
  $heap->{MinAge} = $self->{MinAge};
  $heap->{SleepTime} = $self->{SleepTime};
  $heap->{Rate} = $self->{Rate};
  $heap->{MaxFiles} = $self->{MaxFiles};
  $heap->{Interval} = $self->{Interval};
  $heap->{Persistent} = $self->{Persistent};
  $heap->{CallbackSession} = $self->{CallbackSession};
  $heap->{CallbackMethod} = $self->{CallbackMethod};
  $heap->{SizeTable} = $self->{SizeTable};
  $heap->{Notify} = $self->{Notify};
  $heap->{SenderWatcher} = $self->{SenderWatcher};
  $heap->{Sender} = $self->{Sender};

  $heap->{WaitForData} = 1;
  $heap->{WaitForDataInterval} = 1;

  # if persistent, tie file to hash and clear hash of not injected files
  if ( defined $self->{Persistent} )
    {
      $heap->{Database} = tie(%fileStatusList, 'DB_File', $self->{Persistent}) || die "can't open $self->{Persistent}: $!";

      while ( my ($filename, $status) = each %fileStatusList )
	{
	  if ( defined $self->{Restart} or 0 == $status )
	    {
	      delete($fileStatusList{$filename});
	    }
	}

      $heap->{Database}->sync();
    }

  # add file watcher for config file
  $heap->{Watcher} = T0::FileWatcher->new(
					  File            => $self->{Config},
					  Client          => $session,
					  Event           => 'config_changed',
					  Interval        => $self->{ConfigRefresh},
					 );

  if ( defined $heap->{LFNList} )
    {
      $kernel->yield('start_lfnlist');
    }
  elsif ( defined $heap->{Directory} )
    {
      $kernel->yield('start_rfdir');
    }
}

sub start_lfnlist {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  my %inputhash = (
		   Session => $session,
		   Callback => 'lfnlist_done',
		   LFNList => $heap->{LFNList},
		   MinAge => $heap->{MinAge},
		   Files => {
			     Status => \%fileStatusList,
			     Size => \%fileSizeList,
			     Date => \%fileDateList
			    },
		  );

  my $rfdir = T0::Iterator::LFNList->new(\%inputhash);

  $kernel->yield('inject_file');
}

sub start_rfdir {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  my %inputhash = (
		   Session => $session,
		   Callback => 'rfdir_done',
		   Directory => $heap->{Directory},
		   MinAge => $heap->{MinAge},
		   Files => {
			     Status => \%fileStatusList,
			     Size => \%fileSizeList,
			     Date => \%fileDateList
			    },
		  );

  my $rfdir = T0::Iterator::Rfdir->new(\%inputhash);

  $kernel->yield('inject_file');
}

sub lfnlist_done {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  $heap->{WaitForData} = 0;
}

sub rfdir_done {
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

  $heap->{Self}->Quiet("Configuration file \"$file\" has changed.\n");

  T0::Util::ReadConfig( $heap->{Self} );

  $heap->{Channel} = $heap->{Self}->{Channel};
  $heap->{Directory} = $heap->{Self}->{Directory};
  $heap->{LFNList} = $heap->{Self}->{LFNList};
  $heap->{MinAge} = $heap->{Self}->{MinAge};
  $heap->{SleepTime} = $heap->{Self}->{SleepTime};
  $heap->{Rate} = $heap->{Self}->{Rate};
  $heap->{MaxFiles} = $heap->{Self}->{MaxFiles};
  $heap->{Interval} = $heap->{Self}->{Interval};
  $heap->{SizeTable} = $heap->{Self}->{SizeTable};
  $heap->{Notify} = $heap->{Self}->{Notify};
}

sub ReadConfig {
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;

  T0::Util::ReadConfig( $self );
}

sub inject_file {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  if ( defined $heap->{MaxFiles} and $heap->{MaxFiles} < 1 )
    {
      $heap->{Self}->Quiet("Reached maximum file count, exiting\n");

      $heap->{Watcher}->RemoveClient($session);
      $heap->{SenderWatcher}->RemoveObject($heap->{Sender});

      if ( defined $heap->{Persistent} )
	{
	  delete $heap->{Database};
	  untie(%fileStatusList);
	}
      delete $heap->{Self}->{Session};

      $kernel->post( $heap->{CallbackSession}, 'forceShutdown');
      
      return 1;

    }

  # loop over files, find first uninjected
  my $file = undef;
  my $size = 0;
  my $date = 0;
  for my $filename ( sort keys %fileStatusList )
    {
      if ( 0 == $fileStatusList{$filename} )
	{
	  $fileStatusList{$filename} = 1;
	  if ( exists $heap->{Database} ) { $heap->{Database}->sync(); }

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

	  $kernel->delay_set( 'inject_file', $heap->{WaitForDataInterval} );

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

	  $heap->{Watcher}->RemoveClient($session);
	  $heap->{SenderWatcher}->RemoveObject($heap->{Sender});

	  if ( defined $heap->{Persistent} )
	    {
	      delete $heap->{Database};
	      untie(%fileStatusList);
	    }
	  delete $heap->{Self}->{Session};

	  $kernel->post( $heap->{CallbackSession}, 'forceShutdown'); 
	  
	  return 1;

	}
    }

  # reset time interval
  $heap->{WaitForDataInterval} = 1;

  my $prefix = '';
  if ( defined $heap->{MaxFiles} )
    {
      $heap->{MaxFiles}--;
      if ( $heap->{MaxFiles} > 0 ) { $prefix = "$heap->{MaxFiles}: "; }
    }

  my $datasetNumber = undef;
  if ( defined $heap->{SizeTable} )
    {
      $datasetNumber = T0::Util::bin_table($heap->{SizeTable});
      $heap->{Self}->Quiet("$prefix $file, $size, $date, $datasetNumber\n");
    }
  else
    {
      $heap->{Self}->Quiet("$prefix $file, $size, $date\n");
    }

  my $channel = defined $heap->{Channel} ? $heap->{Channel} : T0::Util::GetChannel($file);

  my %t = (
	   $heap->{Notify} => $file,
	   Size => $size,
	   Date => $date,
	   Channel => $channel,
	   DatasetNumber => $datasetNumber,
	  );
  $kernel->post( $heap->{CallbackSession}, $heap->{CallbackMethod}, ( \%t ) );

  if ( defined $heap->{Rate} )
  {
    $heap->{Interval} = $size / (1024*1024) / $heap->{Rate};
    $heap->{Interval} = int( 1000 * $heap->{Interval} ) / 1000;
    $heap->{Self}->Verbose("Set interval=",$heap->{Interval}," for ",$heap->{Rate}," MB/sec\n");
  }

  if ( defined $heap->{Interval} ) 
  { 
      $kernel->delay_set( 'inject_file', $heap->{Interval} ); 
  }
  else 
  { 
      $kernel->yield( 'inject_file' ); 
  }
  return 1;
}

1;
