use strict;
use warnings;
package T0::CopyCheck::Rfstat;
use POE qw( Wheel::Run Filter::Line );
use File::Basename;

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

sub new
{
  my ($class, $hash_ref) = @_;
  my $self = {};
  bless($self, $class);

  POE::Session->create(
		       inline_states => {
					 _start => \&start_tasks,
					 start_rfstat => \&start_rfstat,
					 cleanup_task => \&cleanup_task,
					},
		       args => [ $hash_ref, $self ],
		      );

  return $self;
}

sub start_tasks {
  my ( $kernel, $heap, $hash_ref, $self ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  # remember hash reference
  $heap->{inputhash} = $hash_ref;

  # remember reference to myself
  $heap->{Self} = $self;

  # put callback session and method on heap
  $heap->{session} = $hash_ref->{session};
  $heap->{callback} = $hash_ref->{callback};

  # remember the rest of the hash
  $heap->{retries} = $hash_ref->{retries};
  $heap->{retry_backoff} = $hash_ref->{retry_backoff};


  # start the job
  $kernel->yield('start_rfstat',($hash_ref->{hash_job_ref}));

}

sub start_rfstat {
  my ( $kernel, $heap, $job ) = @_[ KERNEL, HEAP, ARG0 ];

  my $work = $job->{work};
  my $pfn = $work->{PFN};

  # nothing went wrong yet
  $job->{status} = 0;

  # check if file exists
  my @stats = qx {rfstat $pfn};
  my $status = $?;

  $kernel->yield('cleanup_task',($job,$status,\@stats));
}

sub cleanup_task {
  my ( $kernel, $heap, $job, $status, $stats_ref ) = @_[ KERNEL, HEAP, ARG0, ARG1, ARG2 ];

  my $work = $job->{work};
  my $pfn = $work->{PFN};

  my @stats = @$stats_ref;


  if ( $status != 0 )
  {
      $heap->{Self}->Quiet("Rfstat failed, output follows\n");
      foreach my $stat ( @stats )
      {
	  $heap->{Self}->Quiet("RFSTAT: " . $stat . "\n");
      }

      # Check if we have to retry the command after the fail
      if ( defined($heap->{retries}) && $heap->{retries}>0 )
      {
	  $heap->{Self}->Quiet("Retrying rfstat on $pfn...\n");
	  $heap->{retries}--;

	  if ( defined($heap->{retry_backoff}) )
	  {
	      $kernel->delay_set('start_rfstat', $heap->{retry_backoff}, ($job));
	  }
	  else
	  {
	      $kernel->yield('start_rfstat', ($job));
	  }	  
	  return;
      }
      # Without more retries status is set
      else
      {
	  $heap->{Self}->Quiet("No more retries rfstat on $pfn...\n");
	  $job->{status} = 1;	  
      }
  }
  else
  {
      foreach my $stat ( @stats )
      {
	  if ( $stat =~ /^Size/)
	  {
	      chomp($stat);
	      my ($dummy,$size) = split (" : ",$stat);
	      
	      #if ( $size == $work->{FILESIZE} )
	      if ( $size >= $work->{FILESIZE} and $size > 0 )
	      {
		  $work->{FILESIZE} = $size;

		  $heap->{Self}->Quiet($pfn, " size matches.\n");
		  
		  # delete file if DeleteAfterCheck flag is set
		  if (defined($work->{DeleteAfterCheck}) && $work->{DeleteAfterCheck} == 1)
		  {
		      if ( $pfn =~ m/^\/castor/ )
		      {
			  qx {stager_rm -M $pfn};
			  qx {nsrm $pfn};
		      }
		      else
		      {
			  qx {rfrm $pfn};
		      }
		      
		      $heap->{Self}->Quiet($pfn, " deleted.\n");
		  }
	      }
	      else
	      {
		  $heap->{Self}->Quiet($pfn, " size doesn't match.\n");
		  $job->{status} = 1;
	      }
	  }
      }
  }

  $kernel->post( $heap->{session}, $heap->{callback} );


  delete  $heap->{inputhash};
  delete  $heap->{Self};
  delete  $heap->{session};
  delete  $heap->{callback};
  delete  $heap->{retries};
  delete  $heap->{retry_backoff};
}

1;
