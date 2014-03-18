########################################################################################################
#
# Exec rfstat command and return the output as part of the input hash
#
########################################################################################################
#
# This module has to receive (in the call to new) a hash containing:
# -session => The session which we will come back to after rfstat.
# -callback => The function of the session we will call back to. We will send the input hash as argument.
# -PFN => The complete path of the file we want to check.
#
# Optionally:
# -retries => The number of retries we will do before giving up.
# -retry_backoff => The number of seconds we will wait before the next try.
#
#
# After finishing the rfstat command the funcion will call back the specified funcion
# and will add to the input hash the following information:
# -status => It will be 0 if everything went fine or !=0 if there was something wrong.
# -stats_data => It's a reference to the stats returned by rfstat command (hash of stats).
# The keys are the names of each stat and the content is the stat.
# -stats_fields => It's a reference to the names of each stat(array of names).
# -stats_number => It's the number of stats we have collected.
# (The length of status_data and status_field).
#
# Example:
# First element: $input->{stats_data}->{$input->{stats_fields}->[0]}
# Last element:  $input->{stats_data}->{$input->{stats_fields}->[$input->{stats_number} - 1]}
# With the name of the field: $input->{stats_data}->{'Size (bytes)'}
#
########################################################################################################


use strict;
use warnings;
package T0::Castor::Rfstat;
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
  $heap->{PFN} = $hash_ref->{PFN};


  # start the job
  $kernel->yield('start_rfstat');

}

sub start_rfstat {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  my $pfn = $heap->{PFN};

  # nothing went wrong yet
  $heap->{inputhash}->{status} = 0;
  $heap->{inputhash}->{stats_number} = 0;

  # check if file exists
  my @stats = qx {unset STAGER_TRACE ; unset RFIO_TRACE ; rfstat $pfn};
  my $status = $?;

  $kernel->yield('cleanup_task',($status,\@stats));
}

sub cleanup_task {
  my ( $kernel, $heap, $status, $stats_ref ) = @_[ KERNEL, HEAP, ARG0, ARG1 ];

  my $pfn = $heap->{PFN};

  my @stats = @{$stats_ref};

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
	      $kernel->delay_set('start_rfstat', $heap->{retry_backoff});
	  }
	  else
	  {
	      $kernel->yield('start_rfstat');
	  }
	  return;
      }
      # Without more retries status is set
      else
      {
	  $heap->{Self}->Quiet("No more retries rfstat on $pfn...\n");
	  $heap->{inputhash}->{status} = $status;
      }
  }
  # Organize the data inside a hash
  else
  {
      my($index) = 0;
      foreach my $stat ( @stats )
      {

	chomp($stat);
	my ($field,$data) = split (" : ",$stat);

	# Remove spaces at the end
	$field =~ s/\s+$//;

	$heap->{inputhash}->{stats_fields}->[$index++] = $field;
	$heap->{inputhash}->{stats_data}->{$field} = $data;
      }

      $heap->{inputhash}->{stats_number} = $index;
  }

  $kernel->post( $heap->{session}, $heap->{callback}, $heap->{inputhash});


  delete  $heap->{inputhash};
  delete  $heap->{Self};
  delete  $heap->{session};
  delete  $heap->{callback};
  delete  $heap->{retries};
  delete  $heap->{retry_backoff};
}

1;
