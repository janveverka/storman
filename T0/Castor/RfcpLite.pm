########################################################################################################
#
# Exec rfcp command with all the given files
#
########################################################################################################
#
# This module has to receive (in the call to new) a hash containing:
# -source => Path of the source file.
# -target => Path of the target dir + filename.
#
# Optionally:
# -svcclass =>
# -retries => The number of retries we will do before giving up.
# -retry_backoff => The number of seconds we will wait before the next try.
# -delete_bad_files => If is set (=1) we will delete bad files created after a unsuccessfull rfcp.
#
# After finishing the rfcp command will return:
# -status => It will be 0 if everything went fine or !=0 if there was something wrong.
#
########################################################################################################


use strict;
use warnings;
package T0::Castor::RfcpLite;
use File::Basename;
use T0::Castor::RfstatHelper;

our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS, $VERSION);

use Carp;
$VERSION = 1.00;
@ISA = qw/ Exporter /;

our $hdr = __PACKAGE__ . ':: ';
sub Croak   { croak $hdr,@_; }
sub Carp    { carp  $hdr,@_; }
sub Verbose { T0::Util::Verbose( (shift)->{Verbose}, ("RFCPLITE:\t", @_) ); }
sub Debug   { T0::Util::Debug(   (shift)->{Debug},   ("RFCPLITE:\t", @_) ); }
sub Quiet   { T0::Util::Quiet(   (shift)->{Quiet},   ("RFCPLITE:\t", @_) ); }

sub new
{
  my ($class, $source, $target, $svcclass, $retries, $retry_backoff, $delete_bad_files) = @_;
  my $self = {};
  bless($self, $class);

  # Store arguments
  $self->{source} = $source;
  $self->{target} = $target;
  $self->{svcclass} = $svcclass;
  $self->{retries} = $retries;
  $self->{retry_backoff} = $retry_backoff;
  $self->{delete_bad_files} = $delete_bad_files;

  # Flags
  $self->{checked_source} = 0;
  $self->{checked_target_dir} = 0;



  if ( defined $self->{svcclass} )
    {
      $ENV{STAGE_SVCCLASS} = $self->{svcclass};
    }
  else
    {
      $self->Quiet("SvcClass not set, use t0input!\n");
      $ENV{STAGE_SVCCLASS} = 't0input';
    }

  $self->start_rfcp();

  return $self->{status};
}

# Execute rfcp command.
# Return 1 (retry), 0 (don't retry)
sub start_rfcp {

  my $self = shift;

  do{
    $self->Quiet("Start copy from $self->{source} to $self->{target}\n");

    qx( rfcp $self->{source} $self->{target} 2>&1);
    $self->{status} = $?;
  }while ($self->rfcp_exit_handler());

}



# This process will try to recover from any error.
# This one check if there has been any problem and if so check if the source file exist.
sub rfcp_exit_handler {
  my $self = shift;

  # Something went wrong
  if ( $self->{status} != 0 )
    {
      $self->Quiet("Rfcp of " . $self->{source} . " failed with status $self->{status}\n");

      # Check if the source file exists just the first time.
      if( !$self->{checked_source} )
	{

	  $self->Quiet("Checking if file " . $self->{source} . " exists\n");
	  my $file_status = T0::Castor::RfstatHelper::checkFileExists( $self->{source} );

	  $self->{checked_source} = 1;

	  # Rfstat failed. Source doesn't exist
	  # If the source file doesn't exist we have nothing else to do.
	  if ( $file_status != 0 )
	    {
	      $self->Quiet("Source file " . $self->{source} . " does not exist\n");
	      return 0;
	    }
	  # Source exists
	  # If it exists continue with the cleanup.
	  else
	    {
	      $self->Quiet("Source file " . $self->{source} . " exists\n");
	      return $self->check_target_exists();
	    }
	}
      else
	{
	  return $self->check_target_exists();
	}
    }
  # rfcp succeeded.
  else
    {
      $self->Quiet("$self->{source} successfully copied\n");
      return 0;
    }
}


# Check for existence of directory (if status is 256 or 512)
sub check_target_exists {
  my $self = shift;

  if ( ($self->{status} == 256 || $self->{status} == 512) && ($self->{checked_target_dir} == 0) )
    {
      my $targetdir = dirname( $self->{target} );
      $self->Quiet("Checking if directory $targetdir exists\n");
      my $dir_status = T0::Castor::RfstatHelper::checkDirExists( $targetdir );

      $self->{checked_target_dir} = 1;

      # The target doesn't exists. Create the directory
      if ( $dir_status == 1 )
	{
	  $self->Quiet("Creating directory $targetdir\n");
	  qx { rfmkdir -p $targetdir };

	  return $self->rfcp_retry_handler(1);
	}
      elsif ($dir_status == 2)
	{
	  # The targetdir is not a dir. Stop the iteration
	  $self->Quiet("$targetdir is not a directory\n");
	  return 0;
	}
      # Target exists and it is a directory
      elsif ($dir_status == 0)
	{
	  $self->Quiet("Directory $targetdir exists\n");
	  return $self->rfcp_retry_handler(0);
	}
    }
  # no problems with target, regular retry
  else
    {
      return $self->rfcp_retry_handler(0);
    }
}


# Remove target file if it exists only if I didn't create the target
# directory in the previous step
sub rfcp_retry_handler {
  my $self = shift;
  my $createdTargetDir = shift;

  if ( defined($self->{delete_bad_files}) && $self->{delete_bad_files} == 1
       && $createdTargetDir == 0 )
    {
      $self->Quiet("Deleting file before retrying\n");

      if ( $self->{target} =~ m/^\/castor/ )
	{
	  qx {stager_rm -M $self->{target} 2> /dev/null};
	  qx {nsrm $self->{target} 2> /dev/null};
	}
      else
	{
	  qx {rfrm $self->{target} 2> /dev/null};
	}
    }


  # After creating the dir we retry without waiting and without decreasing retries
  if ( $createdTargetDir == 1 )
    {
      return 1;
    }
  # Retrying
  elsif ( $self->{retries} > 0 )
    {
      $self->Quiet("Retry count at " . $self->{retries} . " , retrying\n");
      $self->{retries}--;

      if ( exists $self->{retry_backoff} )
	{
	  sleep( $self->{retry_backoff});
	}
      return 1;
    }
  else
    {
      $self->Quiet("Retry count at " . $self->{retries} . " , abandoning\n");
      return 0;
    }
}

1;
