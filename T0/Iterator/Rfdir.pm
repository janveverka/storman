use strict;
package T0::Iterator::Rfdir;
use Date::Manip;

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
#   0 for exist
#   1 for injected
my %fileList;


sub _init
{
  my $self = shift;

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;

  $self->ScanDirectory($self->{Directory});

  return $self;
}

sub new
{
  my $proto  = shift;
  my $class  = ref($proto) || $proto;
  my $parent = ref($proto) && $proto;
  my $self = {  };
  bless($self, $class);
  $self->_init(@_);
}

sub Next
{
  my $self = shift;

  # loop over files
  # return first uninjected
  for ( keys(%fileList) )
    {
      my $filename = $_;
      if ( 0 == $fileList{$filename} )
	{
	  $fileList{$filename} = 1;
	  return $filename;
	}
    }

  # reached end of loop, means all files are injected, exit
  return;

  # as an alternative :
  #   sleep for a while to not overload the storage system
  #   then rerun ScanDirectory to search for new files
  #   and call myself again
  #sleep 3600
  #$self->ScanDirectory($self->{Directory});
  #return $self->Next();
}

sub ScanDirectory
{
  my $self = shift;

  my ($currentDir) = @_;

  my @lines = qx {rfdir $currentDir};

  foreach my $line ( @lines )
    {
      chomp($line);

      # parse line
      my @temp = split (" ", $line);

      my $protection = $temp[0];
      my $size = $temp[4];
      my $date = "$temp[5] $temp[6] $temp[7]";
      my $file = $temp[8];

      if ( $protection =~ /^dr/ && ! ( $file =~ /^\./ ) )
	{
	  $self->ScanDirectory($currentDir . '/' . $file);
	}
      elsif ( $protection =~ /^-r/ )
	{
	  if ( not defined($fileList{$file}) )
	    {
	      my $filename = $currentDir . '/' . $file;

	      # check that fileDate is earlier than cutoffDate
	      my $flag = Date_Cmp( ParseDate($date), DateCalc("now","- 1 minute") );
	      if ( $flag < 0 )
		{
		  $fileList{$filename} = 0;
		}
	    }
	}
    }
}

1;
