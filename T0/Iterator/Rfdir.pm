use strict;
package T0::Iterator::Rfdir;
use Date::Manip;
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
my %fileStatusList;
my %fileSizeList;

#
# make fileStatusList persistent
#
my $file = 'fileStatusList.db';
my $database = tie(%fileStatusList, 'DB_File', $file) || die "can't open $file: $!";

sub _init
{
  my $self = shift;

  my %h = @_;
  map { $self->{$_} = $h{$_} } keys %h;
  $self->ReadConfig();

  # clear fileStatusList database
  # remove everything if not persistent
  # remove not yet injected files even if persistent
  while ( my ($filename, $status) = each %fileStatusList )
    {
      if ( not defined $self->{Persistent} or ( 1 != $self->{Persistent} ) or ( 0 == $status ) )
	{
	  delete($fileStatusList{$filename});
	}
    }
  $database->sync();

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

our @attrs = ( qw/ Config ConfigRefresh Directory MinAge SleepTime Persistent Rate / );
our %ok_field;
for my $attr ( @attrs ) { $ok_field{$attr}++; }

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
  $self->{$attr} = shift if @_;
  return $self->{$attr};
}

sub ReadConfig
{
  no strict 'refs';
  my $self = shift;
  my $file = $self->{Config};
  return unless $file;  

  T0::Util::ReadConfig( $self );
}

sub Next
{
  my $self = shift;

  # loop over files
  # return first uninjected
  while ( my ($filename, $status) = each %fileStatusList )
    {
      if ( 0 == $status )
	{
	  $fileStatusList{$filename} = 1;
	  $database->sync();
	  return ($filename,$fileSizeList{$filename}) if wantarray();
	  return $filename;
	}
    }

  # check if SleepTime is configured
  # exit if it isn't, otherwise sleep for the
  # given time and then search for new files
  if ( defined($self->{SleepTime}) and ( $self->{SleepTime} >= 0 ) )
    {
      sleep 60 * $self->{SleepTime};
      $self->ScanDirectory($self->{Directory});
      return $self->Next();
    }
  else
    {
      untie(%fileStatusList);
      return;
    }
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
      my $date = $temp[5] . " " . $temp[6] . " " . $temp[7];
      my $file = $temp[8];

      if ( $protection =~ /^dr/ && ! ( $file =~ /^\./ ) )
	{
	  $self->ScanDirectory($currentDir . '/' . $file);
	}
      elsif ( $protection =~ /^-r/ )
	{
	  my $filename = $currentDir . '/' . $file;

	  if ( not defined($fileStatusList{$filename}) )
	    {
	      # check that fileDate is earlier than cutoffDate
	      my $flag = -1;
              if ( defined($self->{MinAge}) )
		{
		  $flag = Date_Cmp( ParseDate($date), DateCalc("now","- " . $self->{MinAge} . " minutes") );
		}
	      if ( $flag < 0 )
		{
		  $fileStatusList{$filename} = 0;
		  $fileSizeList{$filename} = $size;
		}
	    }
	}
    }
}

1;
