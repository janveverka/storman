use strict;
package T0::Index::Generator;
use T0::Util;
use File::Basename;
use Sys::Hostname;
use Cwd;

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

sub _init
{
  my $self = shift;

  ( $self->{Name} = __PACKAGE__ ) =~ s%T0::%%;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
  if ( $self->{Config} !~ m%^/% )
  {
    $self->{Config} = cwd . '/' . $self->{Config};
  }
  $self->ReadConfig($self->{Config});

  $self->{SvcClass} = $self->{SvcClass}		||
		      $StorageManager::Worker{SvcClass}	||
		      't0input';

  chdir ($self->{IndexDir}) or Croak "chdir: ",$self->{IndexDir},": $!\n";
  $self->{IndexDir} = cwd();
  $self->{Host} = hostname();
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

sub Options
{ 
  my $self = shift;
  my %h = @_;
  map { $self->{$_} = $h{$_}; } keys %h;
}

our @attrs = ( qw/ RawFileProtocol IndexDir Name Host Verbose Quiet Debug
                EventSizeMin EventSizeMax EventSizeStep EventSizeTable
		InputSvcClass OutputSvcClass ConfigRefresh Config / );
our %ok_field;
for my $attr ( @attrs ) { $ok_field{$attr}++; }

sub AUTOLOAD {
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  Croak "AUTOLOAD: Invalid attribute method: ->$attr()" unless $ok_field{$attr};
  $self->{$attr} = shift if @_;
# if ( @_ ) { Croak "Setting attributes not yet supported!\n"; }
  return $self->{$attr};
}

sub ReadConfig
{
  my $self = shift;
  my $file = $self->{Config} or return;
  T0::Util::ReadConfig( $self );

  $self->{IndexDir} = '.' unless defined($self->{IndexDir});
  $self->{RawFileProtocol}  = ''  unless defined($self->{RawFileProtocol});
}

sub Generate
{
  my $self = shift;
  my (%h,$file,$fsize,$esize,$dataset,$offset,$index,$protocol);
  %h = @_;

  $file = $h{file};

  $self->Quiet("Generate index for: $file\n");
  $protocol = $self->{RawFileProtocol} or '';
  if ( $protocol && $protocol !~ m%:$% ) { $protocol .= ':'; }
  if ( !($fsize = $h{size}) )
  {
    if ( -f $file )
    {
      $fsize = (stat($file))[7];
    }
    else
    {
      open RFSTAT, "rfstat $file |" or Croak "rfstat: $file: $!\n";
      while ( <RFSTAT> )
      {
        if ( m%^Size\s+\S+\s+:\s+(\d+)% ) { $fsize = $1; }
      }
      close RFSTAT or Croak "close: rfstat $file: $!\n";
      $protocol = 'rfio:';
    }
  }
  $self->Verbose("  File size: $fsize bytes\n");

  $_ = basename $file;
  s%raw%idx%gi;
  s%dat%idx%gi;
  if ( ! m%idx$% ) { $_ .= '.idx'; }
  $index = $self->{IndexDir} . '/' . $_;
  $self->Verbose("  Index file: $index\n");

  $offset = 0;
  open INDEX, "> $index" or Croak "open: $index: $!\n";
  if ( $protocol eq 'rfio:' && $file =~ m%^/castor% )
    {
      $protocol = "rfio:///?svcClass=" . $self->{SvcClass} . "&path=";
    }
  else
    {
      $protocol = "rfio:";
    }
  print INDEX "FileURL = $protocol$file\n";

  while ( $fsize > $self->{EventSizeMax} )
  {
    $esize = $self->EventSize,"\n";
    $dataset = bin_table($self->{DatasetRateTable});
    print INDEX "$dataset $offset $esize\n";
    $fsize -= $esize;
    $offset += $esize;
  }
  print INDEX "$dataset $offset $fsize\n";
  close INDEX;
  return $index;
}

sub EventSize
{
  my $self = shift;
  return profile_table(
		$self->{EventSizeMin},
		$self->{EventSizeMax},
		$self->{EventSizeStep},
		$self->{EventSizeTable}
	);
}

1;
