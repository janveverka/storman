use strict;
package T0::GenericManager::WorkerParser;

our $verbose = 1;

###
### Routine reads string passed and parses it into the hash given by reference
###

sub parse_output {
  my $h = shift;
  my $s = shift;
  my @p;
  
#  $verbose && print "WorkerParser: Got input string $s\n";

#  if ($s=~m%Send\s*=\s*\{({'\.'+\s*=\s*'\.'+\s*[,]}+)\}%) {
  if ($s=~m%Send\s*=\s*\{(.*)\}%) {

    for $_ (split(/\s*,\s*/, $1)) {
      m%'(.*)'\s*=\s*'(.*)'%;
      $h->{$1} = $2;

      $verbose && print "WorkerParser: Got a hash element $1 => $2\n";
    };
  } else {$verbose && print "WorkerParser: String didn't match!!!!\n"};

};



1;
