#!/usr/bin/env perl

# Call the function by providing the following values:
# 1. dataset: dataset name string.
# 2. block: block name string or undef if you intend to close a dataset.
# 3. dbs: DBS string.
# 4. dls: DLS string.
# 5. output: either a directory where to put the drop or an XML file.
# 6. input: reference to hash containg LFNs as keys and a reference to an array
#    as value.
#    The array should contain filesize and cksum.
#        Example: $input->{LFN} = [size, cksum]
#    In order to close a block, just provide the keyword CLOSE instead of the
#    LFN as key and undef as value.
#        Example: $input->{CLOSE} = undef
#    A dataset is closed, by providing 'undef' as block and the keyword CLOSE
#    as described above.

use strict;
use File::Temp "mkdtemp";

sub generateDrop
{
    my ($dataset, $block, $dbs, $dls, $output, $input) = @_;


    # Analyze the inpout dir and return the meta info for each file if we
    # are in dir mode. Use the provided file directly, if we get a file as
    # input and use the options as meta info otherwise.

    my $error = undef;

    # Create the dropdir, if we deal with a dir as output
    my $now = time();
    $output = &mkdtemp("$output/drop.$now.XXXXXXXXXXXX") if ( -d $output );
    
    # Generate XML fragment for drop box in Output dir
    $error = &XMLFragGen($output, $dataset, $block, $dbs, $dls, $input);
    die "XML fragment could not be generated. Reason was:\n$error" if $error;

    # mark the drop ready to go in case of a dropdir
    $error = eval { system( "touch $output/go" ); return $@ } if ( -d $output && !$error );
    die "Couldn't create go flag for drop dir $output ! Reason was:\n$error" if $error;

    return 1; #true
}


# Create the XML fragment

sub XMLFragGen
{
    my ( $output, $dataset, $block, $dbs, $dls, $fileref ) = @_;

    $output =~ s|^$output|$output/XMLFragment.xml| if (-d $output);

    my @open = ( 'y', 'y' );
    $open[0] = 'n' if ( !$block && exists $fileref->{CLOSE} );
    $open[1] = 'n' if ( $block && exists $fileref->{CLOSE} );

    eval
    {
	open(XMLOUT, ">$output") or die "Could not open $output for writing";
	print XMLOUT "<dbs name=\'$dbs\' dls=\'$dls\'>\n";
	print XMLOUT "  <dataset name=\'$dataset\' is-open=\'$open[0]\' is-transient='n'>\n";
	print XMLOUT "    <block name=\'$block\' is-open=\'$open[1]\'>\n" if $block;
	foreach my $lfn ( keys %{$fileref} )
	{
	    next if $lfn =~ m|^CLOSE|;
	    print XMLOUT "      <file lfn=\'$lfn\' checksum=\'cksum:$fileref->{$lfn}->[1]\' ".
		"size =\'$fileref->{$lfn}->[0]\'/>\n";
	}
	print XMLOUT "    </block>\n" if $block;
	print XMLOUT "  </dataset>\n";
	print XMLOUT "</dbs>\n";
	close(XMLOUT);
    };
    return $@;
}

1;
