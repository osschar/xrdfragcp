#!/usr/bin/perl -w

#-----------------------------------------------------------------------
# Globals / config variables
#-----------------------------------------------------------------------

@DIRS = qw( /store/mc );

$GETBB = "./get_bad_blocks";
$XRDFC = "./xrdfragcp";

$HDP_PREFIX  = "/cms/phedex";
$XRD_PREFIX  = "root://xrootd.unl.edu/";
$XRD_POSTFIX = "?tried=xrootd.t2.ucsd.edu";


#-----------------------------------------------------------------------
# Main program
#-----------------------------------------------------------------------

for $dir (@DIRS)
{
  my $hdp_dir = "$HDP_PREFIX$dir";

  print "Executing: $GETBB $hdp_dir\n";

  open BFS, "$GETBB $hdp_dir |" or die "Execution of $GETBB failed";
  while (my $bfl = <BFS>)
  {
    chomp $bfl;
    my @els = split ' ', $bfl;

    my $hdp_file = shift @els;
    die "Hadoop file $hdp_file does not start with $HDP_PREFIX." unless
	$hdp_file =~ m/^$HDP_PREFIX/;
    my $xrd_file = $hdp_file;
    $xrd_file =~ s/^$HDP_PREFIX//;
    $xrd_file = $XRD_PREFIX . $xrd_file . $XRD_POSTFIX;

    my @frag_args;
    while (my $frag = shift @els)
    {
      die "Wrong fragment format '$frag'" unless
	  $frag =~ m/^(\d+),(\d+)$/;
      push @frag_args, "$1 $2";
    }

    my $xrdfc_cmd = join(" ", $XRDFC,
                              map({ "--frag $_" } @frag_args),
                              $xrd_file);
    print "Executing: $xrdfc_cmd\n";
    system $xrdfc_cmd and die "Execution of $xrdfc_cmd failed.";

    # run file-fixer
    print "I'd execute the file-fixer now if I knew how ...\n";
  }
  close BFS;
}
