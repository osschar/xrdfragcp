#!/usr/bin/perl -w

#-----------------------------------------------------------------------
# Globals / config variables
#-----------------------------------------------------------------------

@DIRS = qw( /store/mc );

$GETBB  = "./get_bad_blocks";
$XRDFC  = "./xrdfragcp";
$REPAIR = "hadoop jar hdtools.jar repair";

$HDP_PREFIX  = "/cms/phedex";
$XRD_PREFIX  = "root://xrootd.unl.edu/";
$XRD_POSTFIX = "?tried=xrootd.t2.ucsd.edu";

$FRAGMENT_NAME   = "fragment";
$FIXED_FILE_NAME = "fixed-file";

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
    my ($basename) = $hdp_file =~ m!/([^/]+)$! or
	die "Can not extract basename from $hdp_file";
    $hdp_file =~ m/^$HDP_PREFIX/ or
	die "Hadoop file $hdp_file does not start with $HDP_PREFIX.";
    my $xrd_file = $hdp_file;
    $xrd_file =~ s/^$HDP_PREFIX//;
    $xrd_file = $XRD_PREFIX . $xrd_file . $XRD_POSTFIX;

    my @frag_args;
    my @repair_args;
    while (my $frag = shift @els)
    {
      $frag =~ m/^(\d+),(\d+)$/ or
	  die "Wrong fragment format '$frag'";
      push @frag_args, "$1 $2";
      push @repair_args, $frag;
    }

    my $xrdfc_cmd = join(" ", $XRDFC,
			      "--prefix", $FRAGMENT_NAME,
                              map({ "--frag $_" } @frag_args),
                              $xrd_file);
    print "Executing: $xrdfc_cmd\n";
    system $xrdfc_cmd and die "Execution of $xrdfc_cmd failed.";

    my $repair_cmd = join("", $REPAIR,
                              "--prefix", $FRAGMENT_NAME,
                              "--outfile", $FIXED_FILE_NAME,
                              $hdp_file);
    print "Executing: $repair_cmd\n";
    # MT: This doesn't quite work yet ....
    system $repair_cmd and die "Execution of $repair_cmd failed.";

    # Register the new file back ...
    print "Now should register $FIXED_FILE_NAME as $basename ...\n";

    # And remove fragments, fixed-file
  }
  close BFS;
}