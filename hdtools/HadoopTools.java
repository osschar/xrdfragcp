package hdtools;

import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;

public class HadoopTools extends Configured implements Tool {
  private static class BlockLocation{
    public long offset;
    public long nBytes;
    
    public BlockLocation(long offset, long nBytes) {
      this.offset = offset;
      this.nBytes = nBytes;
    }
    public String toString() {
      return offset + "," + nBytes;
    }
  }
  
  private static final String REPAIR_USAGE = "repair [OPTIONS] INFILE [OFFSET,LEN]...";
  
  private int explode(String[] argv) throws IOException {
    String inFile = argv[0];
    String outFile = argv[1];
    
    Path path = new Path(inFile);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.getFileStatus(path).isDir()) {
      throw new IOException("Source must be a file.");
    }
    
    long fileSize = srcFs.getFileStatus(path).getLen();
    long blockSize = srcFs.getFileStatus(path).getBlockSize();
    int numBlocks = (int) (fileSize / blockSize);
    int bufSize = getConf().getInt("io.bytes.per.checksum", 4096);
    
    byte[] buffer = new byte[bufSize];
    FSDataInputStream in = null;
    FileOutputStream out = null;
    try {
      in = srcFs.open(path);
      
      for (int i = 0; i < numBlocks; i++) {
        out = new FileOutputStream(outFile + "." + i * blockSize);
        
        for (int j = 0; j < blockSize / bufSize; j++) {
          in.read(buffer);
          out.write(buffer);
        }
        
        out.close();
      }
      
      // if there's a remainder get it now
      if (fileSize % blockSize != 0) {
        out = new FileOutputStream(outFile + "." + numBlocks * blockSize);
        int bytesRead;
        while ((bytesRead = in.read(buffer)) >= 0)
          out.write(buffer, 0, bytesRead);
      }
      
    }
    finally {
      if (in != null)
        in.close();
      if (out != null)
        out.close();
    }
    return 0;
  }

  private int repair(String[] argv) throws IOException {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("p", "prefix", true, "prefix for block files");
    options.addOption("o", "outfile", true, "path to write outfile");
    options.addOption("v", false, "vebose");
    options.addOption("h", "help", false, "print this message");
    
    CommandLine line;
    try {line = parser.parse(options, argv);}
    catch (ParseException e) {
      System.err.println(e.getMessage());
      return 1; 
    }
    
    HelpFormatter formatter = new HelpFormatter();
    
    if (line.hasOption("h")) {
      formatter.printHelp(REPAIR_USAGE, options);
      return 0;
    }
    
    String[] args = line.getArgs();
    
    if (args.length < 1) {
      formatter.printHelp(REPAIR_USAGE, options);
      return 1;
    }
    String inFile = args[0];
    
    String basename = new File(inFile).getName();
    String prefix = line.hasOption("p") ? line.getOptionValue("p") : basename;
    String outFile = line.hasOption("o") ? line.getOptionValue("o") : basename;
    boolean verbose = line.hasOption("v");
    
    BlockLocation[] badBlocks = new BlockLocation[args.length - 1];
    for (int i = 1; i < args.length; i++) {
        if (!args[i].matches("\\d+,\\d+")) {
          System.err.println("Invalid offset,len pair: " + args[i]);
          return 1;
        }
        String[] loc = args[i].split(",");
        // ensure block file exists
        String blockFile = prefix + "-" + loc[0] + "-" + loc[1];
        if (!new File(blockFile).exists()) {
          System.err.println("Block file does not exist: " + blockFile);
          return 1;
        }
        badBlocks[i - 1] = new BlockLocation(Long.valueOf(loc[0]), Long.valueOf(loc[1]));
    }
    
    Path path = new Path(inFile);
    FileSystem srcFs = path.getFileSystem(getConf());
    
    // check first if file is accessible in hadoop
    FileStatus fileStatus;
    try {fileStatus = srcFs.getFileStatus(path);}
    catch (IOException e) {
      System.err.println(e.getMessage());
      return 1;
    }
    if (fileStatus.isDir()) {
      System.err.println("Infile must be a file: " + path);
      return 1;
    }
      
    //long fileSize = srcFs.getFileStatus(path).getLen();
    //long blockSize = srcFs.getFileStatus(path).getBlockSize();
    // use checksum size as buffer size since block is guaranteed
    // to be an integer multiple of it
    int bufSize = getConf().getInt("io.bytes.per.checksum", 4096);
    
    byte[] buffer = new byte[bufSize];
    
    FSDataInputStream inHd = null;
    FileInputStream inLocal = null;
    FileOutputStream out = null;
    try {
      inHd = srcFs.open(path);
      out = new FileOutputStream(outFile);
      
      long curPos = 0;
      
      if (verbose)
        System.out.println("cur pos: " + curPos);
      
      for (BlockLocation bl : badBlocks) {
        long numBytes = bl.offset - inHd.getPos();
        if (verbose)
          System.out.println("copying " + numBytes + " bytes from hadoop");
        
        for (long i = 0; i < numBytes / bufSize; i++) {
          inHd.read(buffer);
          out.write(buffer);
        }
        inHd.skip(bl.nBytes);
        curPos += numBytes;
        if (verbose)
          System.out.println("cur pos: " + curPos);
        
        // now read in the good bytes from local file        
        String blockFile = prefix + "-" + bl.offset + "-" + bl.nBytes;
        if (verbose)
          System.out.println("copying " + bl.nBytes + " bytes from " + blockFile);
        
        inLocal = new FileInputStream(blockFile);
        
        for (long i = 0; i < bl.nBytes / bufSize; i++) {
          inLocal.read(buffer);
          out.write(buffer);
        }
        // truncate to get correct position
        curPos += bl.nBytes / bufSize * bufSize;
        
        // get the remainder if any (only should happen if last bytes of file were bad)
        if (bl.nBytes % bufSize != 0) {
          int bytesRead;
          while ((bytesRead = inLocal.read(buffer)) >= 0)
            out.write(buffer, 0, bytesRead);
          curPos += bl.nBytes % bufSize;
        }
        if (verbose)
          System.out.println("cur pos: " + curPos);
        inLocal.close();
      }
      // if there are any good bytes left in hadoop read the rest
      int bytesRead;
      long oldPos = curPos;
      while ((bytesRead = inHd.read(buffer)) >= 0) {
        out.write(buffer, 0, bytesRead);
        curPos += bytesRead;
      }
      if (verbose && curPos > oldPos) {
        System.out.println("copied remaining " + (curPos - oldPos) + " bytes from hadoop");
      }
    
    }
    finally {
      if (inHd != null)
        inHd.close();
      if (inLocal != null)
        inLocal.close();
      if (out != null)
        out.close();
    }
    return 0;
  }

  private static void printHelp() {
    System.out.println("Usage: hadoop jar hdtools.jar CMD\n\n" +
        "where CMD is one of the following:\n" +
        "    " + REPAIR_USAGE);
  }
  
  public int run(String argv[]) throws IOException {
    // comment out for now, not sure what logging it suppresses
    //getConf().setQuietMode(true);
    if (argv.length < 1) {
      printHelp();
      return 1;
    }
    String[] cmdArgv = new String[argv.length - 1];

    System.arraycopy(argv, 1, cmdArgv, 0, cmdArgv.length);
    
    if (argv[0].equals("repair"))
      return repair(cmdArgv);
    if (argv[0].equals("explode"))
      return explode(cmdArgv);
    
    printHelp();
    return 1;
  }

  public static void main(String[] argv) throws Exception {
    HadoopTools ht = new HadoopTools();
    
    int res = ToolRunner.run(ht, argv);
    
    System.exit(res);
  }
}

