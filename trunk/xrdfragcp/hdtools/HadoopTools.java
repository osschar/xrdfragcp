package hdtools;

//import java.io.File;
//import java.io.FileNotFoundException;
import java.io.*;
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.net.URI;
//import java.text.DecimalFormat;
//import java.text.NumberFormat;
//import java.text.SimpleDateFormat;
import java.util.*;
//import java.util.zip.GZIPInputStream;
//
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.shell.CommandFormat;
//import org.apache.hadoop.fs.shell.Count;
//import org.apache.hadoop.io.DataInputBuffer;
//import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.CompressionCodecFactory;
//import org.apache.hadoop.ipc.RPC;
//import org.apache.hadoop.ipc.RemoteException;
//import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.conf.*;
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
  
  private static HashMap<String, BlockLocation[]> parseMapFile(String mapFile) throws IOException {
    HashMap<String, BlockLocation[]> blockMap = new HashMap<String, BlockLocation[]>();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(mapFile));
  
      String line;
      while ((line = br.readLine()) != null) {
        //ArrayList<BlockLocation> blockLocs = new ArrayList<BlockLocation>();
        String[] fields = line.split("\\s");
        String filename = fields[0];
        BlockLocation[] blockLocs = new BlockLocation[fields.length - 1];
        for (int i = 1; i < fields.length; i++) {
          String[] loc = fields[i].split(",");
          blockLocs[i - 1] = new BlockLocation(Long.valueOf(loc[0]), Long.valueOf(loc[1]));
        }
        blockMap.put(filename, blockLocs);
      }
    }
    finally {
      if (br != null)
        br.close();
    }
    return blockMap;
  }
  
  private void tail(String[] argv) throws IOException {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("f", false, "follow");
    CommandLine line;
    try {line = parser.parse(options, argv);}
    catch (ParseException e) {return;}
    //CommandFormat c = new CommandFormat("tail", 1, 1, "f");
    String src = null;
    Path path = null;

    src = line.getArgs()[0];
    //List<String> parameters = c.parse(argv, pos);
    //src = parameters.get(0);
 
    //boolean foption = c.getOpt("f") ? true: false;
    boolean foption = line.hasOption("f") ? true: false;
    
    path = new Path(src);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.getFileStatus(path).isDir()) {
      throw new IOException("Source must be a file.");
    }

    long fileSize = srcFs.getFileStatus(path).getLen();
    long offset = (fileSize > 1024) ? fileSize - 1024: 0;

    while (true) {
      FSDataInputStream in = null;
      try {
        in = srcFs.open(path);
        in.seek(offset);
        IOUtils.copyBytes(in, System.out, 1024, false);
        offset = in.getPos();
      }
      finally {
         if (in != null)
           in.close();
      }
      if (!foption) {
        break;
      }
      fileSize = srcFs.getFileStatus(path).getLen();
      offset = (fileSize > offset) ? offset: fileSize;
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException e) {
        break;
      }
    }
  }
  
  private void explode(String[] argv) throws IOException {
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
  }

  private int repair(String[] argv) throws IOException {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("p", "prefix", true, "prefix");
    options.addOption("o", "outfile", true, "outfile");
    options.addOption("v", false, "vebose");
    //CommandLine line = parser.parse(options, argv);
    CommandLine line;
    try {line = parser.parse(options, argv);}
    catch (ParseException e) {
      System.err.println(e.getMessage());
      return 1; 
    }
    
    String[] args = line.getArgs();
    String inFile = args[0];
    
    String basename = new File(inFile).getName(); // need basename for later
    
    String prefix = line.hasOption("p") ? line.getOptionValue("p") : basename;
    String outFile = line.hasOption("o") ? line.getOptionValue("o") : basename;
    boolean verbose = line.hasOption("v");
    
    //String mapFile = argv[0];
    //String inFile = argv[1];
    //String outFile = argv[2];
    
    //HashMap<String, BlockLocation[]> blockMap = parseMapFile(mapFile);
    
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
      System.err.println("Source must be a file: " + path);
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

  public int run(String argv[]) throws IOException {
    // comment out for now, not sure what logging it suppresses
    //getConf().setQuietMode(true);
    if (argv.length < 1) {
      printHelp();
      return 1;
    }
    String[] cmdArgv = new String[argv.length - 1];

    System.arraycopy(argv, 1, cmdArgv, 0, cmdArgv.length);
    
    int returnCode = 0;
    
    if (argv[0].equals("tail"))
      tail(cmdArgv);
    else if (argv[0].equals("repair"))
      returnCode = repair(cmdArgv);
    else if (argv[0].equals("explode"))
      explode(cmdArgv); 
    else {
      printHelp();
      return 1;
    }
    return returnCode;
  }
  
  public void printHelp() {
    System.out.println("Usage: hadoop jar hdtools.jar CMD\n\n" +
        "where CMD is one of the following:\n" +
    		"    tail FILE");
  }

  public static void main(String[] argv) throws Exception {
    HadoopTools ht = new HadoopTools();
    
    int res = ToolRunner.run(ht, argv);
    
    System.exit(res);
  }
}

