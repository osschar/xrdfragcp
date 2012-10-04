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
  
  private void tail(String[] argv) throws IOException,ParseException {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("f", false, "follow");
    CommandLine line = parser.parse(options, argv);
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

  private void repair(String[] argv) throws IOException,ParseException {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("p", "prefix", true, "prefix");
    options.addOption("o", "outfile", true, "outfile");
    CommandLine line = parser.parse(options, argv);
    
    String[] args = line.getArgs();
    String inFile = args[0];
    
    String basename = new File(inFile).getName(); // need basename for later
    
    String prefix;
    String outFile;
    if (line.hasOption("p")) {
      prefix = line.getOptionValue("p");
    }
    else {
      prefix = basename;
    }
    if (line.hasOption("o"))
      outFile = line.getOptionValue("o");
    else
      outFile = basename;
    
    //String mapFile = argv[0];
    //String inFile = argv[1];
    //String outFile = argv[2];
    
    //HashMap<String, BlockLocation[]> blockMap = parseMapFile(mapFile);
    
    BlockLocation[] badBlocks = new BlockLocation[args.length - 1];
    for (int i = 1; i < args.length; i++) {
        String[] loc = args[i].split(",");
        badBlocks[i - 1] = new BlockLocation(Long.valueOf(loc[0]), Long.valueOf(loc[1]));
    }
    
    Path path = new Path(inFile);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.getFileStatus(path).isDir()) {
      throw new IOException("Source must be a file.");
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
      
      System.out.println("init pos:" + inHd.getPos());
      
      for (BlockLocation bl : badBlocks) {
        System.out.println("offset:" + bl.offset);
       
        long numToRead =  (bl.offset - inHd.getPos()) / bufSize;
        for (long i = 0; i < numToRead; i++) {
          inHd.read(buffer);
          out.write(buffer);
          System.out.println("write; pos:" + inHd.getPos());
        }
        inHd.skip(bl.nBytes);
        System.out.println("skip; pos:" + inHd.getPos());
        
        // now read in the good bytes from local file
        inLocal = new FileInputStream(prefix + "-" + bl.offset + "-" + bl.nBytes);
        
        for (long i = 0; i < bl.nBytes / bufSize; i++) {
          inLocal.read(buffer);
          out.write(buffer);
        }
        
        // get the remainder if any (only should happen if last bytes of file were bad)
        if (bl.nBytes % bufSize != 0) {
          int bytesRead;
          while ((bytesRead = inLocal.read(buffer)) >= 0)
            out.write(buffer, 0, bytesRead);
        }
        
        inLocal.close();
      }
      // if there are any good bytes left in hadoop read the rest
      int bytesRead;
      while ((bytesRead = inHd.read(buffer)) >= 0)
        out.write(buffer, 0, bytesRead);
    
    }
    finally {
      if (inHd != null)
        inHd.close();
      if (inLocal != null)
        inLocal.close();
      if (out != null)
        out.close();
    }
  }

  public int run(String argv[]) throws ParseException {
    // comment out for now, not sure what logging it suppresses
    //getConf().setQuietMode(true);
    if (argv.length < 1) {
      printHelp();
      return 1;
    }
    String[] cmdArgv = new String[argv.length - 1];

    System.arraycopy(argv, 1, cmdArgv, 0, cmdArgv.length);
    try {
      if (argv[0].equals("tail"))
        tail(cmdArgv);
      else if (argv[0].equals("repair"))
        repair(cmdArgv);
      else if (argv[0].equals("explode"))
        explode(cmdArgv); 
      else {
        printHelp();
        return 1;
      }
    }
    catch (IllegalArgumentException e) {
      System.err.println(argv[0] + ": " + e.getMessage());
      return 1;
    }
    catch (IOException e) {
      System.err.println(argv[0] + ": " + e.getMessage());
      return 1;
    }
    return 0;
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

