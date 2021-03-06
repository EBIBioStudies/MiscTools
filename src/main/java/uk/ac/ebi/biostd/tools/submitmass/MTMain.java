package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import uk.ac.ebi.biostd.util.StringUtils;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.InvalidOptionSpecificationException;

public class MTMain
{
 public static final int FILE_TERM_TIMEOUT_SEC=60*60*24;
 public static final int SUBM_TERM_TIMEOUT_SEC=60*60;

 public static final long lockInterval = 5*60*1000;
 
 public static void main(String[] args)
 {
  Config config = null;

  try
  {
   config = CliFactory.parseArguments(Config.class, args);
  }
  catch(HelpRequestedException e)
  {
   usage();
   System.exit(1);
  }
  catch(InvalidOptionSpecificationException | ArgumentValidationException e)
  {
   System.err.println("Command line processing ERROR: " + e.getMessage());
   usage();
   System.exit(1);
  }

  if(config.getFiles() == null || config.getFiles().size() != 2)
  {
   System.err.println("Command line processing ERROR: invalid number of files specified");
   usage();
   System.exit(1);
  }

  Operation op = null;
  
  for( Operation o : Operation.values() )
  {
   if( o.name().equals(config.getOpeartion()) )
   {
    op = o;
    break;
   }
  }
  
  if( op == null )
  {
   System.err.println("Invalid operation: "+config.getOpeartion()+" Possible values: "+Arrays.asList(Operation.values()) );
   usage();
   System.exit(1);
  }
  
  File srcPath = new File( config.getFiles().get(0) );

  Collection<File> fileList = new ArrayList<File>();
  
  if( srcPath.isDirectory() )
  {
   if( config.getFileNamePattern() != null && config.getFileNamePattern().length() > 0 )
    fileList.addAll( Arrays.asList( srcPath.listFiles( (FilenameFilter)new WildcardFileFilter(config.getFileNamePattern()) ) ) );
   else
    fileList.addAll( Arrays.asList( srcPath.listFiles() ) );
  }
  else
   fileList.add(srcPath);
  
  if( ! config.getMaturationTimeHours().equals("0") )
  {
   long mttime = -1;
   
   try
   {
    mttime = Integer.parseInt(config.getMaturationTimeHours());
   }
   catch( Throwable t )
   {
   }
   
   if( mttime <= 0 )
   {
    System.err.println("Invalid maturation time: "+config.getMaturationTimeHours() );
    System.exit(1);
   }
   
   mttime = System.currentTimeMillis()-mttime*60*60*1000L;
   
   Iterator<File> flIter = fileList.iterator();
   
   while( flIter.hasNext() )
   {
    File f = flIter.next();
    
    if( f.lastModified() > mttime )
    {
     flIter.remove();
     System.out.println("Skipping immature file: "+f);
    }
   }
   
  }
  
  
  Path outDir = FileSystems.getDefault().getPath(config.getFiles().get(1) );
  
  if( ! Files.isDirectory( outDir) )
  {
   try
   {
    Files.createDirectories(outDir);
   }
   catch( IOException e )
   {
    System.out.println("Can't create output directory: "+e.getMessage());
    return;
   }
  }

  Iterator<File> fiter = fileList.iterator();
  
  while( fiter.hasNext() )
  {
   File f = fiter.next();
   
   if( f.isDirectory() )
    fiter.remove();
  }
    
  
  final Map<String, SubmissionPointer> sbmMap;
  
  if( config.getMapFile() != null && config.getMapFile().length() > 0  )
  {
   sbmMap = Collections.synchronizedMap(new HashMap<String, SubmissionPointer>());

   Path mpp = FileSystems.getDefault().getPath(config.getMapFile());
   
   if( ! Files.exists(mpp) )
   {
    System.out.println("Can't open submission mapping file: "+mpp);
    return;
   }
   
   try
   {
    Files.lines(mpp).forEach( new Consumer<String>()
    {
     @Override
     public void accept(String t)
     {
      String[] parts = t.split("\t");
      
      if( parts.length != 3 )
      {
       System.out.println("Invalid submission map file. Line: "+t);
       System.exit(1);
      }
      
      int ord = -1;
      
      try
      {
       ord = Integer.parseInt(parts[2]);
      }
      catch(Exception e)
      {
       System.out.println("Invalid submission map file. Line: "+t);
       System.exit(1);
      }
      
      sbmMap.put(parts[0], new SubmissionPointer(parts[1],ord) );
     }
    });
   }
   catch(IOException e)
   {
    System.out.println("IO error while reading submission mapping file");
    System.exit(1);
   }
   
  }
  else
   sbmMap = null;

  String lockerName = config.getLockerName();
  
//  if( lockerName.length() == 0 )
//   lockerName = UUID.randomUUID().toString();

  if( lockerName.length() == 0 )
   lockerName=null;
  
  long lastLockTime = 0;
  
  String sessId = login(config);

  int nFileProc = config.getParallelFiles();
  
  if( nFileProc > fileList.size() )
   nFileProc = fileList.size();

  int nSbmProc = config.getParallelSubmitters();

  BlockingQueue<SubmitRequest> sbmQueue = new ArrayBlockingQueue<SubmitRequest>(nSbmProc*2);
  BlockingQueue<FileRequest> fileQueue = new ArrayBlockingQueue<FileRequest>(nFileProc);

  ExecutorService sbmExec = Executors.newFixedThreadPool(nSbmProc);
  
  for( int i=1; i <= nSbmProc; i++ )
  {
   SubmitTask st = new SubmitTask("STsk"+i, sbmQueue, config, outDir, op, sessId);
   
   sbmExec.submit(st);
  }
  
  
  ExecutorService fileExec = Executors.newFixedThreadPool(nFileProc);
  
  AtomicInteger count = null;
  
  if( config.getLimit() > 0 )
   count = new AtomicInteger();
  
  for( int i=1; i <= nFileProc; i++ )
  {
   FileProcessor fp = new FileProcessor("FProc"+i, fileQueue, sbmQueue, count, outDir, sbmMap, config);
   
   fileExec.submit(fp);
  }

  int i=0;
  for( File file :  fileList )
  {
   if( count != null && count.get() >= config.getLimit() )
    break;
    
   i++;
   
   if( (System.currentTimeMillis() - lastLockTime ) > lockInterval )
   {
    lastLockTime = System.currentTimeMillis();
    lockExport(config, lockerName, sessId, true);
   }
   
   FileRequest freq = new FileRequest();
   
   freq.setFile(file);
   freq.setOrder(i);
   freq.setTotal(fileList.size());
   freq.setRequestId(file.getName());
   
   putToQueue(fileQueue,freq);
  }
  
  putToQueue(fileQueue,new FileRequest());
  
  fileExec.shutdown();
  System.out.println("Waiting for file thread pool termination");
  
  
  int timeout = FILE_TERM_TIMEOUT_SEC;
  long stime = System.currentTimeMillis();
  
  boolean termOk=false;
  
  while( timeout > 0 )
  {
   try
   {
    if( fileExec.awaitTermination(lockInterval, TimeUnit.MILLISECONDS) )
    {
     termOk = true;
     break;
    }
    
    lockExport(config, lockerName, sessId, true);
   }
   catch(InterruptedException e)
   {
   }

   timeout = FILE_TERM_TIMEOUT_SEC - (int)(System.currentTimeMillis()-stime)/1000;
  }
  
  if( ! termOk )
   System.err.println("File processors pool termination timeout exeeded");
  
  putToQueue(sbmQueue,new SubmitRequest());
  
  if( timeout <=0 )
   System.out.println("File thread pool termination failed");
  
  sbmExec.shutdown();
  System.out.println("Waiting for submission thread pool termination");
  
  timeout = SUBM_TERM_TIMEOUT_SEC;
  stime = System.currentTimeMillis();

  termOk=false;
  
  while( timeout > 0 )
  {
   try
   {
    if( sbmExec.awaitTermination(lockInterval, TimeUnit.MILLISECONDS) )
    {
     termOk = true;
     break;
    }
    
    lockExport(config, lockerName, sessId, true);
   }
   catch(InterruptedException e)
   {
   }

   timeout = SUBM_TERM_TIMEOUT_SEC - (int)(System.currentTimeMillis()-stime)/1000;
  }
  
  if( ! termOk )
   System.err.println("Submission pool termination timeout exeeded");

  lockExport(config, lockerName, sessId, false);
  
  if( timeout <=0 )
   System.out.println("Submission thread pool termination failed");
  else
   System.out.println("All done. Finishing");
  
 }
 
 private static <T> void putToQueue( BlockingQueue<T> que, T freq )
 {
  while(true)
  {
   try
   {
    que.put(freq);
    break;
   }
   catch(InterruptedException e)
   {
   }
  }
 }
 
 private static void usage()
 {
  System.err.println("java -cp BioStdTools.jar uk.ac.ebi.biostd.tools.submitmass.MTMain -q <operation> -s serverURL -u user -p [password] [options] <input file|input dir> <log dir>");
  System.err.println("-s or --server server endpoint URL");
  System.err.println("-u or --user user login");
  System.err.println("-p or --password user password");
  System.err.println("-n or --limit load by the first N submissions only");
  System.err.println("-t or --fileNamePattern process only files that match a pattern. Example: -t '*.cvs'");
  System.err.println("-f or --forcePublic make all submissions public regardless of presence of the 'Public' tag in source files");
  System.err.println("-a or --attachTo force all submission be attached to the specified submission. Normally 'AttachTo' attribute should be used for this purpose");
  System.err.println("-i or --parallelFiles defines a number of input files to be processed in parallel");
  System.err.println("-o or --parallelSubmitters defines a number of submission tasks that will be run in parallel");
  System.err.println("-r or --setPartitionedRootPath force a submission root path by partitioning accession number");
  System.err.println("-e or --dontUseSecAccno remove root section accno. The submission accno will be used instead");
  System.err.println("-d or --outputDirPerFile create a separate log dir for each input file. Useful when input files contain multiple submissions");
  System.err.println("-q or --operation set a query operation. One of:  create, createupdate, update, override, createoverride");
  System.err.println("-m or --mapFile use mapping file created by the mapping tool to avoid resubmitting the same submissions");
  System.err.println("-w or --downloadEPMCDir download EPMC attachment file into the specified directory");
  System.err.println("-l or --refreshFiles download EPMC files even if they are exist");
  System.err.println("-c or --removeDuplicates submit only the last submission if the same accession repeats in a source file");
  System.err.println("      --maturationTimeHours ignore files that are not older than a specified interval");
  System.err.println("-x or --fixCharset try to fix some charset problems (like double conversion)");
  System.err.println("-b or --onBehalf make submission on behalf of other user. Only superuser can do this");
  System.err.println("-v or --validateOnly simulate submission process with no DB changed");
  System.err.println("      --ignoreAbsentFiles allow submissions with unresolved file references");
  System.err.println("      --lockerName unique name for backend export locking");
 }
 
 private static void lockExport( Config config, String lckName, String sessionId, boolean lock )
 {
  if( lckName == null )
   return;
  
  String appUrl = config.getServer();

  if(!appUrl.endsWith("/"))
   appUrl = appUrl + "/";
  
  URL loginURL = null;

  String lckEndpoint = lock?Config.exportLockEndpoint:Config.exportUnlockEndpoint;
  
  try
  {
   loginURL = new URL(appUrl + lckEndpoint + "?locker=" + URLEncoder.encode(lckName, "utf-8") );
  }
  catch(MalformedURLException e)
  {
   System.err.println("Invalid server URL: " + config.getServer());
   System.exit(1);
  }
  catch(UnsupportedEncodingException e)
  {
  }

  try
  {
   HttpURLConnection conn = (HttpURLConnection) loginURL.openConnection();
   
   conn.addRequestProperty("X-Session-Token", sessionId);
   
   String resp = StringUtils.readFully((InputStream)conn.getContent(), Charset.forName("utf-8"));

   conn.disconnect();

   if( ! resp.startsWith("OK") )
    System.err.println((lock?"Lock":"Unlock")+" failed");
   
  }
  catch(IOException e)
  {
   System.err.println("Connection to server (lock operation) '"+config.getServer()+"' failed: "+e.getMessage());
   System.exit(1);
  }
  
 } 
 
 
 private static String login( Config config )
 {
  String appUrl = config.getServer();

  if(!appUrl.endsWith("/"))
   appUrl = appUrl + "/";
  
  URL loginURL = null;

  String password = "";
  
  if( config.getPassword() != null )
  {
   if( config.getPassword().size() > 0 )
    password=config.getPassword().get(0);
   else
   {
    try
    {
     password = new String( System.console().readPassword("Password for %s: ",config.getUser()) );
    }
    catch(IOError e)
    {
     System.err.println("Can't read password from input stream: "+e.getMessage());
     System.exit(1);
    }
   }
  }
  
  
  try
  {
   loginURL = new URL(appUrl + Config.authEndpoint + "?login=" + URLEncoder.encode(config.getUser(), "utf-8") + "&password="
     + URLEncoder.encode(password, "utf-8"));
  }
  catch(MalformedURLException e)
  {
   System.err.println("Invalid server URL: " + config.getServer());
   System.exit(1);
  }
  catch(UnsupportedEncodingException e)
  {
  }

  try
  {
   HttpURLConnection conn = (HttpURLConnection) loginURL.openConnection();
   String resp = StringUtils.readFully((InputStream)conn.getContent(), Charset.forName("utf-8"));

   conn.disconnect();

   if( ! resp.startsWith("OK") )
   {
    System.err.println("Login failed");
    System.exit(1);
   }
   
   String sessId = getSessId(resp);
   
   if( sessId == null )
   {
    System.err.println("Invalid server response. Can't extract session ID");
    System.exit(1);
   }
   
   return sessId;
  }
  catch(IOException e)
  {
   System.err.println("Connection to server '"+config.getServer()+"' failed: "+e.getMessage());
   System.exit(1);
  }
  
  return null;
 }
 
 private static String getSessId(String resp)
 {
  int pos  = resp.indexOf("sessid:");
  
  if( pos == -1 )
   return null;
  
  pos += "sessid:".length();
  
  while( Character.isWhitespace(resp.charAt(pos)))
   pos++;
  
  int beg = pos;
  
  while( Character.isLetterOrDigit(resp.charAt(pos)))
   pos++ ;

  
  return resp.substring(beg,pos);
 }
}
