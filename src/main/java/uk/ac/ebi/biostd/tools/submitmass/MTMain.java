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
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  File srcPath = new File( config.getFiles().get(0) );

  Collection<File> fileList = new ArrayList<File>();
  
  if( srcPath.isDirectory() )
  {
   if( config.getFileTemplate() != null && config.getFileTemplate().length() > 0 )
    fileList.addAll( Arrays.asList( srcPath.listFiles( (FilenameFilter)new WildcardFileFilter(config.getFileTemplate()) ) ) );
   else
    fileList.addAll( Arrays.asList( srcPath.listFiles() ) );
  }
  else
   fileList.add(srcPath);
  
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
   SubmitTask st = new SubmitTask("STsk"+i, sbmQueue, config, outDir, sessId);
   
   sbmExec.submit(st);
  }
  
  
  ExecutorService fileExec = Executors.newFixedThreadPool(nFileProc);
  
  for( int i=1; i <= nFileProc; i++ )
  {
   FileProcessor fp = new FileProcessor("FProc"+i, fileQueue, sbmQueue, outDir);
   
   fileExec.submit(fp);
  }

  int i=0;
  for( File file :  fileList )
  {
   i++;
   
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
  while( timeout > 0 )
  {
   try
   {
    fileExec.awaitTermination(timeout, TimeUnit.SECONDS);
    break;
   }
   catch(InterruptedException e)
   {
    timeout = FILE_TERM_TIMEOUT_SEC - (int)(System.currentTimeMillis()-stime)/1000;
   }
  }
  
  putToQueue(sbmQueue,new SubmitRequest());
  
  if( timeout <=0 )
   System.out.println("File thread pool termination failed");
  
  sbmExec.shutdown();
  System.out.println("Waiting for submission thread pool termination");
  
  timeout = SUBM_TERM_TIMEOUT_SEC;
  stime = System.currentTimeMillis();
  while( timeout > 0 )
  {
   try
   {
    sbmExec.awaitTermination(timeout, TimeUnit.SECONDS);
    break;
   }
   catch(InterruptedException e)
   {
    timeout = SUBM_TERM_TIMEOUT_SEC - (int)(System.currentTimeMillis()-stime)/1000;
   }
  }
  
  if( timeout <=0 )
   System.out.println("Submission thread pool termination failed");

  
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
  System.err.println("Invalid usage");
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
