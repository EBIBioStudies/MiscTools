package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
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

import org.apache.commons.io.filefilter.WildcardFileFilter;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.model.Submission;
import uk.ac.ebi.biostd.model.SubmissionAttributeException;
import uk.ac.ebi.biostd.out.DocumentFormatter;
import uk.ac.ebi.biostd.out.json.JSONFormatter;
import uk.ac.ebi.biostd.tools.fetchpmc.CVSTVSParse;
import uk.ac.ebi.biostd.tools.partdir.AccNoUtil;
import uk.ac.ebi.biostd.treelog.ConvertException;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.JSON4Report;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;
import uk.ac.ebi.biostd.treelog.SubmissionReport;
import uk.ac.ebi.biostd.treelog.Utils;
import uk.ac.ebi.biostd.util.DataFormat;
import uk.ac.ebi.biostd.util.StringUtils;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.InvalidOptionSpecificationException;

public class Main
{


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
   if( config.getFileNamePattern() != null && config.getFileNamePattern().length() > 0 )
    fileList.addAll( Arrays.asList( srcPath.listFiles( (FilenameFilter)new WildcardFileFilter(config.getFileNamePattern()) ) ) );
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

    
  String sessId = login(config);

  int fcnt=0;
  
  for( File f : fileList )
  {
   fcnt++;
   
//   System.out.println("Processing file: "+f.getName()+" ("+fcnt+" of "+fileList.size()+")");
   
   if( ! f.isDirectory() )
    processFile(f, outDir, sessId,config);
  }
 }

 
 private static void processFile( File file, Path outDir, String sessId, Config config )
 {
  ErrorCounter ec = new ErrorCounterImpl();
  SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + file + "'", ec);

  AccessTag publicTag = new AccessTag();
  publicTag.setName("Public");
  
  PMDoc doc = CVSTVSParse.parse(file, "utf-8", '\t', topLn);

  if( doc == null )
  {
   System.err.println("Parser can't read a file: "+file);
   return;
  }
  
//  System.out.println("Read "+doc.getSubmissions().size()+" submission");

  if( config.getFixCharset() )
  {
   for(SubmissionInfo si : doc.getSubmissions() )
    CharsetFix.fixSubmissionCharset(si);
  }
  
  SimpleLogNode.setLevels(topLn);
  
  if( topLn.getLevel().getPriority() >= Level.ERROR.getPriority() )
  {
   System.err.println("Input file parse error");
   
   try( PrintWriter errOut = new PrintWriter( outDir.resolve(file.getName()+".FAIL").toFile() )  ) 
   {
    Utils.printLog(topLn, errOut, Level.WARN  );
   }
   catch( IOException e)
   {
    e.printStackTrace();
   }
   
   return;
  }
  
  
  int i=0;
  int n = doc.getSubmissions().size();
  
  for( SubmissionInfo si : doc.getSubmissions() )
  {
   i++;

   if( config.getLimit() > 0 && i > config.getLimit() )
    break;
   
   
   String acc = si.getSubmission().getRootSection().getAccNo();
   
   if( acc == null )
    acc = si.getSubmission().getAccNo();
   
   if( acc.startsWith("!") )
    acc = acc.substring(1);
   
   
   System.out.print("Submission "+acc+" ("+i+" of "+n+") ");
   
   Path okFile = outDir.resolve(acc+".OK");
   Path failFile = outDir.resolve(acc+".FAIL");
   
   if( Files.exists(okFile) )
   {
    System.out.println("SKIP");
    continue;
   }
   
   if( config.getAttachTo() != null && config.getAttachTo().trim().length() > 0 )
    si.getSubmission().addAttribute(Submission.canonicAttachToAttribute, config.getAttachTo().trim());
   
   try
   {
    si.getSubmission().normalizeAttributes();
   }
   catch(SubmissionAttributeException e)
   {

    PrintWriter out;
    
    try
    {
     out = new PrintWriter( failFile.toFile() );
     e.printStackTrace(out);
     out.close();
    }
    catch(FileNotFoundException e1)
    {
     e1.printStackTrace();
    }
    
    continue;
   }
   
   si.getSubmission().setAccNo(acc);
   si.setAccNoOriginal(acc);
   si.getSubmission().setRootPath(AccNoUtil.getPartitionedPath(acc));
   si.getRootSectionOccurance().setOriginalAccNo(null);
   si.getRootSectionOccurance().setPrefix(null);
   si.getRootSectionOccurance().setSuffix(null);
   si.getSubmission().getRootSection().setAccNo(null);
   
   if( config.isForcePublic() )
   {
    boolean isPublic=false;
    if( si.getSubmission().getAccessTags() != null )
    {
     for( AccessTag at : si.getSubmission().getAccessTags() )
     {
      if( at.getName().equals(publicTag.getName()) )
      {
       isPublic = true;
       break;
      }
     }
    }
    
    if( ! isPublic )
     si.getSubmission().addAccessTag(publicTag);
    
   }
   
   
//   si.getSubmission().setRootPath(acc.substring(acc.indexOf("-")+1));
   
   StringBuilder sb = new StringBuilder();
   DocumentFormatter outfmt = new JSONFormatter(sb);
   
   
   PMDoc cdoc = new PMDoc();
   
   cdoc.addSubmission(si);
   
   try
   {
    outfmt.format(cdoc);
   }
   catch(IOException e)
   {
   }
  
   SubmissionReport rep = submit(sb.toString(), DataFormat.json, sessId, config);
   
   File logFile = null;
   
   if( rep.getLog().getLevel() == Level.ERROR )
   {
    logFile = failFile.toFile();
    System.out.println(" FAIL");
   }
//   else if( rep.getLog().getLevel() == Level.WARN )
//   {
//    logFile = outDir.resolve(acc+".WARN");
//    System.out.println(" WARN");
//   }
   else
   {
    System.out.println(" OK");
   
    if( Files.exists(failFile) )
     try
     {
      Files.delete(failFile);
     }
     catch(IOException e)
     {
     }

    logFile = okFile.toFile();
   }
   
   try
   {
    PrintStream outs = new PrintStream( logFile, "utf-8" );
    Utils.printLog(rep.getLog(), outs, Level.INFO  );
    outs.close();
   }
   catch(FileNotFoundException e)
   {
    System.err.println("File not found "+logFile );
   }
   catch(UnsupportedEncodingException e)
   {
    System.err.println("UTF-8 is not supported");
   }
   catch(IOException e)
   {
    System.err.println("IO error: "+e.getMessage());
   }
   

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
 
 private static SubmissionReport submit(String data, DataFormat fmt, String sess, Config config)
 {
  String appUrl = config.getServer();

  if(!appUrl.endsWith("/"))
   appUrl = appUrl + "/";

  appUrl += "submit/create";
  
  URL loginURL = null;

  
  try
  {
   loginURL = new URL(appUrl  + "?"+Config.SessionKey+"="+URLEncoder.encode(sess, "utf-8"));
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
   
   
   conn.setDoOutput(true);
   conn.setRequestMethod("POST");
   
   conn.setRequestProperty("Content-Type", fmt.getContentType()+"; charset=utf-8");
   
   
   byte[] postData   = data.getBytes( Charset.forName( "UTF-8" ));
   
   conn.setRequestProperty("Content-Length", String.valueOf(postData.length));
   conn.getOutputStream().write( postData );
   conn.getOutputStream().close();
   

   
   String resp = StringUtils.readFully((InputStream)conn.getContent(), Charset.forName("utf-8"));

   conn.disconnect();

   
   try
   {
    return JSON4Report.convert(resp);
   }
   catch(ConvertException e)
   {
    System.err.println("Invalid server response. JSON log expected");
    System.exit(1);
   }
   
   
  }
  catch(IOException e)
  {
   System.err.println("Connection to server '"+config.getServer()+"' failed: "+e.getMessage());
   System.exit(1);
  }
  
  return null;
 }
 
 private static void usage()
 {
  System.err.println("Invalid usage");
 }
}
