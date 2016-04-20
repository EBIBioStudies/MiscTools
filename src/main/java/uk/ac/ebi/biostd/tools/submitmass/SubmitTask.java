package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.in.AccessionMapping;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.SubmissionMapping;
import uk.ac.ebi.biostd.in.pagetab.FileOccurrence;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.model.Submission;
import uk.ac.ebi.biostd.model.SubmissionAttributeException;
import uk.ac.ebi.biostd.out.DocumentFormatter;
import uk.ac.ebi.biostd.out.json.JSONFormatter;
import uk.ac.ebi.biostd.tools.partdir.AccNoUtil;
import uk.ac.ebi.biostd.treelog.ConvertException;
import uk.ac.ebi.biostd.treelog.JSON4Report;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SubmissionReport;
import uk.ac.ebi.biostd.treelog.Utils;
import uk.ac.ebi.biostd.util.DataFormat;
import uk.ac.ebi.biostd.util.StringUtils;

public class SubmitTask implements Runnable
{
 static final String EPMCDownloadURLPrefix = "http://www.ebi.ac.uk/europepmc/webservices/rest/";
 static final String EPMCDownloadURLSuffix = "/supplementaryFiles";
 
 private String taskName;

 private BlockingQueue<SubmitRequest> sbmQueue;
 private Config config;
 private Path outDir;
 private Path downloadDir;
 private String sess;
 private Operation op;
 
 
 public SubmitTask(String taskName, BlockingQueue<SubmitRequest> sbmQueue, Config config, Path outDir, Operation op, String sess)
 {
  this.taskName = taskName;
  this.sbmQueue = sbmQueue;
  this.config = config;
  this.outDir = outDir;
  this.sess = sess;
  this.op=op;
  
  if( config.getDownloadEPMCDir() != null && config.getDownloadEPMCDir().length() > 0 )
   downloadDir = FileSystems.getDefault().getPath(config.getDownloadEPMCDir());
   
 }
 
 @Override
 public void run()
 {
  SubmitRequest req = null;
 
  AccessTag publicTag = new AccessTag();
  publicTag.setName("Public");
  
  String srvUrl = config.getServer();
  String appUrl = srvUrl;

  if(!appUrl.endsWith("/"))
   appUrl = appUrl + "/";

  if( op == Operation.create )
   appUrl += Config.submitEndpoint;
  else if( op == Operation.update )
   appUrl += Config.updateEndpoint;
  else if( op == Operation.replace )
   appUrl += Config.replaceEndpoint;
  
  URL loginURL = null;

  
  String attachTo = config.getAttachTo();
  
  if( attachTo != null &&  (attachTo = attachTo.trim()).length() == 0 )
   attachTo=null;
  
  while(true)
  {

   try
   {
    req = sbmQueue.take();
   }
   catch(InterruptedException e2)
   {
    continue;
   }

   if(req.getFileName() == null )
   {
    while(true)
    {
     try
     {
      sbmQueue.put(req);
      return;
     }
     catch(InterruptedException e)
     {
     }
    }
   }

   String logFileName = req.getFileName()+"-"+req.getOrder()+"of"+req.getTotal();
   
   try
   {
    loginURL = new URL(appUrl  + "?"+Config.SessionKey+"="+URLEncoder.encode(sess, "utf-8")+"&"+Config.SubmitRequestID+"="+
      URLEncoder.encode(req.getFileName()+"-"+req.getFileOrder()+"of"+req.getFileTotal()+"-"+req.getOrder()+"of"+req.getTotal(), "utf-8"));
   }
   catch(MalformedURLException e)
   {
    System.err.println("Invalid server URL: " + srvUrl);
    System.exit(1);
   }
   catch(UnsupportedEncodingException e)
   {
   }
   
   
   SubmissionInfo si = req.getSubmissionInfo();
   
   String acc = null;
   
   
   if( config.isDontUseSecAccno() )
    acc = si.getSubmission().getAccNo();
   else
    acc = si.getRootSectionOccurance().getOriginalAccNo();

   if(acc == null)
    acc = si.getAccNoOriginal();

   si.getSubmission().setAccNo(acc);
   si.setAccNoOriginal(acc);

   
//   if(acc != null && acc.startsWith("!"))
//    acc = acc.substring(1);

   
   Path fileOutDir = outDir;
   
   if( config.isOutputDirPerFile() )
   {
    fileOutDir = outDir.resolve( req.getFileName() );
    
    if( ! Files.exists(fileOutDir) )
    {
     try
     {
      Files.createDirectories(fileOutDir);
     }
     catch(IOException e)
     {
      e.printStackTrace();
      return;
     }
    }
   }
   
   Path okFile = fileOutDir.resolve(logFileName + ".OK");
   Path failFile = fileOutDir.resolve(logFileName + ".FAIL");

   if( Files.exists(okFile) )
   {
    Console.println(taskName+": Sbm: "+req+" SKIP");
    continue;
   }

   if( downloadDir != null && req.getSubmissionInfo().getAccNoPrefix() == null && req.getSubmissionInfo().getAccNoSuffix() == null 
     && si.getFileOccurrences() != null && si.getFileOccurrences().size() > 0 )
   {
    boolean filesOK = true;
    
    for(FileOccurrence fo : si.getFileOccurrences() )
    {
     Path filePath = downloadDir.resolve( AccNoUtil.getPartitionedPath(acc) ).resolve(fo.getFileRef().getName());

     if( ! config.getRefreshFiles() && Files.exists(filePath) )
      continue;
     
     try
     {
      filesOK = loadFromWeb(acc, fo.getFileRef().getName(), filePath);
      
      if( ! filesOK )
       break;
     }
     catch(FileNotFoundException e)
     {
      Console.println(taskName+": Sbm: "+req+" loadFromWeb not found: "+e.getMessage());

      filesOK = false;
      break;
     }
     catch(IOException e)
     {
      e.printStackTrace();

      filesOK = false;
      break;
     }
    }
    
    if( ! filesOK )
    {
     try
     {
      downloadEPMCFiles(acc,downloadDir);
     }
     catch(IOException e)
     {
      // TODO Auto-generated catch block
      e.printStackTrace();
     }
    }
    
   }
   
   if(attachTo != null)
    si.getSubmission().addAttribute(Submission.attachToAttribute, attachTo);

   try
   {
    si.getSubmission().normalizeAttributes();
   }
   catch(SubmissionAttributeException e)
   {

    PrintWriter out;

    try
    {
     out = new PrintWriter(failFile.toFile());
     e.printStackTrace(out);
     out.close();
    }
    catch(FileNotFoundException e1)
    {
     e1.printStackTrace();
    }

    continue;
   }

   
   if( ! config.isDontUseSecAccno() )
   {
    si.getRootSectionOccurance().setOriginalAccNo(null);
    si.getRootSectionOccurance().setPrefix(null);
    si.getRootSectionOccurance().setSuffix(null);
    si.getSubmission().getRootSection().setAccNo(null);
   }
   
   if( config.isSetPartitionedRootPath() )
    si.getSubmission().setRootPath(AccNoUtil.getPartitionedPath(acc));

   if(config.isForcePublic())
   {
    boolean isPublic = false;
    if(si.getSubmission().getAccessTags() != null)
    {
     for(AccessTag at : si.getSubmission().getAccessTags())
     {
      if(at.getName().equals(publicTag.getName()))
      {
       isPublic = true;
       break;
      }
     }
    }

    if(!isPublic)
     si.getSubmission().addAccessTag(publicTag);

   }

   //  si.getSubmission().setRootPath(acc.substring(acc.indexOf("-")+1));

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

   File logFile = null;

   SubmissionReport rep;
   try
   {
    rep = submit(sb.toString(), DataFormat.json, loginURL);
   }
   catch(SubmitException e1)
   {
    Console.println(taskName+": Sbm: "+req+" ERROR "+e1.getMessage());
    
    logFile = failFile.toFile();
    try
    {
     PrintStream outs = new PrintStream(logFile, "utf-8");
     e1.printStackTrace(outs);
     outs.close();
    }
    catch(FileNotFoundException | UnsupportedEncodingException e)
    {
     Console.println(taskName+": Sbm: "+req+" File '"+logFile+"' IO error: " + e.getMessage());
    }
    
    continue;
   }


   if(rep.getLog().getLevel() == Level.ERROR)
   {
    logFile = failFile.toFile();
    Console.println(taskName+": Sbm: "+req+" FAIL");
   }
   //  else if( rep.getLog().getLevel() == Level.WARN )
   //  {
   //   logFile = outDir.resolve(acc+".WARN");
   //   System.out.println(" WARN");
   //  }
   else
   {
    Console.println(taskName+": Sbm: "+req+" OK");

    if(Files.exists(failFile))
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
    PrintStream outs = new PrintStream(logFile, "utf-8");
    Utils.printLog(rep.getLog(), outs, Level.INFO);
    printMappings(rep.getMappings(),outs);
    outs.close();
   }
   catch(FileNotFoundException e)
   {
    Console.println(taskName+": Sbm: "+req+" File not found " + logFile);
   }
   catch(UnsupportedEncodingException e)
   {
    Console.println(taskName+": Sbm: "+req+" UTF-8 is not supported");
   }
   catch(IOException e)
   {
    Console.println(taskName+": Sbm: "+req+" IO error: " + e.getMessage());
   }

  }
 }
 
 
 
 private boolean loadFromWeb(String accNo, String relPath, Path filePath) throws IOException
 {
  int n = accNo.lastIndexOf("PMC");
  
  String origAccNo = accNo.substring(n);

  
  URL url = null;
  try
  {
   url = new URL("http://europepmc.org/articles/" + origAccNo + "/bin/" + relPath);
  }
  catch(MalformedURLException e1)
  {
   return false;
  }

  HttpURLConnection conn = null;

  conn = (HttpURLConnection) url.openConnection();

  InputStream nis = conn.getInputStream();

  if( conn.getResponseCode() != HttpURLConnection.HTTP_OK )
  {
   nis.close();
   conn.disconnect();
   return false;
  }
  
  long total = 0;
  int rd;
  
  byte[] readBuffer = new byte[4*1024*1024];

  Path dirPath = filePath.getParent();
  
  Files.createDirectories( dirPath );

  Path tmpFile = dirPath.resolve( filePath.getFileName().toString()+".tmp~" );
  
  try (FileOutputStream fos = new FileOutputStream(tmpFile.toFile()))
  {

   while((rd = nis.read(readBuffer)) >= 0)
   {
    if(rd > 0)
     fos.write(readBuffer, 0, rd);

    total += rd;
   }
   
  }
  catch(Exception e)
  {
   throw e;
  }
  finally
  {
   nis.close();

   conn.disconnect();
  }
  
  Files.move(tmpFile, filePath);
  System.out.println("File downloaded: "+relPath+" -> "+filePath);


  return true;
 }
 
 private void downloadEPMCFiles(String accNo, Path dlDir) throws IOException
 {
  URL url = null;

  int n = accNo.lastIndexOf("PMC");
  
  String origAccNo = accNo.substring(n);
  
  url = new URL(EPMCDownloadURLPrefix+origAccNo+EPMCDownloadURLSuffix);

  HttpURLConnection conn = null;
  
  conn = (HttpURLConnection) url.openConnection();
  
  if( conn.getResponseCode() != HttpURLConnection.HTTP_OK )
  {
   conn.disconnect();
   return;
  }

  ZipInputStream zis = new ZipInputStream(conn.getInputStream());
  
  ZipEntry ze = null;
  
  long total=0;
  int rd;
  byte[] readBuffer = new byte[4*1024*1024];

  Path sbmPath = dlDir.resolve( AccNoUtil.getPartitionedPath(accNo) );
  
  while( (ze=zis.getNextEntry()) != null )
  {
   Path fPath = sbmPath.resolve(ze.getName());
   
   if( ze.isDirectory() )
   {
    Files.createDirectories(fPath);
   }
   else
   {
    Files.createDirectories( fPath.getParent() );
    
    try( FileOutputStream fos =  new FileOutputStream(fPath.toFile()) )
    {
     
     while( (rd = zis.read(readBuffer) ) >= 0 )
     {
      if( rd > 0 )
       fos.write(readBuffer,0,rd);
      
      total+=rd;
     }
    }
    catch(Exception e)
    {
     zis.close();
     throw e;
    }
    finally
    {
    }
   }

  }

  zis.close();
  conn.disconnect();
  
  System.out.println("Archive downloaded: "+url);

 }

 private void printMappings(List<SubmissionMapping> mappings, PrintStream outs)
 {
  int i=0;
  
  for( SubmissionMapping sm : mappings )
  {
   outs.append("\n------- Submission "+i+"/"+mappings.size()+" ---------\n");
   
   outs.append("Submission: ").append(sm.getSubmissionMapping().getOrigAcc()).append(" -> ").append(sm.getSubmissionMapping().getAssignedAcc()).append("\n");
   
   if( sm.getSectionsMapping() != null )
   {
    for( AccessionMapping secm : sm.getSectionsMapping() )
     outs.append("Section: ").append(secm.getOrigAcc()).append(" -> ").append(secm.getAssignedAcc()).append("\n");
   }
   
  }
 }

 private static SubmissionReport submit(String data, DataFormat fmt, URL loginURL) throws SubmitException
 {
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
    throw new SubmitException("Invalid server response. JSON log expected", e);
   }
   
   
  }
  catch(IOException e)
  {
   throw new SubmitException("Server communucation failed: "+e.getMessage(), e);
  }
  
 }


}
