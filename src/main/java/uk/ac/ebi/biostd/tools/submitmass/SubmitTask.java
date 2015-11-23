package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.in.AccessionMapping;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.SubmissionMapping;
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
 
 private String taskName;

 private BlockingQueue<SubmitRequest> sbmQueue;
 private Config config;
 private Path outDir;
 private String sess;
 
 
 public SubmitTask(String taskName, BlockingQueue<SubmitRequest> sbmQueue, Config config, Path outDir, String sess)
 {
  this.taskName = taskName;
  this.sbmQueue = sbmQueue;
  this.config = config;
  this.outDir = outDir;
  this.sess = sess;
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

  appUrl += Config.submitEndpoint;
  
  URL loginURL = null;

  
  try
  {
   loginURL = new URL(appUrl  + "?"+Config.SessionKey+"="+URLEncoder.encode(sess, "utf-8"));
  }
  catch(MalformedURLException e)
  {
   System.err.println("Invalid server URL: " + srvUrl);
   System.exit(1);
  }
  catch(UnsupportedEncodingException e)
  {
  }
  
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

   SubmissionInfo si = req.getSubmissionInfo();
   
   String acc = null;
   
   
   if( config.isDontUseSecAccno() )
    acc = si.getSubmission().getAccNo();
   else
    acc = si.getRootSectionOccurance().getOriginalAccNo();

   if(acc == null)
    acc = si.getAccNoOriginal();

   
//   if(acc != null && acc.startsWith("!"))
//    acc = acc.substring(1);

   String logFileName = req.getFileName()+"-"+req.getOrder()+"of"+req.getTotal();
   
   Path okFile = outDir.resolve(logFileName + ".OK");
   Path failFile = outDir.resolve(logFileName + ".FAIL");

   if(Files.exists(okFile))
   {
    Console.println(taskName+": Sbm: "+req+" SKIP");
    continue;
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

   si.getSubmission().setAccNo(acc);
   si.setAccNoOriginal(acc);
   
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
