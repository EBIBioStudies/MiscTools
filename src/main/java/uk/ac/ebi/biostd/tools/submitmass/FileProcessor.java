package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.tools.fetchpmc.CVSTVSParse;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;
import uk.ac.ebi.biostd.treelog.Utils;
import uk.ac.ebi.biostd.util.DataFormat;

public class FileProcessor implements Runnable
{
 private String procName;
 private BlockingQueue<FileRequest> fileQueue;
 private BlockingQueue<SubmitRequest> submitQueue;
 private Path outDir;
 private Map<String, SubmissionPointer> submissionMap;
 private boolean rmDups;
 private Config config;
 
 public FileProcessor(String pName, BlockingQueue<FileRequest> fq, BlockingQueue<SubmitRequest> sq, Path outd, Map<String, SubmissionPointer> sbmMap, Config cfg )
 {
  procName = pName;
  fileQueue = fq;
  submitQueue = sq;
  outDir  = outd;
  this.submissionMap=sbmMap;
  rmDups = cfg.getRemoveDuplicates();
  config = cfg;
 }
 
 @Override
 public void run()
 {
  while( true )
  {
   FileRequest fr = null;
   
   try
   {
    fr = fileQueue.take();
   }
   catch(InterruptedException e)
   {
    continue;
   }
   
   
   if( fr.getRequestId() == null )
   {
    while(true)
    {
     try
     {
      fileQueue.put(fr);
      return;
     }
     catch(InterruptedException e)
     {
     }
    }
   }
   
   processFile(fr);
   
  }
  
 }
 
 
 private void processFile( FileRequest fReq )
 {
  File file = fReq.getFile();
  
  ErrorCounter ec = new ErrorCounterImpl();
  SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + file + "'", ec);

  
  DataFormat fmt = null;

  String ext = null;
  
  int pos = file.getName().lastIndexOf('.');
  
  if( pos >=0 )
   ext = file.getName().substring(pos+1);
  
    
  if( "xlsx".equalsIgnoreCase(ext) )
   fmt = DataFormat.xlsx;
  else if( "xls".equalsIgnoreCase(ext) )
   fmt = DataFormat.xls;
  else if( "json".equalsIgnoreCase(ext) )
   fmt = DataFormat.json;
  else if( "ods".equalsIgnoreCase(ext) )
   fmt = DataFormat.ods;
  else if( "csv".equalsIgnoreCase(ext) )
   fmt = DataFormat.csv;
  else if( "tsv".equalsIgnoreCase(ext) )
   fmt = DataFormat.tsv;
  else
   fmt = DataFormat.csvtsv;
  
  PMDoc doc = null;

  if( fmt==DataFormat.xlsx ||  fmt==DataFormat.xls )
   doc = XLParse.parse(file, topLn);
  else if( fmt==DataFormat.ods)
   doc = ODSParse.parse(file, topLn);
  else if( fmt==DataFormat.json)
   doc = JSONParse.parse(file, "utf-8", topLn);
  else if( fmt==DataFormat.csv )
   doc = CVSTVSParse.parse(file, "utf-8", ',', topLn);
  else if( fmt==DataFormat.tsv)
   doc = CVSTVSParse.parse(file, "utf-8", '\t', topLn);
  else if( fmt==DataFormat.csvtsv)
   doc = CVSTVSParse.parse(file, "utf-8", '\0', topLn);
  else if( fmt==DataFormat.csvtsv)
   doc = CVSTVSParse.parse(file, "utf-8", '\0', topLn);
  
  if( doc == null )
  {
   try( PrintWriter errOut = new PrintWriter( outDir.resolve(file.getName()+".FAIL").toFile() )  ) 
   {
    errOut.println("Parser can't read a file: "+file);
   }
   catch( IOException e)
   {
    e.printStackTrace();
   }
   
   return;
  }
 
  String fID = file.getName();
  
  Console.println(procName+": "+fReq+": read "+doc.getSubmissions().size()+" submission");

  
  SimpleLogNode.setLevels(topLn);
  
  if( topLn.getLevel().getPriority() >= Level.ERROR.getPriority() )
  {
   Console.println(procName+": "+fReq+": parser error");
   
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
  
  
  Path fileOutDir = outDir;
  
  if( config.isOutputDirPerFile() )
  {
   fileOutDir = outDir.resolve( fID );
   
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
  
  
  
  int i=0;
  int n = doc.getSubmissions().size();
  
  Map<String,SubmissionPointer> dupMap=null;
  
  if( rmDups )
  {
   dupMap = new HashMap<String, SubmissionPointer>();
   
   for( SubmissionInfo si : doc.getSubmissions() )
   {
    i++;

    if( si.getAccNoPrefix() == null && si.getAccNoSuffix() == null && si.getAccNoOriginal() != null )
    {
     SubmissionPointer ptr = dupMap.get(si.getAccNoOriginal());
     
     if( ptr == null )
      dupMap.put(si.getAccNoOriginal(), new SubmissionPointer(null, i));
     else
      ptr.setOrder(i);
    }
   }
  }
  
  i=0;
  for( SubmissionInfo si : doc.getSubmissions() )
  {
   i++;

   String logFileName = fID+"-"+i+"of"+n;

   Path okFile = fileOutDir.resolve(logFileName + ".OK");

   if( Files.exists(okFile) )
    continue;

   Path obsoleteFile = fileOutDir.resolve(logFileName + ".OBSOLETE");
 
   if(  Files.exists(obsoleteFile) )
    continue;

   SubmitRequest sr = new SubmitRequest();
   
   sr.setFileName(fID);
   sr.setFileOrder(fReq.getOrder());
   sr.setFileTotal(fReq.getTotal());
   
   sr.setOrder(i);
   sr.setTotal(n);
   
   sr.setSubmissionInfo(si);
   
   if( si.getAccNoPrefix() == null && si.getAccNoSuffix() == null && si.getAccNoOriginal() != null )
   {
    if( dupMap != null )
    {
     SubmissionPointer ptr = dupMap.get(si.getAccNoOriginal());
     
     if( ptr != null && ptr.getOrder() != i )
     {
      try
      {
       Files.createFile(obsoleteFile);
      }
      catch(IOException e)
      {
       e.printStackTrace();
      }
      
      Console.println(procName+": Sbm: "+sr+" SKIP DUPLICATE");
      
      continue;
     }
    }
    
    if( submissionMap != null )
    {
     SubmissionPointer ptr = submissionMap.get(si.getAccNoOriginal());
     
     if( ptr == null )
     {
      Path umFile = fileOutDir.resolve(logFileName + ".UNMAPPED");
     
      touch(umFile);
      
      Console.println(procName+": Sbm: "+sr+" SKIP UNMAPPED");

      continue;
     }
     
     if(  i != ptr.getOrder() || ! fID.equals(ptr.getFileName()) )
     {
      touch(obsoleteFile);
      
      Console.println(procName+": Sbm: "+sr+" SKIP OBSOLETE");

      continue;
     }     
    }
    
   }
   

   
   while( true )
   {
    try
    {
     submitQueue.put(sr);
     break;
    }
    catch(InterruptedException e)
    {
    }
   }
  }
 }
 
 private void touch( Path file )
 {
  try
  {
   Files.createFile(file);
  }
  catch(IOException e)
  {
   e.printStackTrace();
  }
 }
 
}
