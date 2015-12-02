package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
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
 
 
 public FileProcessor(String pName, BlockingQueue<FileRequest> fq, BlockingQueue<SubmitRequest> sq, Path outd )
 {
  procName = pName;
  fileQueue = fq;
  submitQueue = sq;
  outDir  = outd;
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
  
  
  int i=0;
  int n = doc.getSubmissions().size();
  
  for( SubmissionInfo si : doc.getSubmissions() )
  {
   i++;

   SubmitRequest sr = new SubmitRequest();
   
   sr.setFileName(fID);
   sr.setFileOrder(fReq.getOrder());
   sr.setFileTotal(fReq.getTotal());
   
   sr.setOrder(i);
   sr.setTotal(n);
   
   sr.setSubmissionInfo(si);
   
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
 
}
