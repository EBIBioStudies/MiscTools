package uk.ac.ebi.biostd.tools.fetchpmc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import uk.ac.ebi.biostd.tools.fetchpmc.Main.FileEntry;
import uk.ac.ebi.biostd.tools.partdir.AccNoUtil;

public class DownloadTask implements Runnable
{
 private FileDispencer disp;
 private File basePath;
 
 private byte[] readBuffer = new byte[4*1024*1024];
 
 public DownloadTask( File basePath, FileDispencer dp )
 {
  this.basePath = basePath;
  
  disp=dp;
 }

 @Override
 public void run()
 {
  try
  {
   runUnsafe();
  }
  catch(Throwable e)
  {
   System.err.println("Thread died: "+e.getMessage());
   e.printStackTrace(System.err);
  }
 }
 
 public void runUnsafe()
 {
  String msg = null;

  while(true)
  {
   FileEntry fe = disp.getEntry(msg);

   if(fe == null)
    return;

//   int pos = fe.submissionAcc.indexOf("PMC");
//
//   if(pos < 0)
//   {
//    System.err.println("Invalid accession: " + fe.submissionAcc);
//    disp.incFail();
//    continue;
//   }
//   String accNo = fe.submissionAcc.substring(pos);

   String accNo = fe.submissionAcc;

   File stdDir = new File(basePath, AccNoUtil.getPartitionedPath(accNo));

   stdDir.mkdirs();

   String fn = fe.fileName;

   File outFile = new File(stdDir, fn);

   if(outFile.exists())
   {
    msg = "File "+fe.srcFile +" "+ accNo + ": " + fn + " EXISTS";
    disp.incSkip();
    continue;
   }

   int n = accNo.lastIndexOf("PMC");
   
   String origAccNo = accNo.substring(n);
   
   try
   {
    try
    {
     msg = loadFromWeb(origAccNo, fn, fe.srcFile, outFile);
    }
    catch(Exception e)
    {
     msg = loadFromRest(origAccNo, fn, fe.srcFile, outFile);
    }
   }
   catch(IOException e)
   {
    if( outFile.exists() )
     outFile.delete();
     
    disp.incFail();
    msg = "I/O error '"+e.getClass().getName()+"': " + fn + " " + e.getMessage();
    
    System.err.println(msg);
    
    continue;
   }

  }

 }

 private String loadFromRest(String origAccNo, String fn, String srcFile, File outFile) throws IOException
 {
  URL url = null;
  try
  {
   url = new URL("http://www.ebi.ac.uk/europepmc/webservices/rest/"+origAccNo+"/supplementaryFiles");
  }
  catch(MalformedURLException e1)
  {
   disp.incFail();
   return "Invalid URL: " + url;
  }  
  
  HttpURLConnection conn = null;
  
  conn = (HttpURLConnection) url.openConnection();

  ZipInputStream zis = new ZipInputStream(conn.getInputStream());
  
  ZipEntry ze = null;
  
  while( (ze=zis.getNextEntry()) != null )
  {
   if( ze.getName().equals(fn) )
    break;
  }

  if( ze== null )
   throw new FileNotFoundException(fn);
  
  long total=0;
  int rd;

  
  try( FileOutputStream fos =  new FileOutputStream(outFile) )
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
   throw e;
  }
  finally
  {
   zis.close();

   conn.disconnect();
  }

  disp.incSuccess();

  return "File " + srcFile +" "+ origAccNo + ": " + fn + " OK (rest)" + total;
  
 }
 
 private String loadFromWeb(String origAccNo, String fn, String srcFile, File outFile) throws IOException
 {
  URL url = null;
  try
  {
   url = new URL("http://europepmc.org/articles/" + origAccNo + "/bin/" + fn);
  }
  catch(MalformedURLException e1)
  {
   disp.incFail();
   return "Invalid URL: " + url;
  }

  HttpURLConnection conn = null;

  conn = (HttpURLConnection) url.openConnection();

  InputStream nis = conn.getInputStream();

  long total = 0;
  int rd;

  try (FileOutputStream fos = new FileOutputStream(outFile))
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



  disp.incSuccess();

  return "File " + srcFile + " " + origAccNo + ": " + fn + " OK (web)" + total;
 }
 
}
