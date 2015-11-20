package uk.ac.ebi.biostd.tools.fetchpmc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.poi.util.IOUtils;

import uk.ac.ebi.biostd.tools.fetchpmc.Main.FileEntry;
import uk.ac.ebi.biostd.tools.partdir.AccNoUtil;

public class DownloadTask implements Runnable
{
 private FileDispencer disp;
 private File basePath;
 
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

   stdDir.mkdir();

   String fn = fe.fileName;

   File outFile = new File(stdDir, fn);

   if(outFile.exists())
   {
    msg = "File " + accNo + ": " + fn + " EXISTS";
    disp.incSkip();
    continue;
   }

   URL url = null;
   try
   {
    url = new URL("http://europepmc.org/articles/" + accNo + "/bin/" + fn);
   }
   catch(MalformedURLException e1)
   {
    msg = "Invalid URL: " + url;
    disp.incFail();
    continue;
   }

   FileOutputStream fos = null;
   
   try
   {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    byte[] data = IOUtils.toByteArray(conn.getInputStream());

    fos = new FileOutputStream(outFile);

    fos.write(data);

    fos.close();
    fos=null;
    
    msg = "File " + accNo + ": " + fn + " OK " + data.length;

    conn.disconnect();

    disp.incSuccess();
   }
   catch(IOException e)
   {
    if( fos != null )
    {
     try
     {
      fos.close();
     }
     catch(Exception e2)
     {
     }
    }
    
    if( outFile.exists() )
     outFile.delete();
     
    disp.incFail();
    msg = "I/O error: " + url + " " + e.getMessage();
    
    System.err.println(msg);
    
    continue;
   }

  }

 }

}
