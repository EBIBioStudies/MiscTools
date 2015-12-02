package uk.ac.ebi.biostd.tools.fetchpmc;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.pagetab.FileOccurrence;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;

public class Main
{
 public static final int THREADS = 8;

 public static void main(String[] args)
 {
  if( args.length != 2 )
  {
   System.err.println("Args: <input file> <output dir>");
   return;
  }
  
  File srcFile = new File( args[0] );

  List<File> files  = new ArrayList<File>();
  
  if( srcFile.isDirectory() )
  {
   for( File f : srcFile.listFiles() )
    files.add(f);
  }
  else
   files.add(srcFile);
  
  File outDir = new File( args[1] );
  
  int i=1;
  
  for( File f : files )
   processFile(f, outDir, i++, files.size());
  
 }
 
 
 private static void processFile( File f, File basePath, int ord, int tot )
 {
  System.out.println("Processing file: '" + f + "' "+ord+"of"+tot);
  
  ErrorCounter ec = new ErrorCounterImpl();
  SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + f + "' "+ord+"of"+tot, ec);

  
  PMDoc doc = CVSTVSParse.parse(f, "utf-8", '\t', topLn);

  
  System.out.println("Read "+doc.getSubmissions().size()+" submission");

  List<FileEntry> files = new ArrayList<Main.FileEntry>();
  
  
  for( SubmissionInfo si : doc.getSubmissions() )
  {
   if( si.getFileOccurrences() == null )
    continue;
   
   String sacc = si.getSubmission().getAccNo();
   
   for( FileOccurrence fo : si.getFileOccurrences() )
   {
    FileEntry fe = new FileEntry();
    fe.submissionAcc=sacc;
    fe.fileName = fo.getFileRef().getName();
    fe.srcFile=f.getName();
    
    files.add(fe);
   }
  }
  
  Collections.sort(files);
  
  List<FileEntry> uniqFile = new ArrayList<Main.FileEntry>(files.size());
  
  FileEntry prev = null;
  
  for( FileEntry fe : files )
  {
   if( ! fe.equals( prev ) )
    uniqFile.add(fe);
   else
    System.err.println("Repeating: "+fe.submissionAcc+" "+fe.fileName);
   
   prev=fe;
  }
  
  
  System.out.println("Found "+uniqFile.size()+" files");
  
  ExecutorService  pool = Executors.newFixedThreadPool(THREADS);
  
  FileDispencer dsp = new FileDispencer(uniqFile, ord, tot);
  
  for( int i=0; i < THREADS; i++ )
   pool.submit( new DownloadTask(basePath, dsp) );
  
  pool.shutdown();
  
  while( true )
  {
   try
   {
    if( pool.awaitTermination(7, TimeUnit.DAYS) )
     break;
   }
   catch(InterruptedException e)
   {
   }
  }

  System.err.println("File: "+f+" "+ord+"of"+tot+" download finished Total: "+uniqFile.size()+" Success: "+dsp.getSuccess()+" Fail: "+dsp.getFail()+" Skip: "+dsp.getSkip());
 }
 
 static class FileEntry implements Comparable<FileEntry>
 {
  String submissionAcc;
  String fileName;
  String srcFile;
  
  @Override
  public boolean equals(Object obj) 
  {
   if( obj == null )
    return false;
   
   return ((FileEntry)obj).submissionAcc.equals(submissionAcc) && ((FileEntry)obj).fileName.equals(fileName);
  }
  
  @Override
  public int compareTo(FileEntry o)
  {
   int cmp = submissionAcc.compareTo( o.submissionAcc );
   
   if( cmp != 0 )
    return cmp;
   
   return fileName.compareTo(o.fileName);
  }
 }
}
