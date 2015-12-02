package uk.ac.ebi.biostd.tools.fetchpmc;

import java.util.List;

import uk.ac.ebi.biostd.tools.fetchpmc.Main.FileEntry;

public class FileDispencer
{
 private List<Main.FileEntry> entries;
 private int ptr;
 private int succ;
 private int fail;
 private int skip;
 
 private int fileOrder;
 private int totalFiles;
 
 public FileDispencer( List<Main.FileEntry> ents, int ord, int tot )
 {
  entries = ents;
  ptr=0;
  
  succ = 0;
  fail = 0;
  skip = 0;
  
  fileOrder = ord;
  totalFiles=tot;
 }
 
 public synchronized FileEntry getEntry( String msg )
 {
  if( msg != null )
  {
   System.out.println( msg+ " ("+fileOrder+"/"+totalFiles+ ") Entry ("+(succ+fail+skip)+"/"+entries.size()+")");
  }
  
  if( ptr >= entries.size() )
   return null;
  
  return entries.get(ptr++);
 }
 
 public synchronized void incSuccess()
 {
  succ++;
 }
 
 public synchronized void incFail()
 {
  fail++;
 }
 
 public synchronized void incSkip()
 {
  skip++;
 }

 
 public int getSuccess()
 {
  return succ;
 }
 
 public int getFail()
 {
  return fail;
 }
 
 public int getSkip()
 {
  return skip;
 }

}

