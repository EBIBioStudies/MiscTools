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
 
 public FileDispencer( List<Main.FileEntry> ents )
 {
  entries = ents;
  ptr=0;
  
  succ = 0;
  fail = 0;
  skip = 0;
 }
 
 public synchronized FileEntry getEntry( String msg )
 {
  if( msg != null )
  {
   System.out.println( msg+ " ("+(succ+fail+skip)+"/"+entries.size()+")");
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

