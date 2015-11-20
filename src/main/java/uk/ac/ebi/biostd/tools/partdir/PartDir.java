package uk.ac.ebi.biostd.tools.partdir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PartDir
{
 public static void main(String[] args) throws IOException
 {
  Path src = FileSystems.getDefault().getPath( args[0] );
  Path dest = FileSystems.getDefault().getPath( args[1] );
  
  int i=0;
  
  List<Path> list = new ArrayList<Path>();
  
  for( Path f : Files.newDirectoryStream(src) )
   list.add(f);
  
  for( Path f : list )
  {
   i++;
   
   String nname = AccNoUtil.getPartitionedPath( f.getFileName().toString() );
   
   Path destDir = dest.resolve(nname);
   
   Files.createDirectories(destDir);
   
   for( Path cf : Files.newDirectoryStream(f) )
   {
    Files.createLink(destDir.resolve(cf.getFileName()), cf);
   }
   
//   FileUtils.copyDirectory(f, destDir);
   
   System.out.println( f.getFileName() + " -> " + nname+" ("+i+" of "+list.size()+")");
   
//   if( i > 2 )
//    break;
  }
  
 }
}
