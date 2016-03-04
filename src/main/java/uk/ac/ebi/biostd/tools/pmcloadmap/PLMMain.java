package uk.ac.ebi.biostd.tools.pmcloadmap;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.tools.fetchpmc.CVSTVSParse;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.InvalidOptionSpecificationException;

public class PLMMain
{
 static final String SnapshotDir = "initial_dump";
 static final String DailyDir = "daily";

 public static void main(String[] args) throws IOException
 {
  Config config = null;

  try
  {
   config = CliFactory.parseArguments(Config.class, args);
  }
  catch(HelpRequestedException e)
  {
   usage();
   System.exit(1);
  }
  catch(InvalidOptionSpecificationException | ArgumentValidationException e)
  {
   System.err.println("Command line processing ERROR: " + e.getMessage());
   usage();
   System.exit(1);
  }

  if(config.getFiles() == null || config.getFiles().size() != 2)
  {
   System.err.println("Command line processing ERROR: invalid number of files specified");
   usage();
   System.exit(1);
  }
  
  Path rpth = FileSystems.getDefault().getPath(config.getFiles().get(0));
  
  Path snshPath = rpth.resolve(SnapshotDir);
  Path dailyPath = rpth.resolve(DailyDir);
  

  List<FileInfo> sFiles = Files.list(snshPath)
    .map(fn -> new FileInfo( fn, SnapshotMapper::apply ) )
    .collect(Collectors.toList());
  
  List<FileInfo> dFiles = Files.list(dailyPath)
    .map(fn -> new FileInfo( fn, DailyMapper::apply ) )
    .collect(Collectors.toList());

  
  Collections.sort(sFiles);
  Collections.sort(dFiles);
  
  sFiles.addAll(dFiles);
  
//  sFiles.stream().forEach(System.out::println);
  
  Map<String, SubmissionInfo> sbmMap = new HashMap<String, SubmissionInfo>();
  
  int tot = sFiles.size();
  int ord=0;
  
  for( int i=tot-1; i >=0; i-- )
  {
   ord++;
   
   FileInfo fi = sFiles.get(i);
   
   ErrorCounter ec = new ErrorCounterImpl();
   SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + fi.filePath + "' "+ord+"of"+tot, ec);

   
   PMDoc doc = CVSTVSParse.parse(fi.filePath.toFile(), "utf-8", '\t', topLn);

   int sz = doc.getSubmissions().size();
   
   System.out.println("Read "+sz+" "+ord+"of"+tot );
   
   int skipped = 0;
   
   for( int j=sz-1; j >= 0; j-- )
   {
    String acc = doc.getSubmissions().get(j).getAccNoOriginal();
    
    if( ! sbmMap.containsKey(acc) )
    {
     SubmissionInfo si = new SubmissionInfo();
     
     si.order=j+1;
     si.file = fi.filePath;
     
     sbmMap.put(acc, si);
    }
    else
     skipped++;
   }
   
   System.out.println(fi.filePath+" skipped "+skipped);
   
  }
  
  System.out.println("Total submissions: "+sbmMap.size());
  
  
  
  try( PrintWriter out = new PrintWriter( config.getFiles().get(1) ) )
  {
   for( Map.Entry<String, SubmissionInfo> me : sbmMap.entrySet() )
    out.printf("%s\t%s\t%d\n", me.getKey(), me.getValue().file.getFileName().toString(), me.getValue().order );
  }
  catch(Exception e)
  {
   e.printStackTrace();
  }
  
  
 }

 static class SnapshotMapper
 {
  static Pattern pat = Pattern.compile("bs-(\\d+)-\\d+-0\\.txt\\.gz");
  
  public static Long apply(String t)
  {
   Matcher mtch = pat.matcher(t);

   return mtch.matches()?Integer.parseInt(mtch.group(1)):-1L;
  }
 }
 
 static class DailyMapper
 {
  static Pattern pat = Pattern.compile("bs-(\\d+)-(\\d+)-(\\d+)-(\\d+)\\.txt\\.gz");
  
  public static Long apply(String t)
  {
   Matcher mtch = pat.matcher(t);

   if( ! mtch.matches() )
    return -1L;
   
   return Integer.parseInt(mtch.group(3))*100*100*100L+Integer.parseInt(mtch.group(2))*100*100+Integer.parseInt(mtch.group(1))*100+Integer.parseInt(mtch.group(4));
  }
 }

 static class SubmissionInfo
 {
  Path file;
  int order;
 }
 
 static class FileInfo implements Comparable<FileInfo>
 {
  Path filePath;
  long fileOrder;

  public FileInfo(Path fn, Function<String, Long> f)
  {
   filePath = fn;
   
   fileOrder=f.apply(fn.getFileName().toString());
  }
  
  @Override
  public boolean equals(Object obj)
  {
   return filePath.equals(((FileInfo)obj).filePath);
  }
  
  @Override
  public int hashCode()
  {
   return filePath.hashCode();
  }

  @Override
  public int compareTo(FileInfo o)
  {
   if( filePath.equals( o.filePath) )
   return 0;
   
   return fileOrder > o.fileOrder ? 1:-1;
  }
  
  @Override
  public String toString()
  {
   return filePath+" "+fileOrder;
  }
 }
 
 
 public static void usage()
 {}
}
