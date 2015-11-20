package uk.ac.ebi.biostd.tools.fileacc;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.tools.fetchpmc.CVSTVSParse;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.InvalidOptionSpecificationException;

public class Main
{
 public static void main(String[] args)
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

  if(config.getFiles() == null || config.getFiles().size() != 1)
  {
   System.err.println("Command line processing ERROR: invalid number of files specified");
   usage();
   System.exit(1);
  }

  File srcPath = new File( config.getFiles().get(0) );

  Collection<File> fileList = new ArrayList<File>();
  
  if( srcPath.isDirectory() )
  {
   if( config.getFileTemplate() != null && config.getFileTemplate().length() > 0 )
    fileList.addAll( Arrays.asList( srcPath.listFiles( (FilenameFilter)new WildcardFileFilter(config.getFileTemplate()) ) ) );
   else
    fileList.addAll( Arrays.asList( srcPath.listFiles() ) );
  }
  else
   fileList.add(srcPath);
  
  Map< String,Collection<String> > fileMap = new HashMap<String, Collection<String>>();
  
  int fcnt=0;
  for( File f : fileList )
  {
   fcnt++;
   
   System.out.println("Processing file: "+f.getName()+" ("+fcnt+" of "+fileList.size()+")");
   
   if( f.isDirectory() )
    continue;

   ErrorCounter ec = new ErrorCounterImpl();
   SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + f + "'", ec);

   AccessTag publicTag = new AccessTag();
   publicTag.setName("Public");
   
   PMDoc doc = CVSTVSParse.parse(f, "utf-8", '\t', topLn);

   if( doc == null )
   {
    System.err.println("Parser can't read a file: "+f);
    return;
   }
   
   System.out.println("Read "+doc.getSubmissions().size()+" submission");

   
   SimpleLogNode.setLevels(topLn);

   
   int i=0;
   int n = doc.getSubmissions().size();
   
   for( SubmissionInfo si : doc.getSubmissions() )
   {
    i++;

    if( config.getLimit() > 0 && i > config.getLimit() )
     break;
    
    
    String acc = si.getSubmission().getRootSection().getAccNo();
    
    if( acc == null )
     acc = si.getSubmission().getAccNo();
    
    if( acc.startsWith("!") )
     acc = acc.substring(1);
    
    
    System.out.print("Submission "+acc+" ("+i+" of "+n+") ");
    
    Collection<String> files = fileMap.get(acc);
    
    if( files == null )
    {
     files = new ArrayList<String>();
     fileMap.put(acc, files);
    }
    
    files.add(f.getName());
    
   }
   
  }
 
  for( Map.Entry<String, Collection<String>> me : fileMap.entrySet() )
  {
   if( me.getValue().size() > 1 )
   {
    me.getValue().stream().forEach(f->System.out.println(me.getKey()+"->"+f));
   }
  }
  
 }
 
 public static void usage()
 {}
}
