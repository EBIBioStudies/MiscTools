package uk.ac.ebi.biostd.tools.chkenc;

import java.io.File;
import java.io.PrintWriter;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.tools.fetchpmc.CVSTVSParse;
import uk.ac.ebi.biostd.treelog.ErrorCounter;
import uk.ac.ebi.biostd.treelog.ErrorCounterImpl;
import uk.ac.ebi.biostd.treelog.LogNode.Level;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;

public class CHKEMain
{

 public static void main(String[] args)
 {
  File srcPath = new File(args[0]);
  
  File outFile = new File(args[1]);
  
  try( PrintWriter out = new PrintWriter(outFile, "UTF-8") )
  {
   int fcnt=0;
   
   out.println("<html><head><style>\n td {border: 1px solid black}\n.nf {background-color: orange}\n.fx {background-color: red}\n</style>"
     + "<script> function collect(){ inp=document.getElementsByTagName('input'); res =''; for( i=0; i < inp.length; i++ ) { if(! inp[i].checked ) res += inp[i].value+' '; "
     + "} document.getElementById('res').innerHTML=res; }"
     + "</script></head> <body><textarea rows='10' cols='200' id='res'></textarea><br><button onClick='collect()'>Collect</button><br><table>");
   
   File[] files = srcPath.listFiles();
   
   for( File f : files )
   {
    fcnt++;
    
    System.out.println("Processing file: "+f.getName()+" ("+fcnt+" of "+files.length+")");
    
    if( ! f.isDirectory() )
     processFile(f,out);
   }
   
   out.println("</table></body></html>");

  }
  catch(Exception e)
  {
   e.printStackTrace();
  }
  
  


 }

 private static void processFile(File file, PrintWriter out)
 {
  ErrorCounter ec = new ErrorCounterImpl();
  SimpleLogNode topLn = new SimpleLogNode(Level.SUCCESS, "Parsing file: '" + file + "'", ec);

  AccessTag publicTag = new AccessTag();
  publicTag.setName("Public");
  
  PMDoc doc = CVSTVSParse.parse(file, "utf-8", '\t', topLn);

  if( doc == null )
   return;
  
  for( SubmissionInfo si : doc.getSubmissions() )
  {
   CharsetCheck.checkSubmissionCharset(si, out, file.getName());
  }
 
 }

}
