package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.ParserConfig;
import uk.ac.ebi.biostd.in.json.JSONReader;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;
import uk.ac.ebi.biostd.util.FileUtil;

public class JSONParse
{

 public static PMDoc parse(File infile, String charset, SimpleLogNode topLn)
 {
  ParserConfig pc = new ParserConfig();

  pc.setMultipleSubmissions(true);

  JSONReader jsnReader = new JSONReader(new AdHocTagResolver(), pc);
  
  String text = null;
  
  try
  {
   Charset cs = null;
   
   if( charset != null )
   {
    try
    {
     cs = Charset.forName(charset);
    }
    catch(Exception e)
    {
     System.err.println("Invalid charset: '"+charset+"'");
     return null;
    }
   }
   else
    cs = Charset.forName("utf-8");
   
   
   text = FileUtil.readFile(infile, cs);
  }
  catch(IOException e)
  {
   System.err.println("Input file read ERROR: "+e.getMessage());
   return null;
  }
  
  return jsnReader.parse(text, topLn);
 }

}
