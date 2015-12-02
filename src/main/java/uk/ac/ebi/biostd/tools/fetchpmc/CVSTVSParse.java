package uk.ac.ebi.biostd.tools.fetchpmc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import uk.ac.ebi.biostd.db.TagResolver;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.ParserConfig;
import uk.ac.ebi.biostd.in.ParserException;
import uk.ac.ebi.biostd.in.pagetab.CSVTSVSpreadsheetReader;
import uk.ac.ebi.biostd.in.pagetab.PageTabSyntaxParser;
import uk.ac.ebi.biostd.in.pagetab.SpreadsheetReader;
import uk.ac.ebi.biostd.treelog.SimpleLogNode;
import uk.ac.ebi.biostd.util.FileUtil;

public class CVSTVSParse
{

 public static PMDoc parse(File infile, String charset, char sep, SimpleLogNode topLn)
 {
  
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
   
   if( infile.getName().endsWith(".gz"))
    text = FileUtil.readGzFile(infile, cs);
   else
    text = FileUtil.readFile(infile, cs);
  }
  catch(IOException e)
  {
   System.err.println("Input file read ERROR: "+e.getMessage());
   return null;
  }
  
  ParserConfig pc = new ParserConfig();
  pc.setMultipleSubmissions(true);
  TagResolver tr = new AdHocTagResolver();
  
  PageTabSyntaxParser prs = new PageTabSyntaxParser(tr, pc);
  
  SpreadsheetReader sp = new CSVTSVSpreadsheetReader(text, sep, false);
  
  try
  {
   return prs.parse(sp, topLn);
  }
  catch(ParserException e)
  {
   System.err.println("Can't read CSV/TSV file: "+infile.getAbsolutePath()+" Error: "+e.getMessage());
  }
  
  return null;
 }

}
