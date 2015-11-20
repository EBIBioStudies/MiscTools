package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import uk.ac.ebi.biostd.db.TagResolver;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.ParserConfig;
import uk.ac.ebi.biostd.in.pagetab.PageTabSyntaxParser;
import uk.ac.ebi.biostd.in.pagetab.SpreadsheetReader;
import uk.ac.ebi.biostd.in.pagetab.XLSpreadsheetReader;
import uk.ac.ebi.biostd.treelog.LogNode;

public class XLParse
{
 public static PMDoc parse( File infile, LogNode topLn )
 {
  
  Workbook wb = null;
  
  try
  {
   wb = WorkbookFactory.create( infile );
   
   SpreadsheetReader reader =  new XLSpreadsheetReader(wb);
   
   ParserConfig pc = new ParserConfig();
   pc.setMultipleSubmissions(true);
   TagResolver tr = new AdHocTagResolver();
   
   PageTabSyntaxParser prs = new PageTabSyntaxParser(tr, pc);
   
   return prs.parse(reader, topLn);
   
  }
  catch( Throwable t )
  {
   System.err.println("XL file read ERROR: "+t.getMessage());
   t.printStackTrace();
  }
  
  return null;
 }
}
