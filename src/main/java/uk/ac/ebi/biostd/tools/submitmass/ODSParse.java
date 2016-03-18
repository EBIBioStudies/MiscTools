package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;

import org.odftoolkit.simple.SpreadsheetDocument;

import uk.ac.ebi.biostd.db.TagResolver;
import uk.ac.ebi.biostd.in.PMDoc;
import uk.ac.ebi.biostd.in.ParserConfig;
import uk.ac.ebi.biostd.in.pagetab.PageTabSyntaxParser;
import uk.ac.ebi.biostd.treelog.LogNode;
import uk.ac.ebi.mg.spreadsheet.SpreadsheetReader;
import uk.ac.ebi.mg.spreadsheet.readers.ODSpreadsheetReader;

public class ODSParse
{
 public static PMDoc parse( File infile, LogNode topLn )
 {
  try
  {
   SpreadsheetDocument doc = SpreadsheetDocument.loadDocument(infile);

   SpreadsheetReader reader =  new ODSpreadsheetReader(doc);
   
   ParserConfig pc = new ParserConfig();
   pc.setMultipleSubmissions(true);
   TagResolver tr = new AdHocTagResolver();
   
   PageTabSyntaxParser prs = new PageTabSyntaxParser(tr, pc);
   
   return prs.parse(reader, topLn);
   
  }
  catch( Throwable t )
  {
   System.err.println("ODS file read ERROR: "+t.getMessage());
  }
  
  return null;
 }
}
