package uk.ac.ebi.biostd.tools.pmcloadmap;

import java.util.List;

import com.lexicalscope.jewel.cli.Unparsed;

public interface Config
{
 
  @Unparsed
  public List<String> getFiles();
}
