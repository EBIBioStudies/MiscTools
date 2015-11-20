package uk.ac.ebi.biostd.tools.fileacc;

import java.util.List;

import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;

public interface Config
{
  @Unparsed
  public List<String> getFiles();

  
  @Option(shortName="n", defaultValue="0")
  public int getLimit();
  
  @Option(helpRequest = true,shortName="h")
  boolean getHelp();
  
  @Option(shortName="t", defaultValue="")
  String getFileTemplate();
}
