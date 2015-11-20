package uk.ac.ebi.biostd.tools.submitmass;

import java.util.List;

import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;

public interface Config
{
 static final String SessionKey = "BIOSTDSESS";
 
 public static final String authEndpoint = "auth/signin";
 public static final String submitEndpoint = "submit/create";
 public static final String updateEndpoint = "submit/update";
 public static final String replaceEndpoint = "submit/replace";
 public static final String deleteEndpoint = "submit/delete";
 
  @Unparsed
  public List<String> getFiles();

  
  @Option( shortName="u")
  String getUser();

  @Option( shortName="p",maximum=1)
  public List<String> getPassword();
  public boolean isPassword();
  
  @Option(shortName="s")
  public String getServer();

  @Option(shortName="n", defaultValue="0")
  public int getLimit();
  
  @Option(helpRequest = true,shortName="h")
  boolean getHelp();
  
  @Option(shortName="t", defaultValue="")
  String getFileTemplate();
  
  @Option(shortName="f")
  boolean isForcePublic();
  
  @Option(shortName="a", defaultValue="")
  String getAttachTo();

  @Option(shortName="i", defaultValue="1")
  public int getParallelFiles();
  
  @Option(shortName="0", defaultValue="1")
  public int getParallelSubmitters();

}
