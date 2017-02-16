package uk.ac.ebi.biostd.tools.submitmass;

import java.util.List;

import com.lexicalscope.jewel.cli.Option;
import com.lexicalscope.jewel.cli.Unparsed;

public interface Config
{
 static final String SessionKey = "BIOSTDSESS";
 static final String SubmitRequestID = "requestId";
 static final String SubmitOnBehalf =  "onBehalf";
 static final String ValidateOnly =  "validateOnly";
 static final String IgnoreAbsentFiles =  "ignoreAbsentFiles";

 public static final String authEndpoint = "auth/signin";
 public static final String exportLockEndpoint = "export/lock";
 public static final String exportUnlockEndpoint = "export/unlock";

// public static final String createEndpoint = "submit/create";
// public static final String updateEndpoint = "submit/update";
// public static final String replaceEndpoint = "submit/replace";
// public static final String overrideEndpoint = "submit/override";
// public static final String deleteEndpoint = "submit/delete";
 

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
  String getFileNamePattern();
  
  @Option(shortName="f")
  boolean isForcePublic();
  
  @Option(shortName="a", defaultValue="")
  String getAttachTo();

  @Option(shortName="i", defaultValue="1")
  public int getParallelFiles();
  
  @Option(shortName="o", defaultValue="1")
  public int getParallelSubmitters();
  
  @Option(shortName="r")
  public boolean isSetPartitionedRootPath();

  @Option(shortName="e")
  public boolean isDontUseSecAccno();

  @Option(shortName="d")
  public boolean isOutputDirPerFile();

  @Option(shortName="q", defaultValue="create")
  public String getOpeartion();
  
  @Option(shortName="m", defaultValue="")
  public String getMapFile();
  
  @Option(shortName="w", defaultValue="")
  public String getDownloadEPMCDir();

  @Option(shortName="l")
  public boolean getRefreshFiles();
  
  @Option(shortName="c")
  public boolean getRemoveDuplicates();
 
  @Option(defaultValue="0")
  public String getMaturationTimeHours();
 
  @Option(shortName="x")
  public boolean getFixCharset();
  
  @Option(shortName="v")
  public boolean getValidateOnly();

  @Option
  public boolean getIgnoreAbsentFiles();
  
  @Option(shortName="b", defaultValue="")
  public String getOnBehalf();
  
  @Option(defaultValue="")
  public String getLockerName();
}
