package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;

public class FileRequest
{
 private String requestId;
 private File   file;

 public String getRequestId()
 {
  return requestId;
 }

 public void setRequestId(String requestId)
 {
  this.requestId = requestId;
 }

 public File getFile()
 {
  return file;
 }

 public void setFile(File filePath)
 {
  this.file = filePath;
 }
}
