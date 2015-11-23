package uk.ac.ebi.biostd.tools.submitmass;

import java.io.File;

public class FileRequest
{
 private String requestId;
 private File   file;
 private int order;
 private int total;
 
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

 public int getOrder()
 {
  return order;
 }

 public void setOrder(int order)
 {
  this.order = order;
 }

 public int getTotal()
 {
  return total;
 }

 public void setTotal(int total)
 {
  this.total = total;
 }
 
 @Override
 public String toString()
 {
  return requestId+" "+order+"of"+total;
 }
}
