package uk.ac.ebi.biostd.tools.submitmass;

public class SubmissionPointer
{
 private int    order;
 private String fileName;

 public SubmissionPointer(String fn, int ord)
 {
  fileName = fn;
  order = ord;
 }

 public int getOrder()
 {
  return order;
 }

 public void setOrder(int order)
 {
  this.order = order;
 }

 public String getFileName()
 {
  return fileName;
 }

 public void setFileName(String fileName)
 {
  this.fileName = fileName;
 }

 @Override
 public String toString()
 {
  return fileName+':'+order;
 }
}
