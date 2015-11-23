package uk.ac.ebi.biostd.tools.submitmass;

import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;

public class SubmitRequest
{
 private SubmissionInfo submissionInfo;
 private String         fileName;
 private int order;
 private int total;
 private int fileOrder;
 private int fileTotal;

 public SubmissionInfo getSubmissionInfo()
 {
  return submissionInfo;
 }

 public void setSubmissionInfo(SubmissionInfo submissionInfo)
 {
  this.submissionInfo = submissionInfo;
 }

 public String getFileName()
 {
  return fileName;
 }

 public void setFileName(String requestId)
 {
  this.fileName = requestId;
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

 public int getFileOrder()
 {
  return fileOrder;
 }

 public void setFileOrder(int fileOrder)
 {
  this.fileOrder = fileOrder;
 }

 public int getFileTotal()
 {
  return fileTotal;
 }

 public void setFileTotal(int fileTotal)
 {
  this.fileTotal = fileTotal;
 }
 
 @Override
 public String toString()
 {
  return "F: "+fileName+" "+fileOrder+"of"+fileTotal+" S: "+order+"of"+total;
 }
}
