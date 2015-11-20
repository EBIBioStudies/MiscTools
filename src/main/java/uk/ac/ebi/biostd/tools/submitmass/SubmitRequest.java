package uk.ac.ebi.biostd.tools.submitmass;

import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;

public class SubmitRequest
{
 private SubmissionInfo submissionInfo;
 private String         requestId;

 public SubmissionInfo getSubmissionInfo()
 {
  return submissionInfo;
 }

 public void setSubmissionInfo(SubmissionInfo submissionInfo)
 {
  this.submissionInfo = submissionInfo;
 }

 public String getRequestId()
 {
  return requestId;
 }

 public void setRequestId(String requestId)
 {
  this.requestId = requestId;
 }
}
