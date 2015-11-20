package uk.ac.ebi.biostd.tools.submitmass;

public class SubmitException extends Exception
{
 private static final long serialVersionUID = 1L;

 public SubmitException()
 {
 }

 public SubmitException( String s )
 {
  super(s);
 }
 
 public SubmitException( String s, Throwable cause )
 {
  super(s, cause);
 }

 
}
