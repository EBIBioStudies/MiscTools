package uk.ac.ebi.biostd.tools.submitmass;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Console
{
 private static Lock lock = new ReentrantLock();
 
 public static void println( String msg )
 {
  try
  {
   lock.lock();

   System.out.println(msg);
  }
  finally
  {
   lock.unlock();
  }
 }
}
