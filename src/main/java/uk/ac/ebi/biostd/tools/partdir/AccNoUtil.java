package uk.ac.ebi.biostd.tools.partdir;

public class AccNoUtil
{

 private static boolean checkCharClass( char ch )
 {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '-';
 }
 
 public static String encode( String src )
 {
  StringBuilder sb = null;
  
  int len = src.length();
  
  for( int i=0; i < len; i++ )
  {
   char ch = src.charAt(i);
   
   if( checkCharClass(ch) )
   {
    if( sb != null )
     sb.append(ch);
   }
   else
   {
    if( sb == null )
    {
     sb=new StringBuilder( len*4 );
     
     if( i > 0 )
      sb.append(src.substring(0,i) );
    }
    
    sb.append('#').append(Integer.toHexString(ch)).append('#');
   }
  }
  
  if( sb != null )
   return sb.toString();
  
  return src;
 }
 
 public static boolean checkAccNoStr( String src )
 {
  int len = src.length();
  
  for( int i=0; i < len; i++ )
  {
   char ch = src.charAt(i);
   
   if( !checkCharClass(ch) )
    return false;
  }
  
  return true;
 }
 
 public static String getPartitionedPath( String acc )
 {
  StringBuilder sb = new StringBuilder();
  
  for( String pt : partition(acc) )
   sb.append(pt).append('/');
  
  sb.setLength( sb.length() -1 );
   
  return sb.toString();
 }
 
 public static String[] partition( String acc )
 {
//  if( acc.startsWith("S-") )
//   acc = acc.substring(2);
  
  int len = acc.length();
  
  int nBegPos=-1, nEndPos=-1; 
  
  for(int i=0; i < len; i++ )
  {
   char ch = acc.charAt(i);
   
   if( Character.isDigit(ch) )
   {
    if( nBegPos < 0 )
     nBegPos = i;
   }
   else if( nBegPos >=0 )
   {
    nEndPos = i;
    break;
   }
    
  }
  
  if( nBegPos >= 0 && nEndPos < 0 )
   nEndPos=len;
  
  if( nBegPos < 0 )
   return new String[]{acc};
  
  String part = null;
  
  if( nEndPos - nBegPos < 3 )
   part = "0-99";
  else
   part = "xxx"+acc.substring(nEndPos-3,nEndPos);
  
  String pfx = nBegPos == 0? "": encode( acc.substring(0,nBegPos) );

  String[] res = new String[ nBegPos == 0? 2:3 ];
  
   
  String sfx = nEndPos < len? encode( acc.substring(nEndPos) ):"";
  
  int n=0;
  
  if( nBegPos > 0 )
   res[n++]=pfx;

  res[n++]=pfx+part+sfx;
  
  res[n]=encode(acc);
  
  return  res;
 }
}
