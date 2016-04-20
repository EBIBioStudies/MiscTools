package uk.ac.ebi.biostd.tools.chkenc;

import java.io.PrintWriter;
import java.util.Collection;

import uk.ac.ebi.arrayexpress.utils.StringTools;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.model.AbstractAttribute;
import uk.ac.ebi.biostd.model.NameValuePair;
import uk.ac.ebi.biostd.model.Node;
import uk.ac.ebi.biostd.model.Section;
import uk.ac.ebi.biostd.model.Submission;

import com.pri.util.StringUtils;

public class CharsetCheck
{
 public static void checkSubmissionCharset(SubmissionInfo si, PrintWriter out, String fn )
 {
  Submission s = si.getSubmission();
  
  fixNode(s, out, fn);
  
  if( s.getRootSection() != null )
   fixSection( s.getRootSection(), out, fn );
 }

 public static void fixSection( Section s, PrintWriter out, String fn )
 {
  fixNode(s, out, fn);
  
  if( s.getSections() != null )
  {
   for( Section ss : s.getSections() )
    fixSection(ss, out, fn);
  }
 }
 
 public static void fixNode( Node n, PrintWriter out, String fn  )
 {
  if( n.getAttributes() == null )
   return;
  
  
  for( AbstractAttribute aa : n.getAttributes() )
  {
   fixPair(aa, out, fn);
   
   fixPairs( aa.getNameQualifiers(), out, fn );
   fixPairs( aa.getValueQualifiers(), out, fn );
  }
  
 }
 
 public static void fixPairs( Collection<? extends NameValuePair> nvlist, PrintWriter out, String fn  )
 {
  if( nvlist == null )
   return;
  
  for( NameValuePair nv : nvlist )
   fixPair(nv, out, fn);
 }
 
 
 private static boolean isUsual( char ch )
 {
  return ch >= 0x20 && ch < 0x7D;
 }
 
 private static void checkString( String s, PrintWriter out, String fn )
 {
  boolean un=false;
  
  StringBuilder sb=null;
  int sbptr=0;
  
  int cmp=-1;
  
  for( int i=0; i < s.length(); i++ )
  {
   if( ! isUsual(s.charAt(i) ) )
   {
    if( cmp < 0 )
     cmp = s.equals(StringTools.detectDecodeUTF8Sequences(s))?0:1;
    
    if( sb == null )
     sb=new StringBuilder();
    
    if( sbptr < i )
     sb.append(StringUtils.htmlEscaped(s.substring(sbptr, i) ) );
    
    sbptr=i+1;
    
    if( ! un )
     sb.append("<span class='"+(cmp==0?"nf":"fx")+"'>");
    
    sb.append(s.charAt(i));
    
//    out.printf("O: %03d %s\n",i+8,s);
//    out.printf("F: %03d %s\n",i+8,StringTools.detectDecodeUTF8Sequences(s));
     un=true;
    
   }
   else
   {
    if( un )
    {
     un=false;
     sb.append("</span>");
    }
   }
  }
  
  if( sb != null )
  {
   if( sbptr < s.length() )
    sb.append( StringUtils.htmlEscaped( s.substring(sbptr) ) );
  
   out.append("<tr><td><input type='checkbox' value='"+fn+"'><br>"+fn+"</td>");
   
   if( cmp == 0 )
   {
    out.append("<td colspan=2>");
    out.append(sb.toString());
   }
   else
   {
    out.append("<td>");
    out.append(sb.toString());
    out.append("</td><td>");
    out.append(StringUtils.htmlEscaped( StringTools.detectDecodeUTF8Sequences(s)));
   }
   
   out.append("</td></tr>");
  }
 }
 
 public static void fixPair( NameValuePair aa, PrintWriter out , String fn )
 {
  checkString( aa.getName(), out, fn );
  checkString( aa.getValue(), out, fn );

 }

}
