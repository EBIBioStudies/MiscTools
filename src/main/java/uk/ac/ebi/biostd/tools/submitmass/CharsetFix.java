package uk.ac.ebi.biostd.tools.submitmass;

import java.util.Collection;

import uk.ac.ebi.arrayexpress.utils.StringTools;
import uk.ac.ebi.biostd.in.pagetab.SubmissionInfo;
import uk.ac.ebi.biostd.model.AbstractAttribute;
import uk.ac.ebi.biostd.model.NameValuePair;
import uk.ac.ebi.biostd.model.Node;
import uk.ac.ebi.biostd.model.Section;
import uk.ac.ebi.biostd.model.Submission;

public class CharsetFix
{
 public static void fixSubmissionCharset(SubmissionInfo si)
 {
  Submission s = si.getSubmission();
  
  fixNode(s);
  
  if( s.getRootSection() != null )
   fixSection( s.getRootSection() );
 }

 public static void fixSection( Section s )
 {
  fixNode(s);
  
  if( s.getSections() != null )
  {
   for( Section ss : s.getSections() )
    fixSection(ss);
  }
 }
 
 public static void fixNode( Node n )
 {
  if( n.getAttributes() == null )
   return;
  
  
  for( AbstractAttribute aa : n.getAttributes() )
  {
   fixPair(aa);
   
   fixPairs( aa.getNameQualifiers() );
   fixPairs( aa.getValueQualifiers() );
  }
  
 }
 
 public static void fixPairs( Collection<? extends NameValuePair> nvlist )
 {
  if( nvlist == null )
   return;
  
  for( NameValuePair nv : nvlist )
   fixPair(nv);
 }
 
 public static void fixPair( NameValuePair aa )
 {
  aa.setName(StringTools.detectDecodeUTF8Sequences(aa.getName()));
  aa.setValue(StringTools.detectDecodeUTF8Sequences(aa.getValue()));
 }
 
 public static void fixPairRep( NameValuePair aa )
 {
  String nm = aa.getName();

  String fxn=StringTools.detectDecodeUTF8Sequences(nm);
    
  if( ! fxn.equals(nm) )
  {
   int l = Math.min(fxn.length(), nm.length());
   
   for( int i=0; i < l; i++ )
   {
    if( nm.charAt(i) != fxn.charAt(i) )
    {
     System.out.println("Orgnl attr name :"+nm.substring(i)+"\nFixed attr name: "+fxn.substring(i));
     break;
    }
   }
   
  }
  String vl = aa.getValue();
  
  String fxv=StringTools.detectDecodeUTF8Sequences(vl);
  
  if( ! fxv.equals(vl) )
  {
   int l = Math.min(fxv.length(), vl.length());
   
   for( int i=0; i < l; i++ )
   {
    if( vl.charAt(i) != fxv.charAt(i) )
    {
     System.out.println("Orgnl attr val :"+vl.substring(i)+"\nFixed attr val: "+fxv.substring(i));
     break;
    }
   }
   
  }
 }

}
