package uk.ac.ebi.biostd.tools.fetchpmc;

import java.util.HashMap;
import java.util.Map;

import uk.ac.ebi.biostd.authz.AccessTag;
import uk.ac.ebi.biostd.authz.Classifier;
import uk.ac.ebi.biostd.authz.Tag;
import uk.ac.ebi.biostd.db.TagResolver;

public class AdHocTagResolver implements TagResolver
{
 private Map<String, Classifier> clsfMap = new HashMap<String, Classifier>();
 private Map<String, AccessTag> accTagMap = new HashMap<String, AccessTag>();
 
 private int idGen = 1;
 
 public Tag getTagByName(String clsfName, String tagName)
 {
  Classifier clsf = clsfMap.get(clsfName);
  
  Tag t = null;
  
  if( clsf == null )
  {
   clsfMap.put(clsfName, clsf = new Classifier() );
   
   clsf.setName(clsfName);
  }
  else
   t = clsf.getTag(tagName);
  
  if( t == null )
  {
   t = new Tag();
   
   t.setName(tagName);
   t.setId(idGen++);
   
   t.setClassifier(clsf);
   clsf.addTag(t);
  }
  
  return t;
 }

 public AccessTag getAccessTagByName(String tagName)
 {
  AccessTag acct = accTagMap.get(tagName);
  
  if( acct == null )
  {
   accTagMap.put(tagName, acct = new AccessTag() );
   
   acct.setId(idGen++);
   acct.setName(tagName);
  }
  
  return acct;
 }

}
