package io.warp10.ext.thrift;

import java.util.HashMap;
import java.util.Map;

public class DynamicEnum {
  
  private final String name;
  private final Map<Integer,String> entries = new HashMap<Integer,String>();
      
  public DynamicEnum(String name) {
    this.name = name;
  }
  
  public void add(String name, int ordinal) {    
    if (entries.containsValue(name)) {
      throw new RuntimeException("Duplicate name '" + name + "'.");
    }
    if (null != entries.put(ordinal, name)) {
      throw new RuntimeException("Duplicate ordinal " + ordinal);
    }   
  }
  
  public String getName() {
    return this.name;
  }
}
