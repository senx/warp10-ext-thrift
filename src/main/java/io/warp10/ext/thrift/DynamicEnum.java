package io.warp10.ext.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.protocol.TType;

import io.warp10.WarpURLEncoder;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;

public class DynamicEnum extends DynamicType implements Snapshotable {
  
  private final Map<Integer,String> entries = new HashMap<Integer,String>();
  private int nextOrdinal = 0;
  
  public DynamicEnum(String name) {
    super(TType.ENUM);
    setTypeName(name);
  }
  
  public void add(String name) {
    add(name, nextOrdinal);
  }
  public void add(String name, int ordinal) {    
    if (entries.containsValue(name)) {
      throw new RuntimeException("Duplicate name '" + name + "'.");
    }
    if (null != entries.put(ordinal, name)) {
      throw new RuntimeException("Duplicate ordinal " + ordinal);
    }
    nextOrdinal = ordinal + 1;
  }
  
  public DynamicEnum clone() {
    DynamicEnum de = new DynamicEnum(getTypeName());
    de.nextOrdinal = this.nextOrdinal;
    de.entries.clear();
    de.entries.putAll(this.entries);
    return de;
  }
  
  public String getThrift() {
    StringBuilder sb = new StringBuilder();
    sb.append("enum " + getTypeName() + " {\n");
    for (Entry<Integer,String> entry: entries.entrySet()) {
      sb.append("  " + entry.getValue() + " = " + entry.getKey() + ",\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
  
  @Override
  public String snapshot() {
    StringBuilder sb = new StringBuilder();
    
    try {
      
      String def = "'" + WarpURLEncoder.encode(getThrift(), StandardCharsets.UTF_8) + "' ";
      sb.setLength(0);
      sb.append(def);
      sb.append(ThriftWarpScriptExtension.THRIFTC);
      sb.append(" ");
      sb.append("'" + WarpURLEncoder.encode(getTypeName(), StandardCharsets.UTF_8) + "'");
      sb.append(" ");
      sb.append(WarpScriptLib.GET);
      return sb.toString();      
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
  }
  
  public int valueOf(String name) {
    for (Entry<Integer,String> entry: entries.entrySet()) {
      if (entry.getValue().equals(name)) {
        return entry.getKey();
      }
    }
    throw new RuntimeException("Unknown enum value '" + name + "'.");
  }
  
  public String toString() {
    return getTypeName();
  }
}
