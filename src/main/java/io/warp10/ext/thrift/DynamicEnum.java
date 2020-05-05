//
//   Copyright 2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.ext.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.protocol.TField;
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
    de.setDefaultValue(this.getDefaultValue());
    de.setFieldName(this.getFieldName());
    de.setModifier(this.getModifier());
    de.setTag(this.getTag());
    
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
  
  public void validate(Object val) {
    if (val instanceof Long) {
      int ordinal = ((Long) val).intValue();
      if (!entries.containsKey(ordinal)) {
        throw new RuntimeException("Invalid numeric enum value " + val);
      }
    } else if (val instanceof String) {
      valueOf((String) val);
    } else {
      throw new RuntimeException("Invalid enum value " + val);
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
  
  public String valueOf(int ordinal) {
    String value = entries.get(ordinal);
    return value;
  }
  
  public String toString() {
    return getTypeName();
  }
  
  public TField getTField() {
    return new TField(getFieldName(), TType.I32, (short) getTag());
  }
}
