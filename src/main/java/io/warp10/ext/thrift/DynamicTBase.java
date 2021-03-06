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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.StandardScheme;

import io.warp10.WarpURLEncoder;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;

public class DynamicTBase extends DynamicType implements TBase, Snapshotable {
  
  public static final byte BINARY = -1;
  
  /**
   * Automatically provided tags are negative
   */
  private int nextTag = -1;
  
  private Map<Integer, DynamicType> fieldsById = new TreeMap<Integer,DynamicType>();
  
  private Map<String, DynamicType> fieldsByName = new LinkedHashMap<String,DynamicType>();
  
  private Map<Object,Object> struct = new HashMap<Object,Object>();
    
  public DynamicTBase() {
    super(TType.STRUCT);
  }
  
  public DynamicTBase(String name) {
    super(TType.STRUCT);
    setTypeName(name);
  }
  
  public DynamicTBase(Map<Integer,DynamicType> fields) {
    super(TType.STRUCT);
    this.fieldsById.putAll(fields);
    for (DynamicType field: fields.values()) {
      fieldsByName.put(field.getFieldName(), field);
    }
  }
  
  @Override
  public void read(TProtocol iprot) throws TException {
    
    if (!StandardScheme.class.equals(iprot.getScheme())) {
      readTuple(iprot);
      return;
    }
    
    org.apache.thrift.protocol.TField schemeField;
    iprot.readStructBegin();
    
    while (true) {
      schemeField = iprot.readFieldBegin();
      
      if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      
      // Attempt to retrieve the field
      DynamicType field = getFieldById(schemeField.id);
      
      // The field is unknown, skip
      if (null == field) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        continue;
      }
      
      // The field does not have the same type as the expected type, skip
      byte type = field.getType();      

      if (type != schemeField.type
          && (type != BINARY && schemeField.type != TType.STRING)
          && (type != TType.ENUM && schemeField.type != TType.I32)) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        continue;        
      }

      Object value = readValue(iprot, field);
      if (null != value) {
        struct.put(field.getFieldName(), value);
      }
      iprot.readFieldEnd();      
    }
    iprot.readStructEnd();
    
    validate();
  }

  private Object readValue(TProtocol iprot, DynamicType field) throws TException {
    Object value = null;
    
    byte type = field.getType();
    
    if (TType.BOOL == type) {
      value = iprot.readBool();
    } else if (TType.BYTE == type) {
      value = (long) iprot.readByte();
    } else if (TType.DOUBLE == type) {
      value = iprot.readDouble();
    } else if (TType.ENUM == type) {
      // Enum values are stored as 32 bit integers
      value = (int) iprot.readI32();
      value = ((DynamicEnum) field).valueOf(((Integer) value).intValue());
    } else if (TType.I16 == type) {
      value = (long) iprot.readI16();
    } else if (TType.I32 == type) {
      value = (long) iprot.readI32();
    } else if (TType.I64 == type) {
      value = iprot.readI64();
    } else if (TType.LIST == type) {
      TList _list = iprot.readListBegin();
      List<Object> list = new ArrayList<Object>(_list.size);
      for (int i = 0; i < _list.size; i++) {
        //_list.elemType
        list.add(readValue(iprot, field.getElementType()));
      }
      iprot.readListEnd();
      value = list;
    } else if (TType.MAP == type) {
      TMap _map0 = iprot.readMapBegin();
      Map<Object,Object> map = new LinkedHashMap<Object,Object>(_map0.size);
      for (int i = 0; i < _map0.size; i++) {
        Object key = readValue(iprot, field.getKeyType());
        Object val = readValue(iprot, field.getElementType());
        map.put(key, val);
      }
      iprot.readMapEnd();
      value = map;
    } else if (TType.SET == type) {
      TSet _set = iprot.readSetBegin();
      Set<Object> set = new HashSet<Object>(_set.size);
      for (int i = 0; i < _set.size; i++) {
        set.add(readValue(iprot, field.getElementType()));
      }
      iprot.readSetEnd();
      value = set;
    } else if (TType.STRING == type) {
      value = iprot.readString();
    } else if (TType.STRUCT == type) {
      DynamicTBase tb = new DynamicTBase(field.getFields());
      tb.read(iprot);
      value = tb.getStruct();
    } else if (TType.VOID == type) {   
    }
    
    return value;
  }
  
  private void readTuple(TProtocol iproto) throws TException {
    throw new TException("Unsupported tuple scheme.");
  }
  
  private DynamicType getFieldById(int id) {
    return fieldsById.get(id);
  }

  private DynamicType getFieldByName(String name) {
    return fieldsByName.get(name);
  }
  
  private void writeTuple(TProtocol oprot) throws TException {
    throw new TException("Unsupported tuple scheme.");
  }
  
  private void writeValue(TProtocol oprot, DynamicType field, Object value) throws TException {
    // Only call fieldBegin/End if the field is an actual one (vs the type description for containers)

    if (0 != field.getTag()) {
      oprot.writeFieldBegin(field.getTField());
    }

    if (field instanceof DynamicEnum) {
      if (value instanceof String) {
        value = ((DynamicEnum) field).valueOf((String) value);
      }
      oprot.writeI32(((Number) value).intValue());
    } else if (field instanceof DynamicTBase) {
      ((DynamicTBase) field).write(oprot);
    } else {
      // primitive or container type
      if (TType.BOOL == field.getType()) {       
        oprot.writeBool(Boolean.TRUE.equals(value));
      } else if (TType.BYTE == field.getType()) {
        oprot.writeByte((byte) value);          
      } else if (TType.DOUBLE == field.getType()) {
        oprot.writeDouble(((Double) value).doubleValue());          
      } else if (TType.I16 == field.getType()) {
        oprot.writeI16(((Number) value).shortValue());          
      } else if (TType.I32 == field.getType()) {
        oprot.writeI32(((Number) value).intValue());          
      } else if (TType.I64 == field.getType()) {
        oprot.writeI64(((Number) value).longValue());          
      } else if (TType.LIST == field.getType()) {
        oprot.writeListBegin(new TList(field.getElementType().getType(), ((List) value).size()));
        for (Object v: (List) value) {
          writeValue(oprot, field.getElementType(), v);
        }
        oprot.writeListEnd();
      } else if (TType.SET == field.getType()) {
        oprot.writeSetBegin(new TSet(field.getElementType().getType(), ((Set) value).size()));
        for (Object v: (Set) value) {
          writeValue(oprot, field.getElementType(), v);
        }
        oprot.writeSetEnd();          
      } else if (TType.MAP == field.getType()) {
        oprot.writeMapBegin(new TMap(field.getKeyType().getType(), field.getElementType().getType(), ((Map) value).size()));
        for (Entry<Object,Object> e: ((Map<Object,Object>) value).entrySet()) {
          writeValue(oprot, field.getKeyType(), e.getKey());
          writeValue(oprot, field.getElementType(), e.getValue());
        }
        oprot.writeMapEnd();
      } else if (TType.STRING == field.getType()) {
        oprot.writeString((String) value);
      } else if (BINARY == field.getType()) {
        oprot.writeBinary(ByteBuffer.wrap((byte[]) value));
      } else {
        throw new RuntimeException("Invalid type, cannot serialize.");
      }
    }
    
    if (0 != field.getTag()) {
      oprot.writeFieldEnd();
    }
  }
  
  @Override
  public void write(TProtocol oprot) throws TException {
    if (!StandardScheme.class.equals(oprot.getScheme())) {
      writeTuple(oprot);
      return;
    }

    validate();
    
    oprot.writeStructBegin(new TStruct(this.getTypeName()));
     
    // Start by writing
    for (DynamicType field: fieldsById.values()) {
      Object value = struct.get(field.getFieldName()); 
      
      if (null == value) {
        continue;
      }
      
      writeValue(oprot, field, value);
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public int compareTo(Object o) {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public TFieldIdEnum fieldForId(int fieldId) {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public boolean isSet(TFieldIdEnum field) {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public Object getFieldValue(TFieldIdEnum field) {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public void setFieldValue(TFieldIdEnum field, Object value) {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public TBase deepCopy() {
    throw new RuntimeException("Method not implemented.");
  }

  @Override
  public void clear() {
    throw new RuntimeException("Method not implemented.");
  }
  
  public void addField(DynamicType field) {
        
    int tag = field.getTag();
    
    // A tag value of 0 means that numbering should be automatic
    // The Thrift convention is to use decreasing negative numbers
    // for automatic tag ids
    if (0 == tag) {
      tag = nextTag--;
      // Modify tag
      field.setTag(tag);
    }
    
    if (null != fieldsById.put(tag, field)) {
      throw new RuntimeException("Field " + tag + " already defined.");
    }
    if (null != fieldsByName.put(field.getFieldName(), field)) {
      throw new RuntimeException("Field '" + field.getFieldName() + " already defined.");
    }
  }
  
  public Map<Object,Object> getStruct() {
    return this.struct;
  }
  
  private void validate() {
    for (DynamicType field: this.fieldsById.values()) {
      if (!struct.containsKey(field.getFieldName())) {
        Object defval = field.getDefaultValue(true);
        if (null != defval) {
          struct.put(field.getFieldName(), field.getDefaultValue(true));
        }
      }
      if (field.isRequired() && !this.struct.containsKey(field.getFieldName())) {
        throw new RuntimeException("Missing required field '" + field.getFieldName() + "'.");
      }
      if (!this.struct.containsKey(field.getFieldName())) {
        continue;
      }
      // Validate enum values
      if (TType.ENUM == field.getType()) {
        DynamicEnum de = (DynamicEnum) field;
        Object val = this.struct.get(field.getFieldName());
        de.validate(val);
      }
    }
  }
  
  public DynamicTBase clone() {
    DynamicTBase tb = new DynamicTBase();
    tb.setTypeName(this.getTypeName());
    for (Entry<Integer,DynamicType> type: this.fieldsById.entrySet()) {
      DynamicType t = type.getValue().clone();
      tb.fieldsById.put(type.getKey(), t);
      tb.fieldsByName.put(t.getFieldName(), t);
    }
    return tb;
  }
  
  /**
   * Return the list of types referenced by this type
   */
  @Override
  public Map<String,DynamicType> getTypes() {
    Map<String,DynamicType> alltypes = new LinkedHashMap<String,DynamicType>();
    
    for (DynamicType t: fieldsByName.values()) {
      if (t instanceof DynamicTBase) {
        Map<String,DynamicType> subtypes = ((DynamicTBase) t).getTypes();
        
        for (Entry<String,DynamicType> entry: subtypes.entrySet()) {
          if (!alltypes.containsKey(entry.getValue().getTypeName())) {
            alltypes.put(entry.getValue().getTypeName(), entry.getValue());
          }
        }
        
        if (!alltypes.containsKey(t.getTypeName())) {
          alltypes.put(t.getTypeName(), t);
        }
      } else if (t instanceof DynamicEnum) {
        if (!alltypes.containsKey(t.getTypeName())) {
          alltypes.put(t.getTypeName(), t);
        }
      } else {
        // Skip standard types
      }
    }
    
    return alltypes;
  }
    
  public String getThrift() {
    StringBuilder sb = new StringBuilder();
    sb.append("struct " + getTypeName() + " {\n");
    for (Entry<String, DynamicType> entry: fieldsByName.entrySet()) {
      sb.append("  " + entry.getValue().getTag() + ": ");
      if (entry.getValue().isOptional()) {
        sb.append("optional ");
      } else if (entry.getValue().isRequired()) {
        sb.append("required ");
      }
      // Type
      sb.append(entry.getValue().toString());
      sb.append(" ");
      // Identifier
      sb.append(entry.getKey());
      // Default value
      if (null != entry.getValue().getDefaultValue()) {          
        sb.append(" = ");
        sb.append(entry.getValue().getDefaultValueRepr(entry.getValue().getDefaultValue()));          
      }
      sb.append(",\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
  
  @Override
  public String snapshot() {

    StringBuilder sb = new StringBuilder();

    try {
      //
      // Gather all the types needed for the current struct by
      // inspecting all fields
      //

      Map<String,DynamicType> alltypes = getTypes();
              
      for (Entry<String,DynamicType> dt: alltypes.entrySet()) {
        if (dt.getValue() instanceof DynamicEnum) {
          sb.append(((DynamicEnum) dt.getValue()).getThrift());
          sb.append("\n");
        } else if (dt.getValue() instanceof DynamicTBase) {
          sb.append(((DynamicTBase) dt.getValue()).getThrift());
          sb.append("\n");
        }
      }        

      sb.append(getThrift());
      
      String idl = sb.toString();
      
      sb.setLength(0);
      sb.append("'");
      sb.append(WarpURLEncoder.encode(idl, StandardCharsets.UTF_8));
      sb.append("' ");
      // Start with an empty universe for the call to THRIFTC
      sb.append("{} ");
      sb.append(ThriftWarpScriptExtension.THRIFTC);
      sb.append(" ");
      sb.append("'");
      sb.append(WarpURLEncoder.encode(getTypeName(), StandardCharsets.UTF_8));
      sb.append("'");
      sb.append(" ");
      sb.append(WarpScriptLib.GET);        
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException(uee);
    }
        
    return sb.toString();
  }
  
  public void setStruct(Map<Object,Object> map) {
    this.struct = map;
  }
}
