package io.warp10.ext.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

public class DynamicTBase implements TBase {
  
  public static final byte BINARY = -1;
  
  private String name = null;
  
  private Map<Integer, DynamicField> fieldsById = new TreeMap<Integer,DynamicField>();
  private Map<String, DynamicField> fieldsByName = new HashMap<String,DynamicField>();
  
  private Map<Object,Object> struct = new HashMap<Object,Object>();
    
  public DynamicTBase() {
  }
  
  public DynamicTBase(String name) {
    this.name = name;
  }
  
  public DynamicTBase(Map<Integer,DynamicField> fields) {
    this.fieldsById.putAll(fields);
    for (DynamicField field: fields.values()) {
      fieldsByName.put(field.getName(), field);
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
      DynamicField field = getFieldById(schemeField.id);
      
      // The field is unknown, skip
      if (null == field) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        continue;
      }
      
      // The field does not have the same type as the expected type, skip
      byte type = field.getType();      

      if (type != schemeField.type && (type != BINARY && schemeField.type != TType.STRING)) {
        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        continue;        
      }

      Object value = readValue(iprot, field);

      struct.put(field.getName(), value);
      iprot.readFieldEnd();      
    }
    iprot.readStructEnd();
  }

  private Object readValue(TProtocol iprot, DynamicField field) throws TException {
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
      value = (long) iprot.readI32();
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
  
  private DynamicField getFieldById(int id) {
    return fieldsById.get(id);
  }

  private DynamicField getFieldByName(String name) {
    return fieldsByName.get(name);
  }
  
  @Override
  public void write(TProtocol oprot) throws TException {
    System.out.println("write " + oprot); 
    //scheme(oprot).write(oprot, this);
  }

  @Override
  public int compareTo(Object o) {
    System.out.println("compareTo " + o);
    return 0;
  }

  @Override
  public TFieldIdEnum fieldForId(int fieldId) {
    System.out.println("fieldForId " + fieldId);
    return null;
  }

  @Override
  public boolean isSet(TFieldIdEnum field) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object getFieldValue(TFieldIdEnum field) {
    System.out.println("getFieldValue " + field);
    return null;
  }

  @Override
  public void setFieldValue(TFieldIdEnum field, Object value) {
    System.out.println("setFieldValue " + field + " " + value);
  }

  @Override
  public TBase deepCopy() {
    System.out.println("deepCopy");
    return null;
  }

  @Override
  public void clear() {
    System.out.println("clear");    
  }
  
  public void addField(DynamicField field) {
    if (null != fieldsById.put(field.getTag(), field)) {
      throw new RuntimeException("Field " + field.getTag() + " already defined.");
    }
    if (null != fieldsByName.put(field.getName(), field)) {
      throw new RuntimeException("Field '" + field.getName() + " already defined.");
    }
  }
  
  public DynamicTBase clone() {
    return new DynamicTBase(this.fieldsById);
  }
  
  public Map<Object,Object> getStruct() {
    return this.struct;
  }
  
  public String getName() {
    return this.name;
  }
  private void validate() {
    for (DynamicField field: this.fieldsById.values()) {
      if (field.isRequired() && !this.struct.containsKey(field.getName())) {
        throw new RuntimeException("Missing required field '" + field.getName() + "'.");
      }
    }
  }
}
