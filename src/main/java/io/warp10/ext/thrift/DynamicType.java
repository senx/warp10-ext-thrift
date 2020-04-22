package io.warp10.ext.thrift;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;

import com.google.common.base.Preconditions;

/**
 * This class describes a Thrift type
 */
public class DynamicType {
  
  private short tag = 0;
  private final byte type;
  private String typeName = null;
  private String fieldName = null;
  private Object defaultValue = null;
  
  /**
   * Used for maps and lists
   */
  private DynamicType keyType;
  private DynamicType eltType;
  
  /**
   * Used for structs
   */
  private final Map<Integer,DynamicType> fields;
  private Modifier modifier;
  
  public enum Modifier {
    REQUIRED,
    OPTIONAL,
  }

  /**
   * Simple type
   */
  public DynamicType(byte type) {
    this(null, 0, null, type, null, null, null);
  }
  
  /**
   * Constructor for sets and lists
   * @param type
   * @param elt
   */
  public DynamicType(byte type, DynamicType elt) {
    this(null, 0, null, type, null, elt, null);
  }

  public DynamicType(int tag, String name, byte type) {
    this(null, tag, name, type, null, null, null);
  }
  
  /**
   * Constructor for maps
   * @param key
   * @param value
   */
  public DynamicType(DynamicType key, DynamicType value) {
    this(null, 0, null, TType.MAP, key, value, null);
  }

  public DynamicType(Modifier modifier, int tag, String fieldName, byte type, DynamicType key, DynamicType elt, Map<Integer,DynamicType> fields) {
    if (null != key && null != elt) {
      Preconditions.checkArgument(TType.MAP == type, "Invalid type, must be MAP.");
    } else if (null != elt) {
      Preconditions.checkArgument(TType.LIST == type || TType.SET == type, "Invalid type, must be SET or LIST.");
    }
    this.tag = (short) tag;
    this.fieldName = fieldName;
    this.type = type;
    this.eltType = elt;
    this.keyType = key;
    this.fields = fields;
    this.modifier = modifier;
  }
  
  public DynamicType getKeyType() {
    return keyType;
  }
  public DynamicType getElementType() {
    return eltType;
  }
  
  public void setTypeName(String name) {
    this.typeName = name;
  }

  public void setFieldName(String name) {
    this.fieldName = name;
  }

  public byte getType() {
    return type;
  }
  
  public int getTag() {
    return tag;
  }
  
  public void setTag(int tag) {
    this.tag = (short) tag;
  }
  
  public Map<Integer,DynamicType> getFields() {
    return this.fields;
  }
  
  public void setModifier(Modifier mod) {
    this.modifier = mod;
  }
  
  public boolean isRequired() {
    return Modifier.REQUIRED == this.modifier;
  }
  
  public boolean isOptional() {
    return Modifier.OPTIONAL == this.modifier;
  }
  
  public void setDefaultValue(Object val) {
    this.defaultValue = val;
  }
  
  public Object getDefaultValue() {
    return getDefaultValue(false);
  }
  
  public Object getDefaultValue(boolean clone) {
    if (!clone) {
      return this.defaultValue;
    } else {
      return clone(this.defaultValue);
    }
  }
  
  private Object clone(Object in) {
    if (null == in) {
      return null;
    } else if (in instanceof Number) {
      return in;
    } else if (in instanceof Boolean) {
      return in;
    } else if (in instanceof String) {
      return in;
    } else if (in instanceof List) {
      ArrayList<Object> l = new ArrayList<Object>(((List) in).size());
      for (Object elt: (List) in) {
        l.add(clone(elt));
      }
      return l;
    } else if (in instanceof Map) {
      Map<Object,Object> m = new LinkedHashMap<Object,Object>(((Map) in).size());
      for (Entry<Object,Object> e: ((Map<Object,Object>) in).entrySet()) {
        m.put(clone(e.getKey()), clone(e.getValue()));
      }
      return m;
    } else {
      throw new RuntimeException("Invalid type.");
    }
  }
  public String getDefaultValueRepr(Object v) {
    if (null == v) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) this.defaultValue).toString();
    } else if (v instanceof Map) {
      StringBuilder sb = new StringBuilder();
      sb.append("{ ");
      for (Entry<Object,Object> entry: ((Map<Object,Object>) v).entrySet()) {
        sb.append(getDefaultValueRepr(entry.getKey()));
        sb.append(" : ");
        sb.append(getDefaultValueRepr(entry.getValue()));
        sb.append(", ");
      }
      sb.append(" }");
      return sb.toString();
    } else if (v instanceof List) {
      StringBuilder sb = new StringBuilder();
      sb.append("[ ");
      for (Object elt: (List) v) {
        sb.append(getDefaultValueRepr(elt));
        sb.append(", ");
      }
      sb.append(" ]");
      return sb.toString();
    } else if (v instanceof String) {
      return "'" + v + "'";
    } else if (v instanceof Boolean) {
      return v.toString();
    } else {
      throw new RuntimeException("Invalid const value.");
    }
  }
  
  public DynamicType clone() {
    DynamicType dt = new DynamicType(this.type);
    dt.defaultValue = this.defaultValue;
    if (null != this.eltType) {
      dt.eltType = this.eltType.clone();
    }
    if (null != this.keyType) {
      dt.keyType = this.keyType.clone();
    }
    if (null != this.fields) {
      for (Entry<Integer,DynamicType> entry: this.fields.entrySet()) {
        dt.fields.put(entry.getKey(), entry.getValue().clone());
      }
    }
    dt.modifier = this.modifier;
    dt.fieldName = this.fieldName;
    dt.typeName = this.typeName;
    
    return dt;
  }
  
  public Map<String,DynamicType> getTypes() {
    Map<String,DynamicType> alltypes = new LinkedHashMap<String,DynamicType>();
    
    if (null != keyType) {
      Map<String,DynamicType> subtypes = keyType.getTypes();
      for (Entry<String,DynamicType> entry: subtypes.entrySet()) {
        if (!alltypes.containsKey(entry.getKey())) {
          alltypes.put(entry.getKey(), entry.getValue());
        }
      }      
    }
    if (null != eltType) {
      Map<String,DynamicType> subtypes = eltType.getTypes();      
      for (Entry<String,DynamicType> entry: subtypes.entrySet()) {
        if (!alltypes.containsKey(entry.getKey())) {
          alltypes.put(entry.getKey(), entry.getValue());
        }
      }
    }
    
    return alltypes;
  }
  
  public String toString() {
    if (TType.BOOL == this.type) {
      return "bool";
    } else if (TType.BYTE == this.type) {
      return "byte";
    } else if (TType.DOUBLE == this.type) {
      return "double";
    } else if (TType.ENUM == this.type) {
      return this.typeName;
    } else if (TType.I16 == this.type) {
      return "i16";
    } else if (TType.I32 == this.type) {
      return "i32";
    } else if (TType.I64 == this.type) {
      return "i64";
    } else if (TType.LIST == this.type) {
      return "list<" + eltType.toString() + ">";
    } else if (TType.MAP == this.type) {
      return "map<" + keyType.toString() + "," + eltType.toString() + ">";
    } else if (TType.SET == this.type) {
      return "set<" + eltType.toString() + ">";
    } else if (TType.STRING == this.type) {
      return "string";
    } else if (TType.STRUCT == this.type) {
      return this.typeName;
    } else if (DynamicTBase.BINARY == this.type) {
      return "binary";
    }
    
    throw new RuntimeException("Invalid type " + this.type);
  }
  
  public String getFieldName() {
    return this.fieldName;
  }
  
  public String getTypeName() {
    if (TType.BOOL == this.type) {
      return "bool";
    } else if (TType.BYTE == this.type) {
      return "byte";
    } else if (TType.DOUBLE == this.type) {
      return "double";
    } else if (TType.ENUM == this.type) {
      return this.typeName;
    } else if (TType.I16 == this.type) {
      return "i16";
    } else if (TType.I32 == this.type) {
      return "i32";
    } else if (TType.I64 == this.type) {
      return "i64";
    } else if (TType.LIST == this.type) {
      return "list<" + eltType.toString() + ">";
    } else if (TType.MAP == this.type) {
      return "map<" + keyType.toString() + "," + eltType.toString() + ">";
    } else if (TType.SET == this.type) {
      return "set<" + eltType.toString() + ">";
    } else if (TType.STRING == this.type) {
      return "string";
    } else if (TType.STRUCT == this.type) {
      return this.typeName;
    } else if (DynamicTBase.BINARY == this.type) {
      return "binary";
    } else {
      return this.typeName;
    }
  }
  
  public TField getTField() {
    return new TField(getFieldName(), getType(), (short) getTag());
  }
}
