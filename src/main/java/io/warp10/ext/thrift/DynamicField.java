package io.warp10.ext.thrift;

import java.util.Map;

import org.apache.thrift.protocol.TType;

import com.google.common.base.Preconditions;

public class DynamicField {
  
  private final short tag;
  private final byte type;
  private final String name;
  private final DynamicField keyType;
  private final DynamicField eltType;
  private final Map<Integer,DynamicField> fields;
  private final Modifier modifier;
  
  public enum Modifier {
    REQUIRED,
    OPTIONAL,
  }
  
  public DynamicField(byte type) {
    this(null, 0, null, type, null, null, null);
  }
  
  public DynamicField(int tag, String name, byte type) {
    this(null, tag, name, type, null, null, null);
  }

  public DynamicField(byte type, DynamicField elt) {
    this(null, 0, null, type, null, elt, null);
  }

  public DynamicField(DynamicField key, DynamicField value) {
    this(null, 0, null, TType.MAP, key, value, null);
  }

  public DynamicField(int tag, String name, byte type, DynamicField key, DynamicField value) {
    this(null, tag, name, type, key, value, null);
  }

  public DynamicField(Modifier modifier, int tag, String name, byte type, DynamicField key, DynamicField elt, Map<Integer,DynamicField> fields) {
    if (null != key && null != elt) {
      Preconditions.checkArgument(TType.MAP == type, "Invalid type, must be MAP.");
    } else if (null != elt) {
      Preconditions.checkArgument(TType.LIST == type || TType.SET == type, "Invalid type, must be SET or LIST.");
    }
    this.tag = (short) tag;
    this.name = name;
    this.type = type;
    this.eltType = elt;
    this.keyType = key;
    this.fields = fields;
    this.modifier = modifier;
  }
  
  public DynamicField getKeyType() {
    return keyType;
  }
  public DynamicField getElementType() {
    return eltType;
  }
  
  public String getName() {
    return name;
  }
  public byte getType() {
    return type;
  }
  
  public int getTag() {
    return tag;
  }
  
  public Map<Integer,DynamicField> getFields() {
    return this.fields;
  }
  
  public boolean isRequired() {
    return Modifier.REQUIRED == this.modifier;
  }
  
  public boolean isOptional() {
    return Modifier.OPTIONAL == this.modifier;
  }
}
