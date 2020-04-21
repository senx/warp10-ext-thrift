package io.warp10.ext.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.protocol.TType;

import io.warp10.ext.thrift.DynamicType.Modifier;
import io.warp10.ext.thrift.antlr.ThriftParser.Base_typeContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_listContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_map_entryContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_ruleContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_valueContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Container_typeContext;
import io.warp10.ext.thrift.antlr.ThriftParser.DefinitionContext;
import io.warp10.ext.thrift.antlr.ThriftParser.DocumentContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Enum_fieldContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Enum_ruleContext;
import io.warp10.ext.thrift.antlr.ThriftParser.FieldContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Field_typeContext;
import io.warp10.ext.thrift.antlr.ThriftParser.StructContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Type_annotationContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Type_annotationsContext;
import io.warp10.ext.thrift.antlr.ThriftParser.TypedefContext;
import io.warp10.ext.thrift.antlr.ThriftParser.UnionContext;

public class DynamicTBaseGenerator {
  
  private DynamicEnum currentEnum = null;
  private int nextOrdinal = 0;
  private List<DynamicTBase> currentStructs = new ArrayList<DynamicTBase>();
  
  private Map<String,DynamicType> types = new LinkedHashMap<String,DynamicType>();
  //private Map<String,DynamicTBase> structs = new LinkedHashMap<String,DynamicTBase>();
  //private Map<String,DynamicEnum> enums = new LinkedHashMap<String,DynamicEnum>();
  //private Map<String,DynamicType> typedefs = new LinkedHashMap<String,DynamicType>();
  
  public DynamicTBaseGenerator() {
    this(null);
  }
  
  public DynamicTBaseGenerator(Map<Object,Object> types) {
    if (null != types) {
      for (Entry<Object,Object> entry: types.entrySet()) {
        if (!(entry.getKey() instanceof String)) {
          continue;
        }
        if (entry.getValue() instanceof DynamicType) {
          types.put((String) entry.getKey(), ((DynamicTBase) entry.getValue()).clone());
        }
      }
    }
  }
  
  public void generate(Object context) {
    if (null == context) {
      return;
    }
    
    if (context instanceof DocumentContext) {
      DocumentContext dc = (DocumentContext) context;
      for (DefinitionContext def: dc.definition()) {
        generate(def);
      }
    } else if (context instanceof DefinitionContext) {
      DefinitionContext dc = (DefinitionContext) context;
      generate(dc.struct());
      generate(dc.union());
      generate(dc.typedef());
      generate(dc.const_rule());
      generate(dc.enum_rule());
    } else if (context instanceof Const_ruleContext) {
      Const_ruleContext cc = (Const_ruleContext) context;
    } else if (context instanceof StructContext) {      
      StructContext sc = (StructContext) context;
      
      currentStructs.add(0, new DynamicTBase(sc.IDENTIFIER().getText()));
      
      for (FieldContext fc: sc.field()) {
        generate(fc);
      }
      
      DynamicTBase dtb = currentStructs.remove(0); 
      //structs.put(dtb.getName(), dtb);
      types.put(dtb.getTypeName(), dtb);
    } else if (context instanceof FieldContext) {
      FieldContext fc = (FieldContext) context;
            
      String name = fc.IDENTIFIER().getText();
      
      int tag = 0;
      
      // We allow explicit negative tag ids which the thrift compiler
      // does not support, but we do to ease SNAPSHOTing of DynamicTBase elements
      if (null != fc.field_id()) {
        tag = Integer.parseInt(fc.field_id().integer().INTEGER().getText());
      }
      
      DynamicType type = getType(fc.field_type());
      
      type.setTag(tag);
      
      if (null != fc.field_req()) {
        if ("required".equals(fc.field_req().getText())) {          
          type.setModifier(Modifier.REQUIRED);
        } else if ("optional".equals(fc.field_req().getText())) {
          type.setModifier(Modifier.OPTIONAL);
        } else {
          throw new RuntimeException("Invalid modifier '" + fc.field_req().getText() + "'.");
        }
      }
      
      type.setFieldName(fc.IDENTIFIER().getText());
      currentStructs.get(0).addField(type);
      if (null != fc.const_value()) {
        Const_valueContext cc = fc.const_value();
        Object cvalue = getConstant(cc);
      
        type.setDefaultValue(cvalue);
      }
    } else if (context instanceof TypedefContext) {
      TypedefContext tc = (TypedefContext) context;
      String name = tc.IDENTIFIER().getText();
      Field_typeContext fc = tc.field_type();
      DynamicType type = null;
            
      if (null != fc.IDENTIFIER()) {
        String tname = fc.IDENTIFIER().getText();      
        type = resolve(tname);
      } else if (null != fc.base_type()) {
        type = resolve(fc.base_type().getText());
      } else if (null != fc.container_type()) {
        type = getContainerType(fc.container_type());
      }

      //###if (structs.containsKey(name) || enums.containsKey(name) || typedefs.containsKey(name)) {
      if (types.containsKey(name)) {
        throw new RuntimeException("Alias '" + name + "' already in use.");
      }

      //###typedefs.put(name, type);
      types.put(name, type);
    } else if (context instanceof UnionContext) {
      UnionContext uc = (UnionContext) context;
      throw new RuntimeException("Union are not supported.");
    } else if (context instanceof Enum_ruleContext) {
      Enum_ruleContext ec = (Enum_ruleContext) context;
      currentEnum = new DynamicEnum(ec.IDENTIFIER().getText());
      nextOrdinal = 0;
      for (Enum_fieldContext field: ec.enum_field()) {
        generate(field);
      }
      //###enums.put(currentEnum.getName(), currentEnum);
      types.put(currentEnum.getTypeName(), currentEnum);
      currentEnum = null;
    } else if (context instanceof Enum_fieldContext) {
      Enum_fieldContext ec = (Enum_fieldContext) context;
      String name = ec.IDENTIFIER().getText();
      int ordinal = nextOrdinal;
      
      if (null == ec.integer()) {
        ordinal = nextOrdinal++;
      } else if (null != ec.integer().INTEGER()) {
        ordinal = Integer.parseInt(ec.integer().INTEGER().getText());
        nextOrdinal = ordinal + 1;
      } else if (null != ec.integer().HEX_INTEGER()) {
        ordinal = Integer.parseUnsignedInt(ec.integer().HEX_INTEGER().getText().substring(2), 16);
        nextOrdinal = ordinal + 1;
      }
      currentEnum.add(name, ordinal);
    }
  }
  
  /**
   * Return a fresh instance of DynamicType matching the type definition
   * @param type
   * @return
   */
  private DynamicType getType(Field_typeContext type) {
    if (null != type.base_type()) {
      Base_typeContext bc = (Base_typeContext) type.base_type();
      
      // Annotations are ignored as they are targeted to code generation
      
      String tname = bc.getText();
      return getBaseType(tname);
    } else if (null != type.container_type()) {      
      return getContainerType(type.container_type());
    } else {
      // Check that the type is known in either enums or structs
      DynamicType t = resolve(type.IDENTIFIER().getText());
      
      if (null == t) {
        throw new RuntimeException("Unknown type '" + type.IDENTIFIER().getText() + "'.");
      }
            
      return t.clone();
    }
  }
  
  public DynamicType getContainerType(Container_typeContext type) {
    if (null != type.list_type()) {      
      DynamicType df = getType(type.list_type().field_type());
      return new DynamicType(TType.LIST, df);
    } else if (null != type.map_type()) {
      DynamicType keydf = getType(type.map_type().field_type(0));
      DynamicType valuedf = getType(type.map_type().field_type(1));
      return new DynamicType(keydf, valuedf);
    } else if (null != type.set_type()) {
      DynamicType df = getType(type.set_type().field_type());
      return new DynamicType(TType.SET, df);
    } else {
      throw new RuntimeException("Invalid type " + type.getText());
    }
  }
  private DynamicType resolve(String name) {
    String oname = name;
    
    DynamicType btype = getBaseType(name);

    if (null != btype) {
      return btype;
    }
    
    DynamicType t = types.get(name); //###typedefs.get(name);
    
    if (null != t) {
      return t;
    } else {
      throw new RuntimeException("Unknown type '" + oname + "'.");
    }
  }
  
  private Object getConstant(Const_valueContext cc) {
    Object cvalue = null;
    
    if (null != cc.integer()) {
      if (null != cc.integer().INTEGER()) {
        cvalue = Long.parseLong(cc.integer().INTEGER().getText());           
      } else if (null != cc.integer().HEX_INTEGER()) {
        cvalue = Long.parseUnsignedLong(cc.integer().HEX_INTEGER().getText().substring(2), 16);            
      }
    } else if (null != cc.DOUBLE()) {
      cvalue = Double.parseDouble(cc.DOUBLE().getText());
    } else if (null != cc.LITERAL()) {
      // "STRING"
      String sconst = cc.LITERAL().getText(); 
      sconst = sconst.substring(1);
      sconst = sconst.substring(0,sconst.length() - 1);
      cvalue = sconst;
    } else if (null != cc.const_list()) {
      List<Object> elts = new ArrayList<Object>(cc.const_list().const_value().size());
      for (Const_valueContext v: cc.const_list().const_value()) {
        elts.add(getConstant(v));
      }
      cvalue = elts;
    } else if (null != cc.const_map()) {
      Map<Object,Object> elts = new HashMap<Object,Object>(cc.const_map().const_map_entry().size());
      for (Const_map_entryContext e: cc.const_map().const_map_entry()) {
        elts.put(getConstant(e.const_value(0)), getConstant(e.const_value(1)));
      }
      cvalue = elts;
    } else if (null != cc.IDENTIFIER()) {
      if ("true".equals(cc.IDENTIFIER().getText())) {
        cvalue = true;
      } else if ("false".equals(cc.IDENTIFIER().getText())) {
        cvalue = false;
      } else {
        String identifier = cc.IDENTIFIER().getText();
        if (!identifier.contains(".")) {
          throw new RuntimeException("Invalid constant identifier, missing '.'.");
        }
        String t = identifier.replaceAll("\\.[^.]*$", "");
        String v = identifier.substring(t.length() + 1);
        
        if (!(types.get(t) instanceof DynamicEnum)) {
          throw new RuntimeException("Unknown enum '" + t + "'.");
        }
        
        DynamicEnum de = (DynamicEnum) types.get(t);
        
        cvalue = de.valueOf(v);
      }
    }
    
    return cvalue;
  }
  
  private DynamicType getBaseType(String tname) {
    switch (tname) {
      case "bool":
        return new DynamicType(TType.BOOL);
      case "i8":
      case "byte":
        return new DynamicType(TType.BYTE);
      case "i16":
        return new DynamicType(TType.I16);
      case "i32":
        return new DynamicType(TType.I32);
      case "i64":
        return new DynamicType(TType.I64);
      case "double":
        return new DynamicType(TType.DOUBLE);
      case "string":
        return new DynamicType(TType.STRING);
      case "binary":
        return new DynamicType(DynamicTBase.BINARY);
      default:
        return null;
    }
  }
  
  public Map<String,DynamicType> getTypes() {
    return types;
  }
}
