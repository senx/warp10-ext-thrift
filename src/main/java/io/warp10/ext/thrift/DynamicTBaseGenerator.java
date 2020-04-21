package io.warp10.ext.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.protocol.TType;

import io.warp10.ext.thrift.antlr.ThriftParser.Base_typeContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_listContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_map_entryContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_ruleContext;
import io.warp10.ext.thrift.antlr.ThriftParser.Const_valueContext;
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
  private DynamicTBase currentStruct = null;
  
  Map<String,DynamicTBase> structs = new HashMap<String,DynamicTBase>();
  Map<String,DynamicEnum> enums = new HashMap<String,DynamicEnum>();
  Map<String,String> typedefs = new HashMap<String,String>();
  
  public void generate(Object context) {
    if (null == context) {
      return;
    }
    
    System.out.println("CTX=" + context.getClass());
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
      
      currentStruct = new DynamicTBase(sc.IDENTIFIER().getText());
      
      for (FieldContext fc: sc.field()) {
        generate(fc);
      }
      
      structs.put(currentStruct.getName(), currentStruct);
      currentStruct = null;
      
    } else if (context instanceof FieldContext) {
      FieldContext fc = (FieldContext) context;
            
      String name = fc.IDENTIFIER().getText();
      int tag = Integer.parseInt(fc.field_id().integer().INTEGER().getText());
      
      DynamicField type = getType(fc.field_type());
      
      if (null != fc.field_req()) {
        System.out.println("req=" + fc.field_req().getText());
      }
      System.out.println("name=" + fc.IDENTIFIER());
      if (null != fc.const_value()) {
        Const_valueContext cc = fc.const_value();
        Object cvalue = getConstant(cc);
        
        System.out.println("CONST=" + cvalue);
      }
    } else if (context instanceof TypedefContext) {
      TypedefContext tc = (TypedefContext) context;
      String name = tc.IDENTIFIER().getText();
      Field_typeContext fc = tc.field_type();
      System.out.println("BASE TYPE=" + fc.IDENTIFIER());
      System.out.println("CONT TYPE=" + fc.container_type());
      String type = null != fc.base_type() ? fc.base_type().getText() : fc.IDENTIFIER().getText();
      if (structs.containsKey(name) || enums.containsKey(name) || typedefs.containsKey(name)) {
        throw new RuntimeException("Alias '" + name + "' already in use.");
      }
      typedefs.put(name, type);
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
      enums.put(currentEnum.getName(), currentEnum);
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
      System.out.println(ec.IDENTIFIER() + " >> " + ordinal);
      currentEnum.add(name, ordinal);
    }
  }
  
  private DynamicField getType(Field_typeContext type) {
    if (null != type.base_type()) {
      Base_typeContext bc = (Base_typeContext) type.base_type();
      
      Type_annotationsContext annotations = bc.type_annotations();
      System.out.println(bc.getText());
      if (null != annotations) {
        for (Type_annotationContext annotation: annotations.type_annotation()) {
          System.out.println("ANNOTATION >> " + annotation.IDENTIFIER().getText());
        }
      }
      
      String tname = bc.getText();
      
      switch (tname) {
        case "bool":
          return new DynamicField(TType.BOOL);
        case "i8":
        case "byte":
          return new DynamicField(TType.BYTE);
        case "i16":
          return new DynamicField(TType.I16);
        case "i32":
          return new DynamicField(TType.I32);
        case "i64":
          return new DynamicField(TType.I64);
        case "double":
          return new DynamicField(TType.DOUBLE);
        case "string":
          return new DynamicField(TType.STRING);
        case "binary":
          return new DynamicField(DynamicTBase.BINARY);
      }
    } else if (null != type.container_type()) {      
      if (null != type.container_type().list_type()) {      
        DynamicField df = getType(type.container_type().list_type().field_type());
        return new DynamicField(TType.SET, df);
      } else if (null != type.container_type().map_type()) {
        DynamicField keydf = getType(type.container_type().map_type().field_type(0));
        DynamicField valuedf = getType(type.container_type().map_type().field_type(1));
        return new DynamicField(keydf, valuedf);
      } else if (null != type.container_type().set_type()) {
        DynamicField df = getType(type.container_type().set_type().field_type());
        return new DynamicField(TType.SET, df);
      }
    } else {
      // Check that the type is known in either enums or structs
      Object t = resolve(type.IDENTIFIER().getText());
      if (null == t) {
        throw new RuntimeException("Unknown type '" + type.IDENTIFIER().getText() + "'.");
      }
      
      
      return null;
    }
    
    return null;
  }
  
  private Object resolve(String name) {
    String oname = name;
    
    while(typedefs.containsKey(name)) {
      name = typedefs.get(name);
    }
    
    if (enums.containsKey(name)) {
      return enums.get(name);
    } else if (structs.containsKey(name)) {
      return structs.get(name);
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
      System.out.println(cc.LITERAL());
      cvalue = cc.LITERAL().getText();
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
      
    }
    
    return cvalue;
  }
}
