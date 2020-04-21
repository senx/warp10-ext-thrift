package io.warp10.ext.thrift;

import io.warp10.ext.thrift.antlr.ThriftLexer;
import io.warp10.ext.thrift.antlr.ThriftParser;
import io.warp10.ext.thrift.antlr.ThriftParser.DocumentContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class THRIFTC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public THRIFTC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    Map<Object,Object> knownTypes = null;
    
    if (top instanceof Map) {
      knownTypes = (Map<Object,Object>) top;
      top = stack.pop();
    }
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a STRING.");
    }
    
    ByteArrayInputStream in = new ByteArrayInputStream(top.toString().getBytes(StandardCharsets.UTF_8));
    
    try {
      CharStream input = CharStreams.fromStream(in);
      ThriftLexer lexer = new ThriftLexer(input);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      ThriftParser parser = new ThriftParser(tokens);
      
      DocumentContext context = parser.document();

      DynamicTBaseGenerator generator = new DynamicTBaseGenerator(knownTypes);
      
      generator.generate(context);
      
      stack.push(generator.getTypes());
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() +" error compiling Thrift IDL.", ioe);
    }

    return stack;
  }

}
