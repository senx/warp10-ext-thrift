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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import io.warp10.ext.thrift.antlr.ThriftLexer;
import io.warp10.ext.thrift.antlr.ThriftParser;
import io.warp10.ext.thrift.antlr.ThriftParser.DocumentContext;
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
