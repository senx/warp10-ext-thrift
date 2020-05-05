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

import java.util.Map;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class THRIFTTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public THRIFTTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof DynamicTBase) && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a compiled Thrift structure (" + ThriftWarpScriptExtension.TBASE + ") or the name of one.");
    }

    if (top instanceof String) {
      String name = (String) top;
      top = stack.pop();
      
      if (!(top instanceof Map)) {
        throw new WarpScriptException(getName() + " expects a map of names to compiled Thrift structures (" + ThriftWarpScriptExtension.TBASE + ").");
      }
      
      top = ((Map<Object,Object>) top).get(name);
      if (!(top instanceof DynamicTBase)) {
        throw new WarpScriptException(getName() + " expected '" + name + "' to be associated with a compiled Thrift structure (" + ThriftWarpScriptExtension.TBASE + ").");
      }
    }
    
    // We clone the DynamicTBase so we can safely deserialize
    DynamicTBase tbase = ((DynamicTBase) top).clone();
    
    top = stack.pop();
    
    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }
    
    byte[] data = (byte[]) top;
    
    TProtocolFactory factory = TProtocolUtil.guessProtocolFactory(data, new TCompactProtocol.Factory());
    
    TDeserializer deser = new TDeserializer(factory);
    
    try {
      deser.deserialize(tbase, data);
    } catch (TException te) {
      throw new WarpScriptException(getName() + " encountered an error while deserializing.", te);
    }
    
    stack.push(tbase.getStruct());
    
    return stack;
  }

}
