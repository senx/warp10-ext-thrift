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

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOTHRIFT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOTHRIFT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof DynamicTBase)) {
      throw new WarpScriptException(getName() + " expects a Thrift structure descriptor.");
    }
    
    DynamicTBase tbase = ((DynamicTBase) top).clone();
    
    top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " operates on a MAP.");
    }
    
    Map<Object,Object> map = (Map<Object,Object>) top;
    
    // Set the structure of tbase to be map
    tbase.setStruct(map);
    
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      byte[] bytes = serializer.serialize(tbase);
      stack.push(bytes);      
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error during serialization attempt.", e);
    }
    
    return stack;
  }
}
