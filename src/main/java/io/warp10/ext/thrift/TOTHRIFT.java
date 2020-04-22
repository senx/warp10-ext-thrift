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
