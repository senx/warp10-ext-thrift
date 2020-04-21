package io.warp10.ext.thrift;

import org.apache.thrift.TBase;
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
    
    if (!(top instanceof DynamicTBase)) {
      throw new WarpScriptException(getName() + " expects a compiled Thrift structure.");
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
      throw new WarpScriptException(getName() + " encountered an error while deserializing.");
    }
    
    stack.push(tbase.getStruct());
    
    return stack;
  }

}
