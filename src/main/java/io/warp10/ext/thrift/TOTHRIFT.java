package io.warp10.ext.thrift;

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
    return stack;
  }

}