package io.warp10.ext.thrift;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class ThriftWarpScriptExtension extends WarpScriptExtension {
  private static Map<String,Object> functions;

  static {
    functions = new HashMap<String, Object>();
    
    functions.put("THRIFTC", new THRIFTC("THRIFTC"));
    functions.put("THRIFT->", new THRIFTTO("THRIFT->"));
    functions.put("->THRIFT", new TOTHRIFT("->THRIFT"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
