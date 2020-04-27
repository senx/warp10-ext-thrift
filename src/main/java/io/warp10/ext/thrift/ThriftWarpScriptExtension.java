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

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class ThriftWarpScriptExtension extends WarpScriptExtension {
  private static Map<String,Object> functions;

  public static final String THRIFTC = "THRIFTC";
  
  static {
    functions = new HashMap<String, Object>();
    
    functions.put(THRIFTC, new THRIFTC(THRIFTC));
    functions.put("THRIFT->", new THRIFTTO("THRIFT->"));
    functions.put("->THRIFT", new TOTHRIFT("->THRIFT"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
