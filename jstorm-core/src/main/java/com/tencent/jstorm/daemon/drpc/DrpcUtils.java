/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.jstorm.daemon.drpc;

import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;

public class DrpcUtils {
  /**
   * 
   * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
   * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
   * 
   */
  @ClojureClass(className = "backtype.storm.daemon.drpc#acquire-queue ")
  public static ConcurrentLinkedQueue<DRPCRequest> acquireQueue(
      ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> queues,
      String function) {
    if (!queues.containsKey(function)) {
      queues.putIfAbsent(function, new ConcurrentLinkedQueue<DRPCRequest>());
    }
    return queues.get(function);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.drpc#check-authorization")
  public static void checkAuthorization(IAuthorizer aclHandler, Map mapping,
      String operation, ReqContext context) throws AuthorizationException {
    if (aclHandler != null) {
      if (context == null) {
        context = ReqContext.context();
      }

      if (!aclHandler.permit(context, operation, mapping)) {
        Principal principal = context.principal();
        String user = "unknown";
        if (principal != null) {
          user = principal.getName();
        }
        throw new AuthorizationException("DRPC request '" + operation
            + "' for '" + user + "' user is not authorized");
      }
      ;
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.drpc#check-authorization")
  public static void checkAuthorization(IAuthorizer aclHandler, Map mapping,
      String operation) throws AuthorizationException {
    checkAuthorization(aclHandler, mapping, operation, ReqContext.context());
  }
}
