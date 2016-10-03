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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import org.apache.storm.Config;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.drpc#service-handler#clear-thread ")
public class ClearThreadCallback extends RunnableCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClearThreadCallback.class);

  private static final long serialVersionUID = 1L;

  @ClojureClass(className = "backtype.storm.daemon.drpc#timeout-check-secs")
  private static final long TIMEOUT_CHECK_SECS = 5;
  private int drpcRequestIimeOut;
  private ConcurrentHashMap<String, Semaphore> idtoSem;
  private ConcurrentHashMap<String, Integer> idtoStart;
  private ConcurrentHashMap<String, Object> idtoResult;
  private ConcurrentHashMap<String, String> idtoFunction;
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues;
  private ConcurrentHashMap<String, DRPCRequest> idtoRequest;

  @SuppressWarnings("rawtypes")
  public ClearThreadCallback(Map conf,
      ConcurrentHashMap<String, Integer> idtoStart,
      ConcurrentHashMap<String, Semaphore> idtoSem,
      ConcurrentHashMap<String, Object> idtoResult,
      ConcurrentHashMap<String, String> idtoFunction,
      ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues,
      ConcurrentHashMap<String, DRPCRequest> idtoRequest) {
    this.idtoSem = idtoSem;
    this.idtoStart = idtoStart;
    this.idtoResult = idtoResult;
    this.idtoFunction = idtoFunction;
    this.requestQueues = requestQueues;
    this.idtoRequest = idtoRequest;
    this.drpcRequestIimeOut =
        Utils.getInt(conf.get(Config.DRPC_REQUEST_TIMEOUT_SECS), 600);
  }

  @Override
  public Long call() throws Exception {
    for (Entry<String, Integer> e : idtoStart.entrySet()) {
      String id = e.getKey();
      Integer start = e.getValue();
      if (Time.deltaSecs(start) > drpcRequestIimeOut) {
        DrpcUtils.acquireQueue(requestQueues, idtoFunction.get(id))
            .remove(idtoRequest.get(id));
        LOG.warn("Timeout DRPC request id: {} start at {}", id, start);
        Semaphore sem = idtoSem.get(id);
        if (sem != null) {
          sem.release();
        }
        cleanup(id);
      }
    }
    return TIMEOUT_CHECK_SECS;
  }

  @ClojureClass(className = "backtype.storm.daemon.drpc#service-handler#cleanup")
  private void cleanup(String id) {
    idtoSem.remove(id);
    idtoResult.remove(id);
    idtoStart.remove(id);
  }
}
