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
package com.tencent.jstorm.daemon.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.InsufficientCapacityException;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 4:33:42 PM Feb 18, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-executor-transfer-fn")
public class ExecutorTransferFn {

  private static final Logger LOG =
      LoggerFactory.getLogger(ExecutorTransferFn.class);

  private DisruptorQueue batchtransferToWorker;
  private boolean debug;

  public ExecutorTransferFn(DisruptorQueue batchtransferToWorker,
      boolean debug) {
    this.batchtransferToWorker = batchtransferToWorker;
    this.debug = debug;
  }

  public void transfer(Integer outTask, TupleImpl tuple)
      throws InsufficientCapacityException {
    AddressedTuple tuplePair = new AddressedTuple(outTask, tuple);
    if (debug) {
      LOG.info("TRANSFERING tuple " + tuplePair.toString());
    }
    batchtransferToWorker.publish(tuplePair);
  }
}
