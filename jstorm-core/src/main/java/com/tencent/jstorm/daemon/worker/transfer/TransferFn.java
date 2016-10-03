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
package com.tencent.jstorm.daemon.worker.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.Config;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 1:52:04 PM Feb 26, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn")
public class TransferFn extends AbstractIFn {
  private static Logger LOG = LoggerFactory.getLogger(TransferFn.class);
  private Set<Integer> localTasks;
  private TransferLocalFn localTransfer;
  private DisruptorQueue transferQueue;
  private boolean trySerializeLocal;
  private WorkerData workerData;

  public TransferFn(WorkerData workerData) {
    this.localTasks = workerData.getTaskids();
    this.localTransfer = new TransferLocalFn(workerData);
    this.transferQueue = workerData.getTransferQueue();
    this.trySerializeLocal = Utils.getBoolean(workerData.getStormConf()
        .get(Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE), false);
    this.workerData = workerData;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn")
  public void transfer(KryoTupleSerializer serializer,
      ArrayList<AddressedTuple> tupleBatch) {
    if (trySerializeLocal) {
      LOG.warn(
          "WILL TRY TO SERIALIZE ALL TUPLES (Turn off TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE for production)");
      trySerializeLocalTransferFn(serializer, tupleBatch);
    } else {
      transferFn(serializer, tupleBatch);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn#fn")
  public void transferFn(KryoTupleSerializer serializer,
      ArrayList<AddressedTuple> tupleBatch) {
    if (tupleBatch == null) {
      return;
    }
    List<AddressedTuple> local = new ArrayList<AddressedTuple>();
    HashMap<Integer, ArrayList<TaskMessage>> remoteMap =
        new HashMap<Integer, ArrayList<TaskMessage>>();
    for (AddressedTuple pair : tupleBatch) {
      Integer task = pair.getDest();
      if (localTasks.contains(task)) {
        local.add(pair);
      } else {
        // Using java objects directly to avoid performance issues in java code
        ConcurrentHashMap<Integer, String> taskToNodePort =
            workerData.getCachedTaskToNodeport();
        String nodePort = taskToNodePort.get(task);
        // if nodePort is null, dropped messages
        if (null != nodePort) {
          if (!remoteMap.containsKey(task)) {
            remoteMap.put(task, new ArrayList<TaskMessage>());
          }
          ArrayList<TaskMessage> remote = remoteMap.get(task);
          try {
            TaskMessage message = new TaskMessage(task,
                serializer.serialize((TupleImpl) pair.getTuple()));
            remote.add(message);
          } catch (Throwable t) {
            LOG.error(
                "Serialize tuple failed for " + CoreUtil.stringifyError(t));
          }
        }
      }
    }

    if (local.size() > 0) {
      localTransfer.invoke(local);
    }
    if (remoteMap.size() > 0) {
      transferQueue.publish(remoteMap);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn#try-serialize-local#fn")
  public void trySerializeLocalTransferFn(KryoTupleSerializer serializer,
      ArrayList<AddressedTuple> tupleBatch) {
    assertCanSerialize(serializer, tupleBatch);
    transferFn(serializer, tupleBatch);
  }

  /**
   * Check that all of the tuples can be serialized by serializing them.
   * 
   * @param serializer
   * @param tupleBatch
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#assert-can-serialize")
  private void assertCanSerialize(KryoTupleSerializer serializer,
      ArrayList<AddressedTuple> tupleBatch) {
    for (AddressedTuple pair : tupleBatch) {
      Tuple tuple = pair.getTuple();
      serializer.serialize(tuple);
    }
  }
}