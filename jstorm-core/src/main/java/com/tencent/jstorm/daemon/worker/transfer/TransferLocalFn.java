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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.worker.WorkerData;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.DisruptorQueue;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-local-fn")
public class TransferLocalFn extends AbstractIFn {
  private static Logger LOG = LoggerFactory.getLogger(TransferLocalFn.class);

  private WorkerData workerData;
  private Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
  private Map<Integer, Integer> taskToShortExecutor;

  public TransferLocalFn(WorkerData workerData) {
    this.workerData = workerData;
    this.shortExecutorReceiveQueueMap =
        workerData.getShortExecutorReceiveQueueMap();
    this.taskToShortExecutor = workerData.getTaskToShortExecutor();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object invoke(Object tupleBatch) {
    if (tupleBatch == null) {
      return null;
    }
    Map<Integer, List<AddressedTuple>> ret =
        new HashMap<Integer, List<AddressedTuple>>();
    List<AddressedTuple> tuples = (List<AddressedTuple>) tupleBatch;
    for (AddressedTuple e : tuples) {

      Integer key = taskToShortExecutor.get(e.getDest());
      if (key != null) {
        List<AddressedTuple> curr = ret.get(key);
        if (curr == null) {
          curr = new ArrayList<AddressedTuple>();
        }
        curr.add(e);

        ret.put(key, curr);
      } else {
        LOG.warn("Received invalid messages for unknown tasks. Dropping...");
      }
    }
    for (Map.Entry<Integer, List<AddressedTuple>> r : ret.entrySet()) {
      DisruptorQueue q = shortExecutorReceiveQueueMap.get(r.getKey());
      if (q != null) {
        q.publish(r.getValue());
      } else {
        LOG.warn("Received invalid messages for unknown tasks {}. Dropping... ",
            r.getKey());
      }
    }
    // TODO
    return null;
  }

  public WorkerData getWorkerData() {
    return workerData;
  }

  public void setWorkerData(WorkerData workerData) {
    this.workerData = workerData;
  }
}