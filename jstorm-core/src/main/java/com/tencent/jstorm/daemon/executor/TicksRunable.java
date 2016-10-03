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

import java.util.ArrayList;
import java.util.Arrays;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Constants;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 2:57:19 PM Feb 29, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#setup-ticks!#fn")
public class TicksRunable implements Runnable {
  private DisruptorQueue receiveQueue;
  private WorkerTopologyContext context;
  private Integer tickTimeSecs;

  public TicksRunable(ExecutorData executorData, Integer tickTimeSecs) {
    this.receiveQueue = executorData.getReceiveQueue();
    this.context = executorData.getWorkerContext();
    this.tickTimeSecs = tickTimeSecs;
  }

  @Override
  public void run() {
    /** added by JStorm developer , add tick timestamp*/
    TupleImpl tupleImpl =
        new TupleImpl(context, Arrays.asList((Object) tickTimeSecs,System.currentTimeMillis()),
            (int)Constants.SYSTEM_TASK_ID, Constants.SYSTEM_TICK_STREAM_ID);
    AddressedTuple tm =
        new AddressedTuple(AddressedTuple.BROADCAST_DEST, tupleImpl);
    ArrayList<AddressedTuple> pairs = new ArrayList<AddressedTuple>();
    pairs.add(tm);
    receiveQueue.publish(pairs);
  }
}
