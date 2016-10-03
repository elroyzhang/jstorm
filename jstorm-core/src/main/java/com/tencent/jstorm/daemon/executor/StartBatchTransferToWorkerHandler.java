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

import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.DisruptorQueue;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 3:24:04 PM Feb 25, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#start-batch-transfer->worker-handler!")
public class StartBatchTransferToWorkerHandler extends RunnableCallback {
  private static final long serialVersionUID = 1L;

  private DisruptorQueue batchTransferQueue;
  private ExecutorData executorData;
  private BatchTransferToWorkerHandler batchTransferToWorkerHandler;

  public StartBatchTransferToWorkerHandler(WorkerData worker,
      ExecutorData executorData) {
    this.executorData = executorData;
    this.batchTransferQueue = executorData.getBatchTransferQueue();
    this.batchTransferToWorkerHandler =
        new BatchTransferToWorkerHandler(worker, executorData);
  }

  @Override
  public Long call() throws Exception {
    if (executorData.getWorker().getStormActiveAtom().get()) {
      batchTransferQueue
          .consumeBatchWhenAvailable(batchTransferToWorkerHandler);
    } else {
      return 1L;
    }
    return 0L;
  }

  @Override
  public Object getResult() {
    return 0;
  }
}
