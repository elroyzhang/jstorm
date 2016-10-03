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

import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 3:13:03 PM Feb 26, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-tuples-handler")
public class TransferTuplesThread extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private WorkerData workerData;

  public TransferTuplesThread(WorkerData workerData) {
    this.workerData = workerData;
  }

  @Override
  public Long call() throws Exception {
    if (workerData.getStormActiveAtom().get()) {
      workerData.getTransferQueue()
          .consumeBatchWhenAvailable(new TransferTuplesHandler(workerData));
    } else {
      return 1L;
    }
    return 0L;
  }
}
