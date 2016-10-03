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
package com.tencent.jstorm.daemon.supervisor.workermanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.storm.daemon.supervisor.workermanager.DefaultWorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallismKillWorkerManager extends DefaultWorkerManager {

  private static Logger LOG =
      LoggerFactory.getLogger(ParallismKillWorkerManager.class);

  @Override
  public void shutdownWorkers(String supervisorId, List<String> workerIds,
      Map<String, String> workerThreadPids) {
    if (CollectionUtils.isEmpty(workerIds)) {
      return;
    }
    boolean usePool = false;
    ExecutorService threadPool = null;
    int count = workerIds.size();
    CountDownLatch latch = new CountDownLatch(count);
    if (count >= 10) {
      LOG.warn("Dead or Disallowed Workers count is more than 10!");
      usePool = true;
      threadPool = Executors.newFixedThreadPool(10);
    }

    for (String workerId : workerIds) {
      if (usePool) {
        threadPool.execute(new ShutdownWorkerTask(supervisorId, workerId,
            workerThreadPids, latch));
      } else {
        new Thread(new ShutdownWorkerTask(supervisorId, workerId,
            workerThreadPids, latch)).start();
      }
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (threadPool != null) {
        threadPool.shutdown();
      }
    }
  }

  class ShutdownWorkerTask implements Runnable {
    String workerId;
    CountDownLatch countDownLatch;
    String supervisorId;
    Map<String, String> workerThreadPids;

    ShutdownWorkerTask(String supervisorId, String workerId,
        Map<String, String> workerThreadPids, CountDownLatch latch) {
      this.workerId = workerId;
      this.countDownLatch = latch;
      this.supervisorId = supervisorId;
      this.workerThreadPids = new HashMap<String, String>(workerThreadPids);
    }

    @Override
    public void run() {
      try {
        shutdownWorker(supervisorId, workerId, workerThreadPids);
      } catch (Exception e) {
        String errMsg =
            "Failed to shutdown worker workId:" + workerId + ",supervisorId: "
                + supervisorId + ",workerThreadPids:" + workerThreadPids;
        LOG.error(errMsg, e);
      } finally {
        countDownLatch.countDown();
      }
    }
  }
}
