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
package com.tencent.jstorm.daemon.executor.spout;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "backtype.storm.daemon.executor#ack-spout-msg")
public class AckSpoutMsg implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(AckSpoutMsg.class);

  private ISpout spout;
  private Object msgId;
  private Integer taskId;
  private String stream;
  private Long timeDelta;
  private Long id;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private ExecutorData executorData;
  private TaskData taskData;
  private Boolean isDebug = false;

  public AckSpoutMsg(ExecutorData executorData, TaskData taskData, Object msgId,
      TupleInfo tupleInfo, Long timeDelta, Long id) {
    this.executorData = executorData;
    this.taskData = taskData;
    this.spout = (ISpout) taskData.getObject();
    this.msgId = msgId;
    this.taskId = taskData.getTaskId();
    this.timeDelta = timeDelta;
    this.id = id;
    this.stormConf = executorData.getStormConf();
    this.stream = tupleInfo.getStream();
    this.isDebug =
        Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
  }

  public void run() {
    if (isDebug) {
      LOG.info("SPOUT Acking message id:{} msgId:{}", id, msgId);
    }

    spout.ack(msgId);

    TaskUtils.applyHooks(taskData.getUserContext(),
        new SpoutAckInfo(msgId, taskId, timeDelta));
    // ack与complete-latencies拆分计算，屏蔽此方法，此处只计算ack数
    try {
      if (executorData.getAckSampler().call()) {
        ((SpoutExecutorStats) executorData.getStats()).spoutAckedTuple(stream,
            timeDelta);
      }
    } catch (Exception e) {
      LOG.error("AckSampler error {}", CoreUtil.stringifyError(e));
    }
  }
}
