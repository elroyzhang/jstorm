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
package com.tencent.jstorm.daemon.executor.error;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.cluster.IStormClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:30:24 AM Mar 15, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.executor#throttled-report-error-fn")
public class ThrottledReportErrorFn implements ITaskReportErr {
  private static Logger LOG =
      LoggerFactory.getLogger(ThrottledReportErrorFn.class);
  private IStormClusterState stormClusterState;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private String stormId;
  private String componentId;
  private int errorIntervalSecs;
  private int maxPerInterval;
  private AtomicInteger intervalErrors = new AtomicInteger(0);
  private String host = "unknown";
  private int port;

  @SuppressWarnings("unchecked")
  public ThrottledReportErrorFn(ExecutorData executorData)
      throws UnknownHostException {
    this.stormClusterState = executorData.getStormClusterState();

    this.stormConf = executorData.getStormConf();
    this.errorIntervalSecs = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS), 10);
    this.maxPerInterval = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL), 5);
    this.stormId = executorData.getStormId();
    this.componentId = executorData.getComponentId();
    this.host = Utils.hostname(stormConf);
    this.port = executorData.getWorkerContext().getThisWorkerPort();

    Map<Integer, Integer> virualToRealPort =
        (Map<Integer, Integer>) stormConf.get("storm.virtual.real.ports");
    if (virualToRealPort != null && virualToRealPort.containsKey(port)) {
      LOG.info("Use {} ---> {}", port, virualToRealPort.get(port));
      port = virualToRealPort.get(port);
    }
  }

  @Override
  public void report(Throwable error) {
    String stormZookeeperRoot =
        Utils.getString(stormConf.get(Config.STORM_ZOOKEEPER_ROOT), "/jstorm");
    LOG.error("Report error to " + stormZookeeperRoot + "/errors/" + stormId
        + "/" + componentId + " from " + host + ":" + port + "\n"
        + CoreUtil.stringifyError(error));
    int intervalStartTime = Time.currentTimeSecs();

    if (Time.deltaSecs(intervalStartTime) > errorIntervalSecs) {
      intervalErrors.set(0);
      intervalStartTime = Time.currentTimeSecs();
    }

    if (intervalErrors.incrementAndGet() <= maxPerInterval) {
      try {
        stormClusterState.reportError(stormId, componentId, host,
            Long.valueOf(port), error);
      } catch (Exception e) {
        LOG.error("Failed update error to " + stormZookeeperRoot + "/errors/"
            + stormId + "/" + componentId + "\n" + CoreUtil.stringifyError(e));
      }
    }
  }
}
