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
package com.tencent.jstorm.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.metrics.jvm.JvmMetrics;
import com.tencent.jstorm.metrics.util.MetricsBase;
import com.tencent.jstorm.metrics.util.MetricsRegistry;
import com.tencent.jstorm.metrics.util.MetricsTimeVaryingInt;
import com.tencent.jstorm.metrics.util.MetricsTimeVaryingRate;

public class WorkerMetrics implements Updater {
  private static Logger LOG = LoggerFactory.getLogger(WorkerMetrics.class);
  private final MetricsRecord metricsRecord;

  public MetricsRegistry registry = new MetricsRegistry();

  public MetricsTimeVaryingInt task_ids = new MetricsTimeVaryingInt("task_ids",
      registry);
  public MetricsTimeVaryingInt localHeartBeat = new MetricsTimeVaryingInt(
      "local_heart_beat", registry);
  public MetricsTimeVaryingRate tupleSend = new MetricsTimeVaryingRate(
      "tuple_send", registry);

  private WorkerActivtyMBean workerActivityMBean;

  public WorkerMetrics(Integer port) {
    // String sessionId = (String) conf.get(Config.STORM_ID);
    String processName = "worker";
    if (port != null) {
      processName += "." + String.valueOf(port);
    }

    JvmMetrics.init(processName, "sessionid");
    workerActivityMBean = new WorkerActivtyMBean(registry);
    MetricsContext metricsContext = MetricsUtil.getContext(processName);
    metricsRecord = MetricsUtil.createRecord(metricsContext, processName);
    metricsRecord.setTag("processName", processName);
    metricsRecord.setTag("sessionId", "sessionid");
    metricsContext.registerUpdater(this);
    LOG.info("Initializing WorkerMetrics using context object:"
        + metricsContext.getClass().getName());
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (workerActivityMBean != null)
      workerActivityMBean.shutdown();
  }
}
