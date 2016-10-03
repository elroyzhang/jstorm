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
package com.tencent.jstorm.daemon.nimbus.threads;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.nimbus#service-handler#fn#monitor")
public class MonitorRunnable implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(MonitorRunnable.class);
  private NimbusData data;

  @SuppressWarnings("rawtypes")
  private Map conf;
  private Boolean isNimbusReassign;

  public MonitorRunnable(NimbusData data) throws Exception {
    this.data = data;
    this.conf = data.getConf();
    this.isNimbusReassign =
        Utils.getBoolean(conf.get(ConfigUtils.NIMBUS_DO_NOT_REASSIGN), false);
  }

  @Override
  public void run() {
    try {
      if (!isNimbusReassign) {
        synchronized (data.getSubmitLock()) {
          NimbusUtils.mkAssignments(data, null);
        }
      }
    } catch (Exception e) {
      LOG.info("Error while calling mkAssignments {}",
          CoreUtil.stringifyError(e));
    }
    try {
      NimbusUtils.doCleanup(data);
    } catch (Exception e) {
      LOG.info("Error while doCleanup for {}", CoreUtil.stringifyError(e));
    }
  }
}
