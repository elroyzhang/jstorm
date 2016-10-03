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
package com.tencent.jstorm.daemon.nimbus.transitions;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.BaseCallback;

import com.tencent.jstorm.ClojureClass;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.nimbus#rebalance-transition")
public class RebalanceTransitionCallback extends BaseCallback {
  private static Logger LOG =
      LoggerFactory.getLogger(RebalanceTransitionCallback.class);
  private NimbusData nimbusData;
  private String stormId;
  private TopologyStatus oldStatus;
  public static final int DEFAULT_DELAY_SECONDS = 60;

  public RebalanceTransitionCallback(NimbusData nimbusData, String stormId,
      TopologyStatus status) {
    this.nimbusData = nimbusData;
    this.stormId = stormId;
    this.oldStatus = status;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public <T> Object execute(T... args) {
    // time num-workers executor-overrides
    Integer delaySecs = null;
    Integer numWorkers = null;
    Map<String, Integer> executorOverrides = null;
    if (args != null && args.length > 0 && args[0] != null) {
      delaySecs = Integer.valueOf(String.valueOf(args[0]));
    }
    if (args != null && args.length > 1 && args[1] != null) {
      if (StringUtils.isNotEmpty(String.valueOf(args[1]))) {
        numWorkers = Integer.valueOf(String.valueOf(args[1]));
      }
    }
    if (args != null && args.length > 2 && args[2] != null) {
      executorOverrides = (Map<String, Integer>) args[2];
    }

    try {
      if (delaySecs == null) {
        Map stormConf = NimbusUtils.readStormConf(nimbusData.getConf(), stormId,
            nimbusData.getBlobStore());
        delaySecs = Utils
            .getInt(stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
      }
      DelayEvent delayEvent = new DelayEvent(nimbusData, stormId, delaySecs,
          TopologyStatus.DO_REBALANCE);
      delayEvent.execute(args);
    } catch (Exception e) {
      LOG.error("Failed Update state while rebalancing:"
          + CoreUtil.stringifyError(e));
    }

    StormBase stormBase = new StormBase();
    stormBase.set_status(TopologyStatus.REBALANCING);
    stormBase.set_prev_status(oldStatus);
    TopologyActionOptions topologyActionOptions = new TopologyActionOptions();
    RebalanceOptions rebalanceOptions = new RebalanceOptions();
    rebalanceOptions.set_wait_secs(delaySecs);
    if (numWorkers != null) {
      rebalanceOptions.set_num_workers(numWorkers);
    }
    if (executorOverrides != null) {
      rebalanceOptions.set_num_executors(executorOverrides);
    }
    topologyActionOptions.set_rebalance_options(rebalanceOptions);
    stormBase.set_topology_action_options(topologyActionOptions);
    return stormBase;
  }
}
