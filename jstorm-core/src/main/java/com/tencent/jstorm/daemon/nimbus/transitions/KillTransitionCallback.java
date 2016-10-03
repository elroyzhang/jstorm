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
import com.tencent.jstorm.utils.thread.BaseCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.nimbus#kill-transition")
public class KillTransitionCallback extends BaseCallback {
  private static Logger LOG =
      LoggerFactory.getLogger(KillTransitionCallback.class);
  public static final int DEFAULT_DELAY_SECONDS = 5;
  private NimbusData nimbusData;
  private String stormId;

  public KillTransitionCallback(NimbusData nimbusData, String stormId) {
    this.nimbusData = nimbusData;
    this.stormId = stormId;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public <T> Object execute(T... args) {
    Integer delaySecs = null;
    if (args == null || args.length == 0 || args[0] == null) {
      try {
        Map stormConf = NimbusUtils.readStormConf(nimbusData.getConf(), stormId,
            nimbusData.getBlobStore());
        delaySecs = Utils
            .getInt(stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
      } catch (Exception e) {
        LOG.info("Failed to get topology configuration " + stormId);
        delaySecs = KillTransitionCallback.DEFAULT_DELAY_SECONDS;
      }
    } else {
      delaySecs = Integer.valueOf(String.valueOf(args[0]));
    }

    if (delaySecs == null || delaySecs < 0) {
      delaySecs = KillTransitionCallback.DEFAULT_DELAY_SECONDS;
    }

    DelayEvent delayEvent =
        new DelayEvent(nimbusData, stormId, delaySecs, TopologyStatus.REMOVE);
    delayEvent.execute();

    StormBase stormBase = new StormBase();
    stormBase.set_status(TopologyStatus.KILLED);
    TopologyActionOptions topologyActionOptions = new TopologyActionOptions();
    KillOptions killOptions = new KillOptions();
    killOptions.set_wait_secs(delaySecs);
    topologyActionOptions.set_kill_options(killOptions);
    stormBase.set_topology_action_options(topologyActionOptions);
    return stormBase;
  }
}
