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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.thread.Callback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyStatus;

@ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions")
public class StateTransitions {
  private final static Logger LOG =
      LoggerFactory.getLogger(StateTransitions.class);
  private final List<TopologyStatus> SYSTEM_EVENTS =
      Arrays.asList(TopologyStatus.START_UP);
  private NimbusData data;

  public StateTransitions(NimbusData data) {
    this.data = data;

  }

  @SuppressWarnings("unchecked")
  public <T> void transition(String stormId, TopologyStatus changeStatus,
      T... args) throws Exception {
    transition(stormId, false, changeStatus, args);
  }

  @SuppressWarnings("unchecked")
  public <T> void transition(String stormId, boolean errorOnNoTransition,
      TopologyStatus changeTopologyStatus, T... args) throws Exception {
    NimbusUtils.isLeader(this.data, true);
    // lock outside

    synchronized (this.data.getSubmitLock()) {
      transitionLock(stormId, errorOnNoTransition, changeTopologyStatus, args);
    }
  }

  private StormBase topologyStatus(String stormId) throws Exception {
    StormBase stormbase = data.getStormClusterState().stormBase(stormId, null);
    if (stormbase == null) {
      return null;
    } else {
      return stormbase;
    }
  }

  @SuppressWarnings("unchecked")
  public <T> void transitionLock(String stormId, boolean errorOnNoTransition,
      TopologyStatus changeTopologyStatus, T... args) throws Exception {
    StormBase stormBase = topologyStatus(stormId);
    // handles the case where event was scheduled but topology has been
    // removed
    if (stormBase == null) {
      LOG.info("Cannot apply event {} to {} because topology no longer exists",
          changeTopologyStatus, stormId);
      return;
    }
    TopologyStatus currentTopologyStatus = stormBase.get_status();

    Map<TopologyStatus, Map<TopologyStatus, Callback>> callbackMap =
        stateTransitions(stormId, currentTopologyStatus, stormBase);

    // get current changingCallbacks
    Map<TopologyStatus, Callback> changingCallbacks =
        callbackMap.get(currentTopologyStatus);

    if (changingCallbacks == null
        || changingCallbacks.containsKey(changeTopologyStatus) == false
        || changingCallbacks.get(changeTopologyStatus) == null) {
      String msg = "No transition for event: changing status "
          + changeTopologyStatus.name() + ", current status: "
          + currentTopologyStatus + " storm-id: " + stormId;
      LOG.info(msg);
      if (errorOnNoTransition) {
        throw new RuntimeException(msg);
      } else if (!SYSTEM_EVENTS.contains(changeTopologyStatus)) {
        LOG.info(msg);
      }
      return;
    }

    Callback callback = changingCallbacks.get(changeTopologyStatus);

    Object obj = callback.execute(args);
    if (obj != null && obj instanceof StormBase) {
      this.data.getStormClusterState().updateStorm(stormId, (StormBase) obj);
      LOG.info("transition storm-id: " + stormId + " changing status: "
          + currentTopologyStatus + " to " + ((StormBase) obj).toString());
    }
    return;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions")
  private Map<TopologyStatus, Map<TopologyStatus, Callback>> stateTransitions(
      String stormId, TopologyStatus status, StormBase stormBase) {

    Map<TopologyStatus, Map<TopologyStatus, Callback>> rtn =
        new HashMap<TopologyStatus, Map<TopologyStatus, Callback>>();

    // current status type is active
    Map<TopologyStatus, Callback> activeMap =
        new HashMap<TopologyStatus, Callback>();
    activeMap.put(TopologyStatus.INACTIVATE, new InactiveTransitionCallback());
    activeMap.put(TopologyStatus.ACTIVE, null);
    activeMap.put(TopologyStatus.REBALANCE,
        new RebalanceTransitionCallback(data, stormId, status));
    activeMap.put(TopologyStatus.KILL,
        new KillTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.ACTIVE, activeMap);

    // current status type is inactive
    Map<TopologyStatus, Callback> inactiveMap =
        new HashMap<TopologyStatus, Callback>();
    inactiveMap.put(TopologyStatus.ACTIVATE, new ActiveTransitionCallback());
    inactiveMap.put(TopologyStatus.INACTIVATE, null);
    inactiveMap.put(TopologyStatus.REBALANCE,
        new RebalanceTransitionCallback(data, stormId, status));
    inactiveMap.put(TopologyStatus.KILL,
        new KillTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.INACTIVE, inactiveMap);

    // current status type is killed
    Map<TopologyStatus, Callback> killedMap =
        new HashMap<TopologyStatus, Callback>();

    int killDelaySecs = 3;
    int rebalanceDelaySecs = 3;

    TopologyActionOptions topologyActionOptions =
        stormBase.get_topology_action_options();
    if (topologyActionOptions != null) {
      if (topologyActionOptions.is_set_kill_options()) {
        KillOptions killOptions = topologyActionOptions.get_kill_options();
        if (killOptions != null) {
          killDelaySecs = killOptions.get_wait_secs();
        }
      }

      if (topologyActionOptions.is_set_rebalance_options()) {
        RebalanceOptions rebalanceOptions =
            topologyActionOptions.get_rebalance_options();
        if (rebalanceOptions != null) {
          rebalanceDelaySecs = rebalanceOptions.get_wait_secs();
        }
      }
    }

    killedMap.put(TopologyStatus.START_UP,
        new DelayEvent(data, stormId, killDelaySecs, TopologyStatus.REMOVE));
    killedMap.put(TopologyStatus.KILL,
        new KillTransitionCallback(data, stormId));
    killedMap.put(TopologyStatus.REMOVE,
        new RemoveTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.KILLED, killedMap);

    // current status type is rebalancing
    Map<TopologyStatus, Callback> rebalancingMap =
        new HashMap<TopologyStatus, Callback>();
    rebalancingMap.put(TopologyStatus.START_UP, new DelayEvent(data, stormId,
        rebalanceDelaySecs, TopologyStatus.DO_REBALANCE));
    rebalancingMap.put(TopologyStatus.KILL,
        new KillTransitionCallback(data, stormId));
    rebalancingMap.put(TopologyStatus.DO_REBALANCE,
        new DoRebalanceTransitionCallback(data, stormId, status, stormBase));
    rtn.put(TopologyStatus.REBALANCING, rebalancingMap);

    return rtn;
  }
}
