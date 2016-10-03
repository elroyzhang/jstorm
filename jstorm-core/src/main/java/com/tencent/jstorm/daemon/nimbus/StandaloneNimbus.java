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
package com.tencent.jstorm.daemon.nimbus;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;

@ClojureClass(className = "backtype.storm.daemon.nimbus#standalone-nimbus")
public class StandaloneNimbus implements INimbus {

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, String localDir) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<WorkerSlot> allSlotsAvailableForScheduling(
      Collection<SupervisorDetails> supervisors, Topologies topologies,
      Set<String> topologiesMissingAssignments) {
    Collection<WorkerSlot> result = new HashSet<WorkerSlot>();
    for (SupervisorDetails s : supervisors) {
      List<Number> ports = (List<Number>) s.getMeta();
      for (Number p : ports) {
        result.add(new WorkerSlot(s.getId(), p));
      }
    }
    return result;
  }

  @Override
  public void assignSlots(Topologies topologies,
      Map<String, Collection<WorkerSlot>> slots) {
  }

  @Override
  public String getHostName(Map<String, SupervisorDetails> supervisors,
      String nodeId) {
    if (supervisors != null && supervisors.get(nodeId) != null) {
      SupervisorDetails supervisor = supervisors.get(nodeId);
      return supervisor.getHost();
    }
    return null;
  }

  @Override
  public IScheduler getForcedScheduler() {
    return null;
  }
}
