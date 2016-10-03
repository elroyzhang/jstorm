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
package com.tencent.jstorm.daemon.common;

import java.util.Map;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;

import com.google.common.collect.Lists;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;

@ClojureClass(className = "org.apache.storm.daemon.common")
public class Common {

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "org.apache.storm.daemon.common#worker-context")
  public static WorkerTopologyContext workerContext(WorkerData workerData)
      throws Exception {
    Map stormConf = workerData.getStormConf();
    String topologyId = workerData.getTopologyId();
    String workerId = workerData.getWorkerId();
    StormTopology systemTopology = workerData.getSystemTopology();
    String stormroot = ConfigUtils.supervisorStormDistRoot(stormConf,
        workerData.getTopologyId());
    String codeDir = ConfigUtils.supervisorStormResourcesPath(stormroot);
    String pidDir = ConfigUtils.workerPidsRoot(stormConf, workerId);
    return new WorkerTopologyContext(systemTopology, stormConf,
        workerData.getTasksToComponent(),
        workerData.getComponentToSortedTasks(),
        workerData.getComponentToStreamToFields(), topologyId, codeDir, pidDir,
        workerData.getPort(), Lists.newArrayList(workerData.getTaskids()),
        workerData.getDefaultSharedResources(),
        workerData.getUserSharedResources());
  }
}