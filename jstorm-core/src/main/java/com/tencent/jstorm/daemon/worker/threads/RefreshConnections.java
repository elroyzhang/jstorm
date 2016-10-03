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
package com.tencent.jstorm.daemon.worker.threads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.daemon.worker.WorkerUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:18:54 AM Mar 15, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.worker#mk-refresh-connections")
public class RefreshConnections extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(RefreshConnections.class);
  private WorkerData workerData;
  private IStormClusterState stormClusterState;
  private String stormId;
  private Set<Integer> outboundTasks;
  private ConcurrentHashMap<String, IConnection> cachedNodePortToSocket;
  private IContext context;
  private ReentrantReadWriteLock.WriteLock endpointSocketWriteLock;

  public RefreshConnections(WorkerData workerData) throws Exception {
    this.workerData = workerData;
    this.stormClusterState = workerData.getStormClusterState();
    this.stormId = workerData.getTopologyId();
    this.outboundTasks = WorkerUtils.workerOutboundTasks(workerData);
    this.cachedNodePortToSocket = workerData.getCachedNodeportToSocket();
    this.context = workerData.getContext();
    this.endpointSocketWriteLock =
        workerData.getEndpointSocketLock().writeLock();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void run() {
    // TODO
    try {
      Assignment assignment = null;
      Integer version = stormClusterState.assignmentVersion(stormId, this);
      if (version == null) {
        return;
      }
      Map map = workerData.getAssignmentVersions().get(stormId);
      Integer oldVersion = null;
      if (map != null) {
        oldVersion = (Integer) map.get(IStateStorage.VERSION);
      }

      if (oldVersion != null && oldVersion.intValue() == version) {
        assignment = (Assignment) map.get(IStateStorage.DATA);
      } else {
        Map thriftifyAssignmentVersion =
            stormClusterState.assignmentInfoWithVersion(stormId, this);
        if (thriftifyAssignmentVersion == null) {
          // Cause by: A reassign topology been killed
          return;
        }
        Assignment newAssignmentData =
            (Assignment) thriftifyAssignmentVersion.get(IStateStorage.DATA);
        Map<String, Object> newAssignment = new HashMap<String, Object>();
        newAssignment.put(IStateStorage.DATA, newAssignmentData);
        newAssignment.put(IStateStorage.VERSION, version);

        workerData.getAssignmentVersions().put(stormId, newAssignment);
        assignment = newAssignmentData;
      }

      Map<List<Long>, NodeInfo> executorInfoToNodePort =
          assignment.get_executor_node_port();

      Map<Integer, NodeInfo> taskToNodePort =
          StormCommon.taskToNodeport(executorInfoToNodePort);

      ConcurrentHashMap<Integer, String> myAssignment =
          new ConcurrentHashMap<Integer, String>();
      Map<Integer, String> neededAssignment =
          new ConcurrentHashMap<Integer, String>();
      for (Integer taskId : outboundTasks) {
        if (taskToNodePort.containsKey(taskId)) {
          String nodeInfoStr =
              WorkerUtils.endpointToString(taskToNodePort.get(taskId));
          myAssignment.put(taskId, nodeInfoStr);
          neededAssignment.put(taskId, nodeInfoStr);
        }
      }

      // we dont need a connection for the local tasks anymore
      Set<Integer> taskIds = workerData.getTaskids();
      for (Integer taskId : taskIds) {
        neededAssignment.remove(taskId);
      }

      Set<String> neededConnections = new HashSet<String>();
      for (String ws : neededAssignment.values()) {
        neededConnections.add(ws);
      }

      Set<Integer> neededTasks = neededAssignment.keySet();

      Set<String> currentConnections = cachedNodePortToSocket.keySet();
      Set<String> newConnections =
          CoreUtil.set_difference(neededConnections, currentConnections);

      Set<String> removeConnections =
          CoreUtil.set_difference(currentConnections, neededConnections);

      Map<String, String> nodeToHost = assignment.get_node_host();

      for (String nodePort : newConnections) {
        NodeInfo endpoint = WorkerUtils.stringToEndpoint(nodePort);
        String host = nodeToHost.get(endpoint.get_node());
        int port = endpoint.get_port_iterator().next().intValue();
        IConnection conn = context.connect(stormId, host, port);
        cachedNodePortToSocket.put(nodePort, conn);
        LOG.info("Add connection to host:{}, port:{} ", host, port);
      }

      endpointSocketWriteLock.lock();
      try {
        workerData.setCachedTaskToNodeport(myAssignment);
      } finally {
        endpointSocketWriteLock.unlock();
      }

      for (String nodeInfo : removeConnections) {
        NodeInfo endpoint = WorkerUtils.stringToEndpoint(nodeInfo);
        String host = nodeToHost.get(endpoint.get_node());
        if (host == null) {
          host = endpoint.get_node();
        }
        int port = endpoint.get_port_iterator().next().intValue();
        IConnection removeConnection = cachedNodePortToSocket.get(nodeInfo);
        if (removeConnection != null) {
          removeConnection.close();
          LOG.info("Closed Connection to host:{}, port:{} ", host, port);
        }
        cachedNodePortToSocket.remove(nodeInfo);
        LOG.info("Removed connection to host:{}, port:{} ", host, port);
      }
      workerData.setCachedNodeportToSocket(cachedNodePortToSocket);

      Set<Integer> missingTasks = new HashSet<Integer>();
      for (Integer taskId : neededTasks) {
        if (!myAssignment.containsKey(taskId)) {
          missingTasks.add(taskId);
        }
      }
      if (!missingTasks.isEmpty()) {
        LOG.warn("Missing assignment for following tasks: "
            + missingTasks.toString());
      }

    } catch (Exception e) {
      LOG.error(
          "Failed to refresh worker Connection" + CoreUtil.stringifyError(e));
      throw new RuntimeException(e);
    }
  }
}
