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
package org.apache.storm.cluster;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.TopologyBackpressure;
import org.apache.storm.generated.WorkerJvmInfo;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;

import com.tencent.jstorm.daemon.drpc.DRPCServerInfo;

public interface IStormClusterState {
    public List<String> assignments(Runnable callback);

    public Assignment assignmentInfo(String stormId, Runnable callback);

    public Map assignmentInfoWithVersion(String stormId, Runnable callback);

    public Integer assignmentVersion(String stormId, Runnable callback) throws Exception;

    public List<String> blobstoreInfo(String blobKey);

    public List nimbuses();

    public void addNimbusHost(String nimbusId, NimbusSummary nimbusSummary, ILeaderElector leaderElector);

    public List<String> activeStorms();

    public StormBase stormBase(String stormId, Runnable callback);

    public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port);

    public List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo);

    public List<ProfileRequest> getTopologyProfileRequests(String stormId);

    public void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest);

    public void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest);

    public Map<ExecutorInfo, ExecutorBeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort);

    public List<String> supervisors(Runnable callback);

    public SupervisorInfo supervisorInfo(String supervisorId); // returns nil if doesn't exist

    public void setupHeatbeats(String stormId);

    public void teardownHeartbeats(String stormId);

    public void teardownTopologyErrors(String stormId);

    public List<String> heartbeatStorms();

    public List<String> errorTopologies();

    public List<String> backpressureTopologies();

    public void setTopologyLogConfig(String stormId, LogConfig logConfig);

    public LogConfig topologyLogConfig(String stormId, Runnable cb);

    public void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info);

    public void removeWorkerHeartbeat(String stormId, String node, Long port);

    public void supervisorHeartbeat(String supervisorId, SupervisorInfo info);

    public void workerBackpressure(String stormId, String node, Long port, boolean on);

    public boolean topologyBackpressure(String stormId, Runnable callback);

    public void setupBackpressure(String stormId);

    public void removeBackpressure(String stormId);

    public void removeWorkerBackpressure(String stormId, String node, Long port);

    public void activateStorm(String stormId, StormBase stormBase);

    public void updateStorm(String stormId, StormBase newElems);

    public void removeStormBase(String stormId);

    public void setAssignment(String stormId, Assignment info);

    public void setupBlobstore(String key, NimbusInfo nimbusInfo, Integer versionInfo);

    public List<String> activeKeys();

    public List<String> blobstore(Runnable callback);

    public void removeStorm(String stormId);

    public void removeBlobstoreKey(String blobKey);

    public void removeKeyVersion(String blobKey);

    public void reportError(String stormId, String componentId, String node, Long port, Throwable error);

    public List<ErrorInfo> errors(String stormId, String componentId);

    public ErrorInfo lastError(String stormId, String componentId);

    public void setCredentials(String stormId, Credentials creds, Map topoConf) throws NoSuchAlgorithmException;

    public Credentials credentials(String stormId, Runnable callback);

    public void disconnect();

    /**add by jstorm developer -- begin**/
    public void registerUIServerInfo(NodeInfo bs) throws Exception;

    public NodeInfo getUIServerInfo() throws Exception;
    
    public void setWorkerJvmInfo(String host, Long port, WorkerJvmInfo workerJvmInfo);
    public WorkerJvmInfo getWorkerJvmInfo(String host, Long port);
    public List<String> workerJvmNodes();
    public void removeWorkerJvmNode(String host, Long port);
    
    public void addDrpcServer(DRPCServerInfo drpcServerInfo);
    public List<String> drpcServers();

    public void addLogviewer(String logviewerId, NodeInfo realNodePorts);
    public List<String> logviewers(Runnable callback);
    public NodeInfo logviewer(String logviewerId);
    public List<String> nimbusHostports(Runnable callback);
    // add for http://git.code.oa.com/trc/jstorm/issues/73
    public Map<String, List<ErrorInfo>> errorsByStormId(String stormId);
    // add for http://git.code.oa.com/trc/jstorm/issues/73
    public Map<String, ErrorInfo> lastErrorByStormId(String stormId);
    public void updateComponentBackpressure(String stormId, String component);
    public TopologyBackpressure getTopologyBackpressure(String stormId);
    /**add by jstorm developer -- end**/
}
