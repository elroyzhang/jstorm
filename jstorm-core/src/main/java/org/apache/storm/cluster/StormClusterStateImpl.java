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

import clojure.lang.*;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.generated.*;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.drpc.DRPCServerInfo;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class StormClusterStateImpl implements IStormClusterState {

    private static Logger LOG = LoggerFactory.getLogger(StormClusterStateImpl.class);

    private IStateStorage stateStorage;

    private ConcurrentHashMap<String, Runnable> assignmentInfoCallback;
    private ConcurrentHashMap<String, Runnable> assignmentInfoWithVersionCallback;
    private ConcurrentHashMap<String, Runnable> assignmentVersionCallback;
    private AtomicReference<Runnable> supervisorsCallback;
    // we want to reigister a topo directory getChildren callback for all workers of this dir
    private ConcurrentHashMap<String, Runnable> backPressureCallback;
    private AtomicReference<Runnable> assignmentsCallback;
    private ConcurrentHashMap<String, Runnable> stormBaseCallback;
    private AtomicReference<Runnable> blobstoreCallback;
    private ConcurrentHashMap<String, Runnable> credentialsCallback;
    private ConcurrentHashMap<String, Runnable> logConfigCallback;

    /**add by jstorm developer -- begin**/
    private AtomicReference<Runnable> nimbusesCallback;
    private AtomicReference<Runnable> logviewerCallback;
    /**add by jstorm developer -- end**/

    private List<ACL> acls;
    private String stateId;
    private boolean solo;

    /**add by jstorm developer -- begin**/
    private ConnectionStateListener defaultListener;
    /**add by jstorm developer -- end**/

    @ClojureClass(className="add callback for logviewer for docker, add default listener")
    public StormClusterStateImpl(IStateStorage StateStorage, List<ACL> acls, ClusterStateContext context, boolean solo) throws Exception {

        this.stateStorage = StateStorage;
        this.solo = solo;
        this.acls = acls;

        assignmentInfoCallback = new ConcurrentHashMap<>();
        assignmentInfoWithVersionCallback = new ConcurrentHashMap<>();
        assignmentVersionCallback = new ConcurrentHashMap<>();
        supervisorsCallback = new AtomicReference<>();
        backPressureCallback = new ConcurrentHashMap<>();
        assignmentsCallback = new AtomicReference<>();
        stormBaseCallback = new ConcurrentHashMap<>();
        credentialsCallback = new ConcurrentHashMap<>();
        logConfigCallback = new ConcurrentHashMap<>();
        blobstoreCallback = new AtomicReference<>();
        /**add by jstorm developer -- begin**/
        logviewerCallback = new AtomicReference<>();
        nimbusesCallback = new AtomicReference<>();
        /**add by jstorm developer -- end**/

        stateId = this.stateStorage.register(new ZKStateChangedCallback() {

            public void changed(Watcher.Event.EventType type, String path) {
                List<String> toks = Zookeeper.tokenizePath(path);
                int size = toks.size();
                if (size >= 1) {
                    String root = toks.get(0);
                    if (root.equals(ClusterUtils.ASSIGNMENTS_ROOT)) {
                        if (size == 1) {
                            // set null and get the old value
                            issueCallback(assignmentsCallback);
                        } else {
                            issueMapCallback(assignmentInfoCallback, toks.get(1));
                            issueMapCallback(assignmentVersionCallback, toks.get(1));
                            issueMapCallback(assignmentInfoWithVersionCallback, toks.get(1));
                        }

                    } else if (root.equals(ClusterUtils.SUPERVISORS_ROOT)) {
                        issueCallback(supervisorsCallback);
                    } else if (root.equals(ClusterUtils.BLOBSTORE_ROOT)) {
                        issueCallback(blobstoreCallback);
                    } else if (root.equals(ClusterUtils.STORMS_ROOT) && size > 1) {
                        issueMapCallback(stormBaseCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.CREDENTIALS_ROOT) && size > 1) {
                        issueMapCallback(credentialsCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.LOGCONFIG_ROOT) && size > 1) {
                        issueMapCallback(logConfigCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.BACKPRESSURE_ROOT) && size > 1) {
                        issueMapCallback(backPressureCallback, toks.get(1));
                    } 
                    /** add by jstorm developer -- begin */
                    else if (root.equals(ClusterUtils.LOGVIEWER_ROOT)) {
                      issueCallback(logviewerCallback);
                    } else if (root.equals(ClusterUtils.NIMBUSES_ROOT)) {
                      issueCallback(nimbusesCallback);
                    } 
                    /** add by jstorm developer -- end */
                    else {
                        LOG.error("{} Unknown callback for subtree {}", new RuntimeException("Unknown callback for this path"), path);
                        Runtime.getRuntime().exit(30);
                    }

                }

                return;
            }

        });

        String[] pathlist = { ClusterUtils.ASSIGNMENTS_SUBTREE, 
                              ClusterUtils.STORMS_SUBTREE, 
                              ClusterUtils.SUPERVISORS_SUBTREE, 
                              ClusterUtils.WORKERBEATS_SUBTREE,
                              ClusterUtils.ERRORS_SUBTREE, 
                              ClusterUtils.BLOBSTORE_SUBTREE, 
                              ClusterUtils.NIMBUSES_SUBTREE, 
                              ClusterUtils.LOGCONFIG_SUBTREE,
                              ClusterUtils.BACKPRESSURE_SUBTREE };
        for (String path : pathlist) {
            this.stateStorage.mkdirs(path, acls);
        }

        /**add by jstorm developer -- begin**/
        this.defaultListener = new ConnectionStateListener() {
          @Override
          public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
              LOG.info("Connection state listener invoked, zookeeper connection state has changed to {}", connectionState);
              if (connectionState.equals(ConnectionState.LOST)) {
                  Utils.exitProcess(1, "Process exit for Zookeeper connection lost!");
              }
          }
        };
        /**add by jstorm developer -- end**/
    }

    protected void issueCallback(AtomicReference<Runnable> cb) {
        Runnable callback = cb.getAndSet(null);
        if (callback != null)
            callback.run();
    }

    protected void issueMapCallback(ConcurrentHashMap<String, Runnable> callbackConcurrentHashMap, String key) {
        Runnable callback = callbackConcurrentHashMap.remove(key);
        if (callback != null)
            callback.run();
    }

    @Override
    public List<String> assignments(Runnable callback) {
        if (callback != null) {
            assignmentsCallback.set(callback);
        }
        return stateStorage.get_children(ClusterUtils.ASSIGNMENTS_SUBTREE, callback != null);
    }

    @Override
    public Assignment assignmentInfo(String stormId, Runnable callback) {
        if (callback != null) {
            assignmentInfoCallback.put(stormId, callback);
        }
        byte[] serialized = stateStorage.get_data(ClusterUtils.assignmentPath(stormId), callback != null);
        return ClusterUtils.maybeDeserialize(serialized, Assignment.class);
    }

    @Override
    public Map assignmentInfoWithVersion(String stormId, Runnable callback) {
        Map map = new HashMap();
        if (callback != null) {
            assignmentInfoWithVersionCallback.put(stormId, callback);
        }
        Assignment assignment = null;
        Integer version = 0;
        Map dataWithVersionMap = stateStorage.get_data_with_version(ClusterUtils.assignmentPath(stormId), callback != null);
        if (dataWithVersionMap != null) {
            assignment = ClusterUtils.maybeDeserialize((byte[]) dataWithVersionMap.get(IStateStorage.DATA), Assignment.class);
            version = (Integer) dataWithVersionMap.get(IStateStorage.VERSION);
        }
        map.put(IStateStorage.DATA, assignment);
        map.put(IStateStorage.VERSION, version);
        return map;
    }

    @Override
    public Integer assignmentVersion(String stormId, Runnable callback) throws Exception {
        if (callback != null) {
            assignmentVersionCallback.put(stormId, callback);
        }
        return stateStorage.get_version(ClusterUtils.assignmentPath(stormId), callback != null);
    }

    // blobstore state
    @Override
    public List<String> blobstoreInfo(String blobKey) {
        String path = ClusterUtils.blobstorePath(blobKey);
        stateStorage.sync_path(path);
        return stateStorage.get_children(path, false);
    }

    @Override
    public List nimbuses() {
        List<NimbusSummary> nimbusSummaries = new ArrayList<>();
        List<String> nimbusIds = stateStorage.get_children(ClusterUtils.NIMBUSES_SUBTREE, false);
        for (String nimbusId : nimbusIds) {
            byte[] serialized = stateStorage.get_data(ClusterUtils.nimbusPath(nimbusId), false);
            NimbusSummary nimbusSummary = ClusterUtils.maybeDeserialize(serialized, NimbusSummary.class);
            nimbusSummaries.add(nimbusSummary);
        }
        return nimbusSummaries;
    }

    @Override
    @ClojureClass(className="add ILeaderElector, make sure nimbus summary isleader has a correct value while connection state is RECONNECTED")
    public void addNimbusHost(final String nimbusId, final NimbusSummary nimbusSummary, final ILeaderElector leaderElector) {
        // explicit delete for ephmeral node to ensure this session creates the entry.
        stateStorage.delete_node(ClusterUtils.nimbusPath(nimbusId));
        stateStorage.add_listener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                LOG.info("Connection state listener invoked, zookeeper connection state has changed to {}", connectionState);
                if (connectionState.equals(ConnectionState.RECONNECTED)) {
                    try {
                        NimbusInfo nimbusInfo = leaderElector.getLeader();
                        LOG.info("Connection state listener invoked, current nimbus leader is {}", nimbusInfo);
                        if(nimbusInfo != null && nimbusInfo.getHost().equalsIgnoreCase(nimbusSummary.get_host()) && nimbusInfo.getPort() == nimbusSummary.get_port()) {
                            nimbusSummary.set_isLeader(true);
                        }
                    } catch (Throwable t) {
                        LOG.warn("Get nimus leader failed! {}", t.getLocalizedMessage());
                    }
                    LOG.info("Connection state has changed to reconnected so setting nimbuses entry one more time, {}", nimbusSummary);
                    stateStorage.set_ephemeral_node(ClusterUtils.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
                } else if (connectionState.equals(ConnectionState.LOST)) {
                    Utils.exitProcess(1, "Nimbus server exit for Zookeeper connection lost!");
                }
            }
        });

        stateStorage.set_ephemeral_node(ClusterUtils.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
    }

    @Override
    public List<String> activeStorms() {
        return stateStorage.get_children(ClusterUtils.STORMS_SUBTREE, false);
    }

    @Override
    public StormBase stormBase(String stormId, Runnable callback) {
        if (callback != null) {
            stormBaseCallback.put(stormId, callback);
        }
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(ClusterUtils.stormPath(stormId), callback != null), StormBase.class);
    }

    @Override
    public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port) {
        byte[] bytes = stateStorage.get_worker_hb(ClusterUtils.workerbeatPath(stormId, node, port), false);
        return ClusterUtils.maybeDeserialize(bytes, ClusterWorkerHeartbeat.class);

    }

    @Override
    public List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo) {
        List<ProfileRequest> requests = new ArrayList<>();
        List<ProfileRequest> profileRequests = getTopologyProfileRequests(stormId);
        for (ProfileRequest profileRequest : profileRequests) {
            NodeInfo nodeInfo1 = profileRequest.get_nodeInfo();
            if (nodeInfo1.equals(nodeInfo))
                requests.add(profileRequest);
        }
        return requests;
    }

    @Override
    public List<ProfileRequest> getTopologyProfileRequests(String stormId) {
        List<ProfileRequest> profileRequests = new ArrayList<>();
        String path = ClusterUtils.profilerConfigPath(stormId);
        if (stateStorage.node_exists(path, false)) {
            List<String> strs = stateStorage.get_children(path, false);
            for (String str : strs) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + str;
                byte[] raw = stateStorage.get_data(childPath, false);
                ProfileRequest request = ClusterUtils.maybeDeserialize(raw, ProfileRequest.class);
                if (request != null)
                    profileRequests.add(request);
            }
        }
        return profileRequests;
    }

    @Override
    public void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = ClusterUtils.profilerConfigPath(stormId, host, port, profileAction);
        stateStorage.set_data(path, Utils.serialize(profileRequest), acls);
    }

    @Override
    public void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = ClusterUtils.profilerConfigPath(stormId, host, port, profileAction);
        stateStorage.delete_node(path);
    }

    /**
     * need to take executor->node+port in explicitly so that we don't run into a situation where a long dead worker with a skewed clock overrides all the
     * timestamps. By only checking heartbeats with an assigned node+port, and only reading executors from that heartbeat that are actually assigned, we avoid
     * situations like that
     * 
     * @param stormId
     * @param executorNodePort
     * @return
     */
    @Override
    public Map<ExecutorInfo, ExecutorBeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort) {
        Map<ExecutorInfo, ExecutorBeat> executorWhbs = new HashMap<>();

        Map<NodeInfo, List<List<Long>>> nodePortExecutors = Utils.reverseMap(executorNodePort);

        for (Map.Entry<NodeInfo, List<List<Long>>> entry : nodePortExecutors.entrySet()) {

            String node = entry.getKey().get_node();
            Long port = entry.getKey().get_port_iterator().next();
            ClusterWorkerHeartbeat whb = getWorkerHeartbeat(stormId, node, port);
            List<ExecutorInfo> executorInfoList = new ArrayList<>();
            for (List<Long> list : entry.getValue()) {
                executorInfoList.add(new ExecutorInfo(list.get(0).intValue(), list.get(list.size() - 1).intValue()));
            }
            if (whb != null)
                executorWhbs.putAll(ClusterUtils.convertExecutorBeats(executorInfoList, whb));
        }
        return executorWhbs;
    }

    @Override
    public List<String> supervisors(Runnable callback) {
        if (callback != null) {
            supervisorsCallback.set(callback);
        }
        return stateStorage.get_children(ClusterUtils.SUPERVISORS_SUBTREE, callback != null);
    }

    @Override
    public SupervisorInfo supervisorInfo(String supervisorId) {
        String path = ClusterUtils.supervisorPath(supervisorId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, false), SupervisorInfo.class);
    }

    @Override
    public void setupHeatbeats(String stormId) {
        stateStorage.mkdirs(ClusterUtils.workerbeatStormRoot(stormId), acls);
    }

    @Override
    public void teardownHeartbeats(String stormId) {
        try {
            stateStorage.delete_worker_hb(ClusterUtils.workerbeatStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown heartbeats for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void teardownTopologyErrors(String stormId) {
        try {
            stateStorage.delete_node(ClusterUtils.errorStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown errors for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public List<String> heartbeatStorms() {
        return stateStorage.get_worker_hb_children(ClusterUtils.WORKERBEATS_SUBTREE, false);
    }

    @Override
    public List<String> errorTopologies() {
        return stateStorage.get_children(ClusterUtils.ERRORS_SUBTREE, false);
    }

    @Override
    public List<String> backpressureTopologies() {
        return stateStorage.get_children(ClusterUtils.BACKPRESSURE_SUBTREE, false);
    }

    @Override
    public void setTopologyLogConfig(String stormId, LogConfig logConfig) {
        stateStorage.set_data(ClusterUtils.logConfigPath(stormId), Utils.serialize(logConfig), acls);
    }

    @Override
    public LogConfig topologyLogConfig(String stormId, Runnable cb) {
        if (cb != null){
            logConfigCallback.put(stormId, cb);
        }
        String path = ClusterUtils.logConfigPath(stormId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, cb != null), LogConfig.class);
    }

    @Override
    public void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info) {
        if (info != null) {
            String path = ClusterUtils.workerbeatPath(stormId, node, port);
            stateStorage.set_worker_hb(path, Utils.serialize(info), acls);
        }
    }

    @Override
    public void removeWorkerHeartbeat(String stormId, String node, Long port) {
        String path = ClusterUtils.workerbeatPath(stormId, node, port);
        stateStorage.delete_worker_hb(path);
    }

    @Override
    public void supervisorHeartbeat(String supervisorId, SupervisorInfo info) {
        String path = ClusterUtils.supervisorPath(supervisorId);
        stateStorage.set_ephemeral_node(path, Utils.serialize(info), acls);
    }

    /**
     * if znode exists and to be not on?, delete; if exists and on?, do nothing; if not exists and to be on?, create; if not exists and not on?, do nothing;
     * 
     * @param stormId
     * @param node
     * @param port
     * @param on
     */
    @Override
    public void workerBackpressure(String stormId, String node, Long port, boolean on) {
        String path = ClusterUtils.backpressurePath(stormId, node, port);
        boolean existed = stateStorage.node_exists(path, false);
        if (existed) {
            if (on == false)
                stateStorage.delete_node(path);

        } else {
            if (on == true) {
                stateStorage.set_ephemeral_node(path, null, acls);
            }
        }
    }

    /**
     * Check whether a topology is in throttle-on status or not:
     * if the backpresure/storm-id dir is not empty, this topology has throttle-on, otherwise throttle-off.
     * 
     * @param stormId
     * @param callback
     * @return
     */
    @Override
    public boolean topologyBackpressure(String stormId, Runnable callback) {
        if (callback != null) {
            backPressureCallback.put(stormId, callback);
        }
        String path = ClusterUtils.backpressureStormRoot(stormId);
        List<String> childrens = null;
        if(stateStorage.node_exists(path, false)) {
            childrens = stateStorage.get_children(path, callback != null);
        } else {
            childrens = new ArrayList<>();
        }
        return childrens.size() > 0;

    }

    @Override
    public void setupBackpressure(String stormId) {
        stateStorage.mkdirs(ClusterUtils.backpressureStormRoot(stormId), acls);
    }

    @Override
    public void removeBackpressure(String stormId) {
        try {
            stateStorage.delete_node(ClusterUtils.backpressureStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown backpressure node for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void removeWorkerBackpressure(String stormId, String node, Long port) {
        String path = ClusterUtils.backpressurePath(stormId, node, port);
        boolean existed = stateStorage.node_exists(path, false);
        if (existed) {
            stateStorage.delete_node(path);
        }
    }

    @Override
    public void activateStorm(String stormId, StormBase stormBase) {
        String path = ClusterUtils.stormPath(stormId);
        stateStorage.set_data(path, Utils.serialize(stormBase), acls);
    }

    /**
     * To update this function due to APersistentMap/APersistentSet is clojure's structure
     * 
     * @param stormId
     * @param newElems
     */
    @Override
    public void updateStorm(String stormId, StormBase newElems) {

        StormBase stormBase = stormBase(stormId, null);
        if (stormBase.get_component_executors() != null) {

            Map<String, Integer> newComponentExecutors = new HashMap<>();
            Map<String, Integer> componentExecutors = newElems.get_component_executors();
            // componentExecutors maybe be APersistentMap, which don't support "put"
            for (Map.Entry<String, Integer> entry : componentExecutors.entrySet()) {
                newComponentExecutors.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, Integer> entry : stormBase.get_component_executors().entrySet()) {
                if (!componentExecutors.containsKey(entry.getKey())) {
                    newComponentExecutors.put(entry.getKey(), entry.getValue());
                }
            }
            if (newComponentExecutors.size() > 0)
                newElems.set_component_executors(newComponentExecutors);
        }

        Map<String, DebugOptions> ComponentDebug = new HashMap<>();
        Map<String, DebugOptions> oldComponentDebug = stormBase.get_component_debug();

        Map<String, DebugOptions> newComponentDebug = newElems.get_component_debug();
        /// oldComponentDebug.keySet()/ newComponentDebug.keySet() maybe be APersistentSet, which don't support addAll
        Set<String> debugOptionsKeys = new HashSet<>();
        debugOptionsKeys.addAll(oldComponentDebug.keySet());
        debugOptionsKeys.addAll(newComponentDebug.keySet());
        for (String key : debugOptionsKeys) {
            boolean enable = false;
            double samplingpct = 0;
            if (oldComponentDebug.containsKey(key)) {
                enable = oldComponentDebug.get(key).is_enable();
                samplingpct = oldComponentDebug.get(key).get_samplingpct();
            }
            if (newComponentDebug.containsKey(key)) {
                enable = newComponentDebug.get(key).is_enable();
                samplingpct += newComponentDebug.get(key).get_samplingpct();
            }
            DebugOptions debugOptions = new DebugOptions();
            debugOptions.set_enable(enable);
            debugOptions.set_samplingpct(samplingpct);
            ComponentDebug.put(key, debugOptions);
        }
        if (ComponentDebug.size() > 0) {
            newElems.set_component_debug(ComponentDebug);
        }

        if (StringUtils.isBlank(newElems.get_name())) {
            newElems.set_name(stormBase.get_name());
        }
        if (newElems.get_status() == null) {
            newElems.set_status(stormBase.get_status());
        }
        if (newElems.get_num_workers() == 0) {
            newElems.set_num_workers(stormBase.get_num_workers());
        }
        if (newElems.get_launch_time_secs() == 0) {
            newElems.set_launch_time_secs(stormBase.get_launch_time_secs());
        }
        if (StringUtils.isBlank(newElems.get_owner())) {
            newElems.set_owner(stormBase.get_owner());
        }
        if (newElems.get_topology_action_options() == null) {
            newElems.set_topology_action_options(stormBase.get_topology_action_options());
        }
        if (newElems.get_status() == null) {
            newElems.set_status(stormBase.get_status());
        }
        stateStorage.set_data(ClusterUtils.stormPath(stormId), Utils.serialize(newElems), acls);
    }

    @Override
    public void removeStormBase(String stormId) {
        stateStorage.delete_node(ClusterUtils.stormPath(stormId));
    }

    @Override
    public void setAssignment(String stormId, Assignment info) {
        stateStorage.set_data(ClusterUtils.assignmentPath(stormId), Utils.serialize(info), acls);
    }

    @Override
    public void setupBlobstore(String key, NimbusInfo nimbusInfo, Integer versionInfo) {
        String path = ClusterUtils.blobstorePath(key) + ClusterUtils.ZK_SEPERATOR + nimbusInfo.toHostPortString() + "-" + versionInfo;
        LOG.info("set-path: {}", path);
        stateStorage.mkdirs(ClusterUtils.blobstorePath(key), acls);
        stateStorage.delete_node_blobstore(ClusterUtils.blobstorePath(key), nimbusInfo.toHostPortString());
        stateStorage.set_ephemeral_node(path, null, acls);
    }

    @Override
    public List<String> activeKeys() {
        return stateStorage.get_children(ClusterUtils.BLOBSTORE_SUBTREE, false);
    }

    // blobstore state
    @Override
    public List<String> blobstore(Runnable callback) {
        if (callback != null) {
            blobstoreCallback.set(callback);
        }
        stateStorage.sync_path(ClusterUtils.BLOBSTORE_SUBTREE);
        return stateStorage.get_children(ClusterUtils.BLOBSTORE_SUBTREE, callback != null);

    }

    @Override
    public void removeStorm(String stormId) {
        stateStorage.delete_node(ClusterUtils.assignmentPath(stormId));
        stateStorage.delete_node(ClusterUtils.credentialsPath(stormId));
        stateStorage.delete_node(ClusterUtils.logConfigPath(stormId));
        stateStorage.delete_node(ClusterUtils.profilerConfigPath(stormId));
        removeStormBase(stormId);
    }

    @Override
    public void removeBlobstoreKey(String blobKey) {
        LOG.debug("remove key {}", blobKey);
        stateStorage.delete_node(ClusterUtils.blobstorePath(blobKey));
    }

    @Override
    public void removeKeyVersion(String blobKey) {
        stateStorage.delete_node(ClusterUtils.blobstoreMaxKeySequenceNumberPath(blobKey));
    }

    @Override
    public void reportError(String stormId, String componentId, String node, Long port, Throwable error) {

        String path = ClusterUtils.errorPath(stormId, componentId);
        String lastErrorPath = ClusterUtils.lastErrorPath(stormId, componentId);
        ErrorInfo errorInfo = new ErrorInfo(ClusterUtils.stringifyError(error), Time.currentTimeSecs());
        errorInfo.set_host(node);
        errorInfo.set_port(port.intValue());
        byte[] serData = Utils.serialize(errorInfo);
        stateStorage.mkdirs(path, acls);
        stateStorage.create_sequential(path + ClusterUtils.ZK_SEPERATOR + "e", serData, acls);
        stateStorage.set_data(lastErrorPath, serData, acls);
        List<String> childrens = stateStorage.get_children(path, false);

        Collections.sort(childrens, new Comparator<String>() {
            public int compare(String arg0, String arg1) {
                return Long.compare(Long.parseLong(arg0.substring(1)), Long.parseLong(arg1.substring(1)));
            }
        });

        while (childrens.size() > 10) {
            stateStorage.delete_node(path + ClusterUtils.ZK_SEPERATOR + childrens.remove(0));
        }
    }

    @Override
    public List<ErrorInfo> errors(String stormId, String componentId) {
        List<ErrorInfo> errorInfos = new ArrayList<>();
        String path = ClusterUtils.errorPath(stormId, componentId);
        if (stateStorage.node_exists(path, false)) {
            List<String> childrens = stateStorage.get_children(path, false);
            for (String child : childrens) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + child;
                ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(childPath, false), ErrorInfo.class);
                if (errorInfo != null)
                    errorInfos.add(errorInfo);
            }
        }
        Collections.sort(errorInfos, new Comparator<ErrorInfo>() {
            public int compare(ErrorInfo arg0, ErrorInfo arg1) {
                return Integer.compare(arg1.get_error_time_secs(), arg0.get_error_time_secs());
            }
        });

        return errorInfos;
    }

    @Override
    public ErrorInfo lastError(String stormId, String componentId) {

        String path = ClusterUtils.lastErrorPath(stormId, componentId);
        if (stateStorage.node_exists(path, false)) {
            ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(path, false), ErrorInfo.class);
            return errorInfo;
        }

        return null;
    }

    @Override
    public void setCredentials(String stormId, Credentials creds, Map topoConf) throws NoSuchAlgorithmException {
        List<ACL> aclList = ClusterUtils.mkTopoOnlyAcls(topoConf);
        String path = ClusterUtils.credentialsPath(stormId);
        stateStorage.set_data(path, Utils.serialize(creds), aclList);

    }

    @Override
    public Credentials credentials(String stormId, Runnable callback) {
        if (callback != null) {
            credentialsCallback.put(stormId, callback);
        }
        String path = ClusterUtils.credentialsPath(stormId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, callback != null), Credentials.class);

    }

    @Override
    public void disconnect() {
        stateStorage.unregister(stateId);
        if (solo)
            stateStorage.close();
    }

    /**add by jstorm developer -- begin**/

    public void registerUIServerInfo(NodeInfo serverInfo) throws Exception {
      if (serverInfo != null) {
        stateStorage.add_listener(this.defaultListener);
        stateStorage.set_ephemeral_node(ClusterUtils.UI_SUBTREE, Utils.serialize(serverInfo), acls);
      } else {
        LOG.error("register host to zk fail!");
        throw new Exception();
      }
    }

    @Override
    public NodeInfo getUIServerInfo() throws Exception {
      byte[] zk_data = stateStorage.get_data(ClusterUtils.UI_SUBTREE, false);
      if (zk_data == null) {
        return null;
      }
      return Utils.deserialize(zk_data, NodeInfo.class);
    }

    @Override
    public void setWorkerJvmInfo(String host, Long port,
        WorkerJvmInfo workerJvmInfo) {
      stateStorage.set_ephemeral_node(ClusterUtils.workerJvmInfoPath(host, port), Utils.serialize(workerJvmInfo), acls);
    }

    @Override
    public WorkerJvmInfo getWorkerJvmInfo(String host, Long port) {
     return ClusterUtils.maybeDeserialize(stateStorage.get_data(ClusterUtils.workerJvmInfoPath(host, port), false), WorkerJvmInfo.class);
    }

    @Override
    public List<String> workerJvmNodes() {
      stateStorage.mkdirs(ClusterUtils.WORKER_JVM_SUBTREE, acls);
      return stateStorage.get_children(ClusterUtils.WORKER_JVM_SUBTREE, false);
    }

    @Override
    public void removeWorkerJvmNode(String host, Long port) {
      stateStorage.delete_node(ClusterUtils.workerJvmInfoPath(host, port)); 
    }

    @Override
    public void addDrpcServer(DRPCServerInfo drpcServerinfo) {
      // explicit delete for ephmeral node to ensure this session creates the entry.
      String drpcServerId = drpcServerinfo.toHostPortString();
      stateStorage.delete_node(ClusterUtils.drpcServerPath(drpcServerId));
      stateStorage.add_listener(this.defaultListener);
      stateStorage.set_ephemeral_node(ClusterUtils.drpcServerPath(drpcServerId), null, acls);
    }

    @Override
    public List<String> drpcServers() {
      return stateStorage.get_children(ClusterUtils.DRPC_SERVER_SUBTREE, false);
    }

    @Override
    public List<String> logviewers(Runnable callback) {
      stateStorage.mkdirs(ClusterUtils.LOGVIEWER_SUBTREE, acls);
      if (callback != null) {
        logviewerCallback.set(callback);
      }
      return stateStorage.get_children(ClusterUtils.LOGVIEWER_SUBTREE, callback != null);
    }

    @Override
    public void addLogviewer(String logviewerId, NodeInfo realNodePorts) {
      String path = ClusterUtils.logviewerPath(logviewerId);
      stateStorage.delete_node(path);
      stateStorage.add_listener(this.defaultListener);
      stateStorage.set_ephemeral_node(path, Utils.serialize(realNodePorts), acls);
    }

    @Override
    public NodeInfo logviewer(String logviewerId) {
      String path = ClusterUtils.logviewerPath(logviewerId);
      return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, false), NodeInfo.class);
    }

    @Override
    public List<String> nimbusHostports(Runnable callback) {
      if (callback != null) {
        nimbusesCallback.set(callback);
      }
      return stateStorage.get_children(ClusterUtils.NIMBUSES_SUBTREE, callback != null);
    }
    
    @Override
    public Map<String, List<ErrorInfo>> errorsByStormId(String stormId) {
        Map<String, List<ErrorInfo>> componentToErrorInfos = new HashMap<String, List<ErrorInfo>>();
        String errorRootPath = ClusterUtils.errorStormRoot(stormId);
        if (stateStorage.node_exists(errorRootPath, false)) {
            List<String> componentChildrens = stateStorage.get_children(errorRootPath, false);
            for (String component : componentChildrens) {
              String componentPath = ClusterUtils.errorPath(stormId, component);
              List<String> childrens = stateStorage.get_children(componentPath, false);
              List<ErrorInfo> errorInfos = new ArrayList<>();
              for (String child : childrens) {
                String childPath = componentPath + ClusterUtils.ZK_SEPERATOR + child;
                ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(childPath, false), ErrorInfo.class);
                if (errorInfo != null) {
                  errorInfos.add(errorInfo);
                }
              }
              Collections.sort(errorInfos, new Comparator<ErrorInfo>() {
                public int compare(ErrorInfo arg0, ErrorInfo arg1) {
                    return Integer.compare(arg1.get_error_time_secs(), arg0.get_error_time_secs());
                }
              });
              componentToErrorInfos.put(component, errorInfos);
            }
        }
        return componentToErrorInfos;
    }

    @Override
    public Map<String, ErrorInfo> lastErrorByStormId(String stormId) {
        Map<String, ErrorInfo> componentToLastError = new HashMap<String, ErrorInfo>();
        String errorRootPath = ClusterUtils.errorStormRoot(stormId);
        if (stateStorage.node_exists(errorRootPath, false)) {
          List<String> componentChildrens = stateStorage.get_children(errorRootPath, false);
          for (String component : componentChildrens) {
            String componentLastErrorpath = ClusterUtils.lastErrorPath(stormId, component);
            if (stateStorage.node_exists(componentLastErrorpath, false)) {
              ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(componentLastErrorpath, false), ErrorInfo.class);
              componentToLastError.put(component, errorInfo);
            }
          }
        }
        return componentToLastError;
    }

    @Override
    public void updateComponentBackpressure(String stormId, String component) {
      String path = ClusterUtils.backpressureStormRoot(stormId);
      if(stateStorage.node_exists(path, false)) {
        TopologyBackpressure topologyBackpressure  = new TopologyBackpressure();
        byte[] backpressureData = stateStorage.get_data(path, false);
        if (backpressureData!=null && backpressureData.length >= Utils.serialize(topologyBackpressure).length) {
          topologyBackpressure = ClusterUtils.maybeDeserialize(backpressureData, TopologyBackpressure.class);
        }
        int componentCount = topologyBackpressure.get_componentToBackpressure().get(component) == null ? 
            0 : topologyBackpressure.get_componentToBackpressure().get(component);
        componentCount++;
        topologyBackpressure.get_componentToBackpressure().put(component, componentCount);
        stateStorage.set_data(path, Utils.serialize(topologyBackpressure), acls);
      } else {
        TopologyBackpressure topologyBackpressure  = new TopologyBackpressure();
        topologyBackpressure.get_componentToBackpressure().put(component, 1);
        stateStorage.set_data(path, Utils.serialize(topologyBackpressure), acls);
      }
    }

    @Override
    public TopologyBackpressure getTopologyBackpressure(String stormId) {
      TopologyBackpressure topologyBackpressure  = new TopologyBackpressure();
      String rootPath = ClusterUtils.backpressureStormRoot(stormId);
      if(!stateStorage.node_exists(rootPath, false)) {
        return topologyBackpressure;
      }
      byte[] backpressureData = stateStorage.get_data(rootPath, false);
      if (backpressureData!=null && backpressureData.length >= Utils.serialize(topologyBackpressure).length) {
        topologyBackpressure = ClusterUtils.maybeDeserialize(backpressureData, TopologyBackpressure.class);
      }
      return topologyBackpressure;
    }

    /**add by jstorm developer -- end**/
}
