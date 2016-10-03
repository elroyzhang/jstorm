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
package org.apache.storm.daemon.supervisor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.workermanager.IWorkerManager;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.Worker;
import com.tencent.jstorm.daemon.worker.WorkerShutdown;
import com.tencent.jstorm.psim.ProcessSimulator;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 1. to kill are those in allocated that are dead or disallowed 2. kill the ones that should be dead - read pids, kill -9 and individually remove file - rmr
 * heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log) 3. of the rest, figure out what assignments aren't yet satisfied 4. generate new worker
 * ids, write new "approved workers" to LS 5. create local dir for worker id 5. launch new workers (give worker-id, port, and supervisor-id) 6. wait for workers
 * launch
 */
public class SyncProcessEvent implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(SyncProcessEvent.class);

    private  LocalState localState;
    private  SupervisorData supervisorData;
    @ClojureClass(className="For docker")
    private  Map<Integer, Integer> realToVirtualPort;
    public static final ExecutorInfo SYSTEM_EXECUTOR_INFO = new ExecutorInfo(-1, -1);

    private class ProcessExitCallback implements Utils.ExitCodeCallable {
        private final String logPrefix;
        private final String workerId;

        public ProcessExitCallback(String logPrefix, String workerId) {
            this.logPrefix = logPrefix;
            this.workerId = workerId;
        }

        @Override
        public Object call() throws Exception {
            return null;
        }

        @Override
        public Object call(int exitCode) {
            LOG.info("{} exited with code: {}", logPrefix, exitCode);
            supervisorData.getDeadWorkers().add(workerId);
            return null;
        }
    }

    public SyncProcessEvent(){

    }
    public SyncProcessEvent(SupervisorData supervisorData) {
        init(supervisorData);
    }
    
    @SuppressWarnings("unchecked")
    @ClojureClass(className="For docker")
    public void init(SupervisorData supervisorData){
        this.supervisorData = supervisorData;
        this.localState = supervisorData.getLocalState();
        this.realToVirtualPort =
            (Map<Integer, Integer>) supervisorData.getConf().get("storm.real.virtual.ports");
    }

    @ClojureClass(className = "add supervisor active check , debug log and catch exception")
    @Override
    public void run() {
        LOG.debug("Syncing processes");
        if (!supervisorData.isActive()) {
          return;
        }
        try {
            Map conf = supervisorData.getConf();
            Map<Integer, LocalAssignment> assignedExecutors = localState.getLocalAssignmentsMap();

            if (assignedExecutors == null) {
                assignedExecutors = new HashMap<>();
            }
            int now = Time.currentTimeSecs();

            Map<String, StateHeartbeat> localWorkerStats = getLocalWorkerStats(supervisorData, assignedExecutors, now);

            Set<String> keeperWorkerIds = new HashSet<>();
            Set<Integer> keepPorts = new HashSet<>();
            for (Map.Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
                StateHeartbeat stateHeartbeat = entry.getValue();
                if (stateHeartbeat.getState() == State.VALID) {
                    keeperWorkerIds.add(entry.getKey());
                    keepPorts.add(stateHeartbeat.getHeartbeat().get_port());
                }
            }
            Map<Integer, LocalAssignment> reassignExecutors = getReassignExecutors(assignedExecutors, keepPorts);
            Map<Integer, String> newWorkerIds = new HashMap<>();
            for (Integer port : reassignExecutors.keySet()) {
                newWorkerIds.put(port, Utils.uuid());
            }
            LOG.debug("Assigned executors: {}", assignedExecutors);
            LOG.debug("Allocated: {}", localWorkerStats);
            LOG.debug("keepPorts:" + keepPorts.toString());
            LOG.debug("newWorkerIds:" + newWorkerIds.toString());

            /** added by JStorm developer, for paral kill workers */
            List<String> shutdownWorkerIds = new ArrayList<String>();
            for (Map.Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
                StateHeartbeat stateHeartbeat = entry.getValue();
                if (stateHeartbeat.getState() != State.VALID) {
                    LOG.info("Shutting down and clearing state for id {}, Current supervisor time: {}, State: {}, Heartbeat: {}", entry.getKey(), now,
                            stateHeartbeat.getState(), stateHeartbeat.getHeartbeat());
                    // 2. kill the ones that should be dead
                    shutdownWorkerIds.add(entry.getKey());
                    // killWorker(supervisorData, supervisorData.getWorkerManager(), entry.getKey());
                }
            }
            /** added by JStorm developer, for paral kill workers */
            killWorkersParallel(supervisorData, supervisorData.getWorkerManager(), shutdownWorkerIds);

            // start new workers
            Map<String, Integer> newWorkerPortToIds = startNewWorkers(newWorkerIds, reassignExecutors);

            Map<String, Integer> allWorkerPortToIds = new HashMap<>();
            Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
            for (String keeper : keeperWorkerIds) {
                allWorkerPortToIds.put(keeper, approvedWorkers.get(keeper));
            }
            allWorkerPortToIds.putAll(newWorkerPortToIds);
            localState.setApprovedWorkers(allWorkerPortToIds);
            // waitForWorkersLaunch(conf, newWorkerPortToIds.keySet());
            waitForWorkersLaunch(conf, newWorkerPortToIds);
        } catch (Exception e) {
            LOG.error("Failed Sync Process", e);
            throw Utils.wrapInRuntime(e);
        }

    }

    @ClojureClass(className = "add success or fail worker check and message, and port for log")
    protected void waitForWorkersLaunch(Map conf, Map<String, Integer> portToIds) throws Exception {
        int startTime = Time.currentTimeSecs();
        int timeOut = (int) conf.get(Config.NIMBUS_SUPERVISOR_TIMEOUT_SECS);
        for (Entry<String, Integer> e : portToIds.entrySet()) {
            String workerId = e.getKey();
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            while (true) {
                LSWorkerHeartbeat hb = localState.getWorkerHeartBeat();
                 if (hb != null || (Time.currentTimeSecs() - startTime) > timeOut)
                     break;
                 LOG.info("{} still hasn't started", workerId);
                 Time.sleep(500);
            }

            // if (localState.getWorkerHeartBeat() == null) {
            //     LOG.info("Worker {} failed to start", workerId);
            // }
            if (localState.getWorkerHeartBeat() != null) {
              LOG.info("Successfully start worker {}:{} ", workerId, e.getValue().intValue());
            } else {
              LOG.info("Worker {}:{} failed to start!", workerId, e.getValue().intValue());
            }
        }
    }

    protected Map<Integer, LocalAssignment> getReassignExecutors(Map<Integer, LocalAssignment> assignExecutors, Set<Integer> keepPorts) {
        Map<Integer, LocalAssignment> reassignExecutors = new HashMap<>();
        reassignExecutors.putAll(assignExecutors);
        for (Integer port : keepPorts) {
            reassignExecutors.remove(port);
        }
        return reassignExecutors;
    }
    
    /**
     * Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead
     * 
     * @param assignedExecutors
     * @return
     * @throws Exception
     */
    @ClojureClass(className="add PROCESS_NOT_EXIST for State while worker's process not exist, e.g. worker been killed.")
    public Map<String, StateHeartbeat> getLocalWorkerStats(SupervisorData supervisorData, Map<Integer, LocalAssignment> assignedExecutors, int now) throws Exception {
        Map<String, StateHeartbeat> workerIdHbstate = new HashMap<>();
        Map conf = supervisorData.getConf();
        LocalState localState = supervisorData.getLocalState();
        Map<String, LSWorkerHeartbeat> idToHeartbeat = SupervisorUtils.readWorkerHeartbeats(conf);
        Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
        Set<String> approvedIds = new HashSet<>();
        if (approvedWorkers != null) {
            approvedIds.addAll(approvedWorkers.keySet());
        }
        for (Map.Entry<String, LSWorkerHeartbeat> entry : idToHeartbeat.entrySet()) {
            String workerId = entry.getKey();
            LSWorkerHeartbeat whb = entry.getValue();
            State state;
            if (whb == null) {
                state = State.NOT_STARTED;
            } else if (!approvedIds.contains(workerId) || !matchesAssignment(whb, assignedExecutors)) {
                state = State.DISALLOWED;
            } else if (whb.get_process_id() != -1
                && !CoreUtil.isProcessExists(String.valueOf(whb.get_process_id()))) {
              state = State.PROCESS_NOT_EXIST;
            } else if (supervisorData.getDeadWorkers().contains(workerId)) {
                LOG.info("Worker Process {} has died", workerId);
                state = State.TIMED_OUT;
            } else if (SupervisorUtils.isWorkerHbTimedOut(now, whb, conf)) {
                state = State.TIMED_OUT;
            } else {
                state = State.VALID;
            }
            LOG.debug("Worker:{} state:{} WorkerHeartbeat:{} at supervisor time-secs {}", workerId, state, whb, now);
            workerIdHbstate.put(workerId, new StateHeartbeat(state, whb));
        }
        return workerIdHbstate;
    }

    protected boolean matchesAssignment(LSWorkerHeartbeat whb, Map<Integer, LocalAssignment> assignedExecutors) {
        LocalAssignment localAssignment = assignedExecutors.get(whb.get_port());
        if (localAssignment == null || !localAssignment.get_topology_id().equals(whb.get_topology_id())) {
            return false;
        }
        List<ExecutorInfo> executorInfos = new ArrayList<>();
        executorInfos.addAll(whb.get_executors());
        // remove SYSTEM_EXECUTOR_ID
        executorInfos.remove(SYSTEM_EXECUTOR_INFO);
        List<ExecutorInfo> localExecuorInfos = localAssignment.get_executors();

        if (localExecuorInfos.size() != executorInfos.size())
            return false;

        for (ExecutorInfo executorInfo : localExecuorInfos){
            if (!localExecuorInfos.contains(executorInfo))
                return false;
        }
        return true;
    }

    /**
     * launch a worker in local mode.
     */
    @SuppressWarnings("rawtypes")
    @ClojureClass(className = "Finish this method with our code")
    protected void launchLocalWorker(SupervisorData supervisorData, String stormId, Long port, String workerId, WorkerResources resources) throws IOException {
        // port this function after porting worker to java
      Map conf = supervisorData.getConf();
      String pid = Utils.uuid();
      WorkerShutdown worker;
      try {
        worker = Worker.mkWorker(conf, supervisorData.getSharedContext(),
            stormId, supervisorData.getAssignmentId(), port.intValue(), workerId);
      } catch (Exception e) {
        throw Utils.wrapInRuntime(e);
      }
      ProcessSimulator.registerProcess(pid, worker);
      supervisorData.getWorkerThreadPids().put(workerId, pid);
    }

    protected void launchDistributedWorker(IWorkerManager workerManager, Map conf, String supervisorId, String assignmentId, String stormId, Long port, String workerId,
                                           WorkerResources resources, ConcurrentHashSet deadWorkers) throws IOException {
        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        writeLogMetadata(stormConf, user, workerId, stormId, port, conf);
        ConfigUtils.setWorkerUserWSE(conf, workerId, user);
        createArtifactsLink(conf, stormId, port, workerId);

        String logPrefix = "Worker Process " + workerId;
        if (deadWorkers != null)
            deadWorkers.remove(workerId);
        createBlobstoreLinks(conf, stormId, workerId);
        ProcessExitCallback processExitCallback = new ProcessExitCallback(logPrefix, workerId);
        workerManager.launchWorker(supervisorId, assignmentId, stormId, port, workerId, resources, processExitCallback);
    }

    @ClojureClass(className="For docker")
    protected Map<String, Integer> startNewWorkers(Map<Integer, String> newWorkerIds, Map<Integer, LocalAssignment> reassignExecutors) throws IOException {
        Map<String, Integer> newValidWorkerIds = new HashMap<>();
        Map conf = supervisorData.getConf();
        String supervisorId = supervisorData.getSupervisorId();
        String clusterMode = ConfigUtils.clusterMode(conf);

        for (Map.Entry<Integer, LocalAssignment> entry : reassignExecutors.entrySet()) {
            // Integer port = entry.getKey();
            /** added by JStorm developer start */
            Integer realPort = entry.getKey();
            Integer virtualPort = realPort;
            if (realToVirtualPort != null && realToVirtualPort.containsKey(realPort)) {
              virtualPort = realToVirtualPort.get(realPort);
              LOG.info("RealPort-->VirtualPort: " + realPort + ":" + virtualPort);
            }
            /** added by JStorm developer end */

            LocalAssignment assignment = entry.getValue();
            // String workerId = newWorkerIds.get(port);
            String workerId = newWorkerIds.get(realPort);
            String stormId = assignment.get_topology_id();
            WorkerResources resources = assignment.get_resources();

            // This condition checks for required files exist before launching the worker
            if (SupervisorUtils.doRequiredTopoFilesExist(conf, stormId)) {
                String pidsPath = ConfigUtils.workerPidsRoot(conf, workerId);
                String hbPath = ConfigUtils.workerHeartbeatsRoot(conf, workerId);

                LOG.info("Launching worker with assignment {} for this supervisor {} on port {} with id {}", assignment, supervisorData.getSupervisorId(), virtualPort,
                        workerId);

                FileUtils.forceMkdir(new File(pidsPath));
                FileUtils.forceMkdir(new File(ConfigUtils.workerTmpRoot(conf, workerId)));
                FileUtils.forceMkdir(new File(hbPath));

                if (clusterMode.endsWith("distributed")) {
                    // launchDistributedWorker(supervisorData.getWorkerManager(), conf, supervisorId, supervisorData.getAssignmentId(), stormId, port.longValue(), workerId, resources, supervisorData.getDeadWorkers());
                    launchDistributedWorker(supervisorData.getWorkerManager(), conf, supervisorId, supervisorData.getAssignmentId(), stormId, virtualPort.longValue(), workerId, resources, supervisorData.getDeadWorkers());
                } else if (clusterMode.endsWith("local")) {
                    // launchLocalWorker(supervisorData, stormId, port.longValue(), workerId, resources);
                    launchLocalWorker(supervisorData, stormId, virtualPort.longValue(), workerId, resources);
                }
                // newValidWorkerIds.put(workerId, port);
                newValidWorkerIds.put(workerId, virtualPort);

            } else {
                // LOG.info("Missing topology storm code, so can't launch worker with assignment {} for this supervisor {} on port {} with id {}", assignment,
                //        supervisorData.getSupervisorId(), port, workerId);
                LOG.info("Missing topology storm code, so can't launch worker with assignment {} for this supervisor {} on port {} with id {}", assignment,
                    supervisorData.getSupervisorId(), virtualPort, workerId);
            }

        }
        return newValidWorkerIds;
    }

    public void writeLogMetadata(Map stormconf, String user, String workerId, String stormId, Long port, Map conf) throws IOException {
        Map data = new HashMap();
        data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        data.put("worker-id", workerId);

        Set<String> logsGroups = new HashSet<>();
        //for supervisor-test
        if (stormconf.get(Config.LOGS_GROUPS) != null) {
            List<String> groups = (List<String>) stormconf.get(Config.LOGS_GROUPS);
            for (String group : groups){
                logsGroups.add(group);
            }
        }
        if (stormconf.get(Config.TOPOLOGY_GROUPS) != null) {
            List<String> topGroups = (List<String>) stormconf.get(Config.TOPOLOGY_GROUPS);
            logsGroups.addAll(topGroups);
        }
        data.put(Config.LOGS_GROUPS, logsGroups.toArray());

        Set<String> logsUsers = new HashSet<>();
        if (stormconf.get(Config.LOGS_USERS) != null) {
            List<String> logUsers = (List<String>) stormconf.get(Config.LOGS_USERS);
            for (String logUser : logUsers){
                logsUsers.add(logUser);
            }
        }
        if (stormconf.get(Config.TOPOLOGY_USERS) != null) {
            List<String> topUsers = (List<String>) stormconf.get(Config.TOPOLOGY_USERS);
            for (String logUser : topUsers){
                logsUsers.add(logUser);
            }
        }
        data.put(Config.LOGS_USERS, logsUsers.toArray());
        writeLogMetadataToYamlFile(stormId, port, data, conf);
    }

    /**
     * run worker as user needs the directory to have special permissions or it is insecure
     * 
     * @param stormId
     * @param port
     * @param data
     * @param conf
     * @throws IOException
     */
    public void writeLogMetadataToYamlFile(String stormId, Long port, Map data, Map conf) throws IOException {
        File file = ConfigUtils.getLogMetaDataFile(conf, stormId, port.intValue());

        if (!Utils.checkFileExists(file.getParent())) {
            if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
                FileUtils.forceMkdir(file.getParentFile());
                SupervisorUtils.setupStormCodeDir(conf, ConfigUtils.readSupervisorStormConf(conf, stormId), file.getParentFile().getCanonicalPath());
            } else {
                file.getParentFile().mkdirs();
            }
        }
        FileWriter writer = new FileWriter(file);
        Yaml yaml = new Yaml();
        try {
            yaml.dump(data, writer);
        }finally {
            writer.close();
        }
    }

    /**
     * Create a symlink from workder directory to its port artifacts directory
     * 
     * @param conf
     * @param stormId
     * @param port
     * @param workerId
     */
    @ClojureClass(className="for docker")
    protected void createArtifactsLink(Map conf, String stormId, Long port, String workerId) throws IOException {
        String workerDir = ConfigUtils.workerRoot(conf, workerId);
        String topoDir = ConfigUtils.workerArtifactsRoot(conf, stormId);
        if (Utils.checkFileExists(workerDir)) {
            LOG.debug("Creating symlinks for worker-id: {} storm-id: {} to its port artifacts directory", workerId, stormId);

            /** Added by JStorm developer */
            Map<Integer, Integer> virualToRealPort = (Map<Integer, Integer>)conf.get("storm.virtual.real.ports");
            String portStr = String.valueOf(port);
            if (virualToRealPort != null && virualToRealPort.containsKey(port.intValue())) {
              portStr = String.valueOf(virualToRealPort.get(port.intValue()));
            }
            Utils.createSymlink(workerDir, topoDir, portStr,"artifacts");
        }
    }

    /**
     * Create symlinks in worker launch directory for all blobs
     * 
     * @param conf
     * @param stormId
     * @param workerId
     * @throws IOException
     */
    protected void createBlobstoreLinks(Map conf, String stormId, String workerId) throws IOException {
        String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        Map stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        String workerRoot = ConfigUtils.workerRoot(conf, workerId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        List<String> blobFileNames = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> blobInfo = entry.getValue();
                String ret = null;
                if (blobInfo != null && blobInfo.containsKey("localname")) {
                    ret = (String) blobInfo.get("localname");
                } else {
                    ret = key;
                }
                blobFileNames.add(ret);
            }
        }
        List<String> resourceFileNames = new ArrayList<>();
        resourceFileNames.add(ConfigUtils.RESOURCES_SUBDIR);
        resourceFileNames.addAll(blobFileNames);
        LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", workerId, stormId, resourceFileNames.size(), resourceFileNames);
        Utils.createSymlink(workerRoot, stormRoot, ConfigUtils.RESOURCES_SUBDIR);
        for (String fileName : blobFileNames) {
            Utils.createSymlink(workerRoot, stormRoot, fileName, fileName);
        }
    }

    public void killWorker(SupervisorData supervisorData, IWorkerManager workerManager, String workerId) throws IOException, InterruptedException{
        workerManager.shutdownWorker(supervisorData.getSupervisorId(), workerId, supervisorData.getWorkerThreadPids());
        boolean success = workerManager.cleanupWorker(workerId);
        if (success){
            supervisorData.getDeadWorkers().remove(workerId);
        }
    }

    @ClojureClass(className = "add shut down worker parallism method")
    public void killWorkersParallel(SupervisorData supervisorData, IWorkerManager workerManager, List<String> workerIds) throws IOException, InterruptedException{
      workerManager.shutdownWorkers(supervisorData.getSupervisorId(), workerIds, supervisorData.getWorkerThreadPids());
      for(String workerId : workerIds){
        boolean success = workerManager.cleanupWorker(workerId);
        if (success){
            supervisorData.getDeadWorkers().remove(workerId);
        }
      }
   }
}
