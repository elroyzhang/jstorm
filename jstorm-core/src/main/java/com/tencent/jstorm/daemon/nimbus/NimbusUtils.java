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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.BlobStoreUtils;
import org.apache.storm.blobstore.KeyFilter;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.ExecutorBeat;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.nimbus.DefaultTopologyValidator;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorCache;
import com.tencent.jstorm.daemon.nimbus.threads.OlderFileFilter;
import com.tencent.jstorm.localstate.LocalStateUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.ReflectionUtils;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 5:53:40 PM Jan 8, 2016
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class NimbusUtils {
  private static Logger LOG = LoggerFactory.getLogger(NimbusUtils.class);

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#mapify-serializations")
  private static Map mapifySerializations(List sers) {
    Map rtn = new HashMap();
    if (sers != null) {
      int size = sers.size();
      for (int i = 0; i < size; i++) {
        if (sers.get(i) instanceof Map) {
          rtn.putAll((Map) sers.get(i));
        } else {
          rtn.put(sers.get(i), null);
        }
      }
    }
    return rtn;
  }

  /**
   * ensure that serializations are same for all tasks no matter what's on the
   * supervisors. this also allows you to declare the serializations as a
   * sequence
   * 
   * @param conf
   * @param stormConf
   * @param topology
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#normalize-conf")
  public static Map normalizeConf(Map conf, Map stormConf,
      StormTopology topology) throws Exception {
    Set<String> componentIds = ThriftTopologyUtils.getComponentIds(topology);
    List kryoRegisterList = new ArrayList();
    List kryoDecoratorList = new ArrayList();
    for (String componentId : componentIds) {
      ComponentCommon common =
          ThriftTopologyUtils.getComponentCommon(topology, componentId);
      String json = common.get_json_conf();
      if (json == null) {
        continue;
      }
      Map mtmp = (Map) CoreUtil.from_json(json);
      if (mtmp == null) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed to deserilaize " + componentId);
        sb.append(" json configuration: ");
        sb.append(json);
        LOG.info(sb.toString());
        throw new Exception(sb.toString());
      }
      Object componentKryoRegister = mtmp.get(Config.TOPOLOGY_KRYO_REGISTER);
      if (componentKryoRegister != null) {
        LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
            + ", componentId:" + componentId + ", TOPOLOGY_KRYO_REGISTER"
            + componentKryoRegister.getClass().getName());
        CoreUtil.mergeList(kryoRegisterList, componentKryoRegister);
      }
      Object componentDecorator = mtmp.get(Config.TOPOLOGY_KRYO_DECORATORS);
      if (componentDecorator != null) {
        LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
            + ", componentId:" + componentId + ", TOPOLOGY_KRYO_DECORATOR"
            + componentDecorator.getClass().getName());
        CoreUtil.mergeList(kryoDecoratorList, componentDecorator);
      }
    }

    /**
     * topology level serialization registrations take priority that way, if
     * there's a conflict, a user can force which serialization to use append
     * component conf to storm-conf
     */

    Map totalConf = new HashMap();
    totalConf.putAll(conf);
    totalConf.putAll(stormConf);
    Object totalRegister = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
    if (totalRegister != null) {
      LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
          + ", TOPOLOGY_KRYO_REGISTER" + totalRegister.getClass().getName());
      CoreUtil.mergeList(kryoRegisterList, totalRegister);
    }
    Object totalDecorator = totalConf.get(Config.TOPOLOGY_KRYO_DECORATORS);
    if (totalDecorator != null) {
      LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
          + ", TOPOLOGY_KRYO_DECORATOR" + totalDecorator.getClass().getName());
      CoreUtil.mergeList(kryoDecoratorList, totalDecorator);
    }

    Map kryoRegisterMap = mapifySerializations(kryoRegisterList);
    List decoratorList = CoreUtil.distinctList(kryoDecoratorList);
    Map rtn = new HashMap();
    rtn.putAll(stormConf);
    rtn.put(Config.TOPOLOGY_KRYO_DECORATORS, decoratorList);
    rtn.put(Config.TOPOLOGY_KRYO_REGISTER, kryoRegisterMap);
    rtn.put(Config.TOPOLOGY_ACKER_EXECUTORS,
        totalConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
    rtn.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS,
        totalConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS));
    rtn.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,
        totalConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
    return rtn;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#component-parallelism")
  public static Integer componentParallelism(Map stormConf,
      ComponentCommon component) {
    Map mergeMap = new HashMap();
    mergeMap.putAll(stormConf);
    String jsonConfString = component.get_json_conf();
    if (jsonConfString != null) {
      Map componentMap = (Map) CoreUtil.from_json(jsonConfString);
      mergeMap.putAll(componentMap);
    }
    Integer taskNum = null;
    Object taskNumObject = mergeMap.get(Config.TOPOLOGY_TASKS);
    if (taskNumObject != null) {
      taskNum = Utils.getInt(taskNumObject);
    } else {
      taskNum = component.is_set_parallelism_hint()
          ? component.get_parallelism_hint() : Integer.valueOf(1);
    }
    Object maxParallelismObject =
        mergeMap.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
    if (maxParallelismObject == null) {
      return taskNum;
    } else {
      int maxParallelism = Utils.getInt(maxParallelismObject);
      return Math.min(maxParallelism, taskNum);
    }
  }

  public static StormTopology optimizeTopology(
      StormTopology normalizedTopology) {
    // TODO
    // create new topology by collapsing bolts into CompoundSpout
    // and CompoundBolt
    // need to somehow maintain stream/component ids inside tuples
    return normalizedTopology;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#normalize-topology")
  public static StormTopology normalizeTopology(Map stormConf,
      StormTopology topology) {
    StormTopology ret = topology.deepCopy();
    Map<String, Object> components = StormCommon.allComponents(ret);
    for (Entry<String, Object> entry : components.entrySet()) {
      Object component = entry.getValue();
      ComponentCommon common = null;
      if (component instanceof Bolt) {
        common = ((Bolt) component).get_common();
      }
      if (component instanceof SpoutSpec) {
        common = ((SpoutSpec) component).get_common();
      }
      if (component instanceof StateSpoutSpec) {
        common = ((StateSpoutSpec) component).get_common();
      }
      Map componentConf = new HashMap();
      String jsonConfString = common.get_json_conf();
      if (jsonConfString != null) {
        componentConf.putAll((Map) CoreUtil.from_json(jsonConfString));
      }
      Integer taskNum = componentParallelism(stormConf, common);
      componentConf.put(Config.TOPOLOGY_TASKS, taskNum);

      common.set_json_conf(JSONValue.toJSONString(componentConf));
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#code-ids")
  public static Set<String> codeIds(BlobStore blobStore) throws IOException {
    KeyFilter filter = new KeyFilter() {
      @Override
      public Object filter(String key) {
        return ConfigUtils.getIdFromBlobKey(key);
      }
    };
    return blobStore.filterAndListKeys(filter);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#cleanup-storm-ids")
  public static Set<String> cleanupStormIds(Map conf,
      IStormClusterState stormClusterState, BlobStore blobStore)
          throws Exception {
    List<String> heartbeatIds = stormClusterState.heartbeatStorms();
    List<String> errorIds = stormClusterState.errorTopologies();
    Set<String> codeIds = codeIds(blobStore);
    List<String> assignedIds = stormClusterState.activeStorms();
    Set<String> toCleanUpIds = new HashSet<String>();
    if (heartbeatIds != null) {
      toCleanUpIds.addAll(heartbeatIds);
    }
    if (errorIds != null) {
      toCleanUpIds.addAll(errorIds);
    }
    if (codeIds != null) {
      toCleanUpIds.addAll(codeIds);
    }
    if (assignedIds != null) {
      toCleanUpIds.removeAll(assignedIds);
    }
    return toCleanUpIds;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#extract-status-str")
  public static String extractStatusStr(StormBase base) {
    TopologyStatus t = base.get_status();
    return t.name().toUpperCase();
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#blob-rm-key")
  public static void blobRmKey(BlobStore blobStore, String key,
      IStormClusterState stormClusterState) {
    try {
      blobStore.deleteBlob(key, nimbusSubject());
      if (blobStore instanceof LocalFsBlobStore) {
        stormClusterState.removeBlobstoreKey(key);
      }
    } catch (Exception e) {
      LOG.info("Exception", e);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#blob-rm-topology-keys")
  public static void blobRmTopologyKeys(String id, BlobStore blobStore,
      IStormClusterState stormClusterState) {
    blobRmKey(blobStore, ConfigUtils.masterStormJarKey(id), stormClusterState);
    blobRmKey(blobStore, ConfigUtils.masterStormConfKey(id), stormClusterState);
    blobRmKey(blobStore, ConfigUtils.masterStormCodeKey(id), stormClusterState);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#do-cleanup")
  public static void doCleanup(NimbusData data) throws Exception {
    if (isLeader(data, false)) {
      IStormClusterState clusterState = data.getStormClusterState();

      Set<String> toCleanupIds = new HashSet<String>();
      synchronized (data.getSubmitLock()) {
        toCleanupIds.addAll(cleanupStormIds(data.getConf(),
            data.getStormClusterState(), data.getBlobStore()));
      }

      for (String id : toCleanupIds) {
        LOG.info("Cleaning up " + id);
        clusterState.teardownHeartbeats(id);
        clusterState.teardownTopologyErrors(id);
        clusterState.removeBackpressure(id);

        String masterStormdistRoot =
            ConfigUtils.masterStormDistRoot(data.getConf(), id);
        try {
          Utils.forceDelete(masterStormdistRoot);
        } catch (IOException e) {
          LOG.error("[cleanup for" + id + " ]Failed to delete "
              + masterStormdistRoot + ",", CoreUtil.stringifyError(e));
        }
        blobRmTopologyKeys(id, data.getBlobStore(), clusterState);
        data.getExecutorHeartbeatsCache().remove(id);
      }
    } else {
      LOG.info("not a leader, skipping cleanup");
    }
  }

  /**
   * Deletes jar files in dir older than seconds.
   * 
   * @param dirLocation
   * @param seconds
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#clean-inbox")
  public static void cleanInbox(String dirLocation, int seconds) {
    File inboxdir = new File(dirLocation);
    OlderFileFilter filter = new OlderFileFilter(seconds);
    File[] files = inboxdir.listFiles(filter);
    for (File f : files) {
      if (f.delete()) {
        LOG.info("Cleaning inbox ... deleted: " + f.getName());
      } else {
        // This should never happen
        LOG.error("Cleaning inbox ... error deleting: " + f.getName());
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#cleanup-corrupt-topologies!")
  public static void cleanupCorruptTopologies(NimbusData nimbusData)
      throws Exception {
    if (NimbusUtils.isLeader(nimbusData, false)) {
      IStormClusterState stormClusterState = nimbusData.getStormClusterState();
      Set<String> code_ids = codeIds(nimbusData.getBlobStore());
      // get topology in ZK /storms
      List<String> active_ids =
          nimbusData.getStormClusterState().activeStorms();
      if (active_ids != null && active_ids.size() > 0) {
        if (code_ids != null) {
          // clean the topology which is in ZK but not in local dir
          active_ids.removeAll(code_ids);
        }
        for (String corrupt : active_ids) {
          LOG.info("Corrupt topology " + corrupt
              + " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...");
          // Just removing the /STORMS is enough
          stormClusterState.removeStorm(corrupt);
          if (nimbusData.getBlobStore() instanceof LocalFsBlobStore) {
            List<String> keyList =
                getKeyListFromId(nimbusData.getConf(), corrupt);
            for (String blobKey : keyList) {
              stormClusterState.removeBlobstoreKey(blobKey);
            }
          }
        }
      }
      LOG.info("Successfully cleanup all old toplogies");
    } else {
      LOG.info("not a leader, skipping cleanup Corrupt Topologies");
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-errors")
  public static List<ErrorInfo> getErrors(IStormClusterState stormClusterState,
      String stormId, String componentId) throws Exception {
    return stormClusterState.errors(stormId, componentId);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-last-error")
  private static ErrorInfo getLastError(IStormClusterState stormClusterState,
      String stormId, String componentId) throws Exception {
    return stormClusterState.lastError(stormId, componentId);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyInfoWithOpts#errors-fn")
  public static List<ErrorInfo> errorsFn(NumErrorsChoice numErrChoice,
      IStormClusterState stormClusterState, String stormId, String componentId)
          throws Exception {
    List<ErrorInfo> ret = new ArrayList<ErrorInfo>();
    if (numErrChoice.equals(NumErrorsChoice.NONE)) {
      // empty list only
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ONE)) {
      ErrorInfo errorInfo =
          getLastError(stormClusterState, stormId, componentId);
      if (errorInfo != null && !StringUtils.isEmpty(errorInfo.get_error())) {
        ret.add(errorInfo);
      }
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ALL)) {
      ret = getErrors(stormClusterState, stormId, componentId);
      return ret;
    } else {
      LOG.warn("Got invalid NumErrorsChoice '" + numErrChoice + "'");
      ret = getErrors(stormClusterState, stormId, componentId);
      return ret;
    }
  }

  @ClojureClass(className = " add form http://git.code.oa.com/trc/jstorm/issues/73")
  public static List<ErrorInfo> errorsFnByStormId(NumErrorsChoice numErrChoice,
      String componentId, Map<String, List<ErrorInfo>> componentToErrors,
      Map<String, ErrorInfo> componentToLastError) throws Exception {
    List<ErrorInfo> ret = new ArrayList<ErrorInfo>();
    if (numErrChoice.equals(NumErrorsChoice.NONE)) {
      // empty list only
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ONE)) {
      ErrorInfo errorInfo = componentToLastError.get(componentId);
      if (errorInfo != null && !StringUtils.isEmpty(errorInfo.get_error())) {
        ret.add(errorInfo);
      }
      return ret;
    } else if (numErrChoice.equals(NumErrorsChoice.ALL)) {
      ret = componentToErrors.get(componentId) == null
          ? new ArrayList<ErrorInfo>() : componentToErrors.get(componentId);
      return ret;
    } else {
      LOG.warn("Got invalid NumErrorsChoice '" + numErrChoice + "'");
      ret = componentToErrors.get(componentId) == null
          ? new ArrayList<ErrorInfo>() : componentToErrors.get(componentId);
      return ret;
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#transition-name!")
  public static <T> void transitionName(NimbusData nimbusData, String stormName,
      boolean errorOnNoTransition, TopologyStatus transitionStatus, T... args)
          throws Exception {
    IStormClusterState stormClusterState = nimbusData.getStormClusterState();
    String stormId = StormCommon.getStormId(stormClusterState, stormName);
    if (stormId == null) {
      throw new NotAliveException(stormName);
    }
    transition(nimbusData, stormId, errorOnNoTransition, transitionStatus,
        args);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#transition!")
  public static <T> void transition(NimbusData nimbusData, String stormId,
      TopologyStatus transitionStatus, T... args) throws Exception {
    transition(nimbusData, stormId, false, transitionStatus, args);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#transition!")
  public static <T> void transition(NimbusData nimbusData, String stormId,
      boolean errorOnNoTransition, TopologyStatus transitionStatus, T... args)
          throws Exception {
    nimbusData.getStatusTransition().transition(stormId, errorOnNoTransition,
        transitionStatus, args);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#mk-scheduler")
  public static IScheduler mkScheduler(Map conf, INimbus inimbus) {
    IScheduler forcedScheduler = inimbus.getForcedScheduler();
    IScheduler scheduler = null;
    if (forcedScheduler != null) {
      LOG.info("Using forced scheduler from INimbus "
          + inimbus.getForcedScheduler().getClass().getName());
      scheduler = forcedScheduler;
    } else if (conf.get(Config.STORM_SCHEDULER) != null) {
      String custormScheduler = (String) conf.get(Config.STORM_SCHEDULER);
      LOG.info("Using custom scheduler: " + custormScheduler);
      try {
        scheduler = (IScheduler) ReflectionUtils.newInstance(custormScheduler);

      } catch (Exception e) {
        LOG.error(custormScheduler
            + " : Scheduler initialize error! Using default scheduler", e);
        scheduler = new DefaultScheduler();
      }
    } else {
      LOG.info("Using default scheduler {}", DefaultScheduler.class.getName());
      scheduler = new DefaultScheduler();
    }
    scheduler.prepare(conf);
    return scheduler;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#NIMBUS-ZK-ACLS")
  public static List<ACL> nimbusZKAcls() {
    ACL first = ZooDefs.Ids.CREATOR_ALL_ACL.get(0);
    return Arrays.asList(first,
        new ACL(ZooDefs.Perms.READ ^ ZooDefs.Perms.CREATE,
            ZooDefs.Ids.ANYONE_ID_UNSAFE));
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#nimbus-data#:validator")
  public static ITopologyValidator mkTopologyValidator(Map conf) {
    String topology_validator =
        (String) conf.get(Config.NIMBUS_TOPOLOGY_VALIDATOR);
    ITopologyValidator validator = null;
    if (topology_validator != null) {
      try {
        validator = ReflectionUtils.newInstance(topology_validator);
      } catch (ClassNotFoundException e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(
            "Fail to make configed NIMBUS_TOPOLOGY_VALIDATOR");
      }
    } else {
      validator = new DefaultTopologyValidator();
    }
    return validator;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getClusterInfo#topology-summaries")
  public static List<TopologySummary> mkTopologySummaries(NimbusData nimbus,
      IStormClusterState stormClusterState, Map<String, StormBase> bases)
          throws Exception {
    List<TopologySummary> topologySummaries = new ArrayList<TopologySummary>();

    for (Entry<String, StormBase> entry : bases.entrySet()) {
      String id = entry.getKey();
      StormBase base = entry.getValue();
      Assignment assignment = stormClusterState.assignmentInfo(id, null);
      TopologySummary topoSumm = null;
      if (assignment == null) {
        LOG.warn("Assignment of " + id + " is NULL!");
        topoSumm = new TopologySummary(id, base.get_name(), 0, 0, 0,
            Time.deltaSecs(base.get_launch_time_secs()),
            NimbusUtils.extractStatusStr(base));
      } else {
        Map<List<Long>, NodeInfo> executorToNodePort =
            assignment.get_executor_node_port();
        // num_tasks
        Set<List<Long>> executorInfoSet = executorToNodePort.keySet();
        Set<Integer> tasks = new HashSet<Integer>();
        for (List<Long> executorInfo : executorInfoSet) {
          List<Integer> taskIds = StormCommon.executorIdToTasks(executorInfo);
          tasks.addAll(taskIds);
        }
        int numTasks = tasks.size();
        // num_executors
        Integer numExecutors = executorToNodePort.keySet().size();
        // num_workers
        Set<NodeInfo> NodeInfos = new HashSet<NodeInfo>();
        for (NodeInfo NodeInfo : executorToNodePort.values()) {
          NodeInfos.add(NodeInfo);
        }
        Integer numWorkers = NodeInfos.size();
        topoSumm =
            new TopologySummary(id, base.get_name(), numTasks, numExecutors,
                numWorkers, Time.deltaSecs(base.get_launch_time_secs()),
                NimbusUtils.extractStatusStr(base));
        String owner = base.get_owner();
        if (owner != null) {
          topoSumm.set_owner(owner);
        }
        Map<String, String> idToSchedStatus = nimbus.getIdToSchedStatus();
        String schedStatus = idToSchedStatus.get(id);
        if (schedStatus != null) {
          topoSumm.set_sched_status(schedStatus);
        }
        Map<String, Double[]> idToResources = nimbus.getIdToResources();
        Double[] resources = idToResources.get(id);
        if (resources != null && resources.length >= 6) {
          topoSumm.set_requested_memonheap(resources[0]);
          topoSumm.set_requested_memoffheap(resources[1]);
          topoSumm.set_requested_cpu(resources[2]);
          topoSumm.set_assigned_memonheap(resources[3]);
          topoSumm.set_assigned_memoffheap(resources[4]);
          topoSumm.set_assigned_cpu(resources[5]);
        }
        Integer repliCount =
            getBlobReplicationCount(ConfigUtils.masterStormCodeKey(id), nimbus);
        if (repliCount != null) {
          topoSumm.set_replication_count(repliCount);
        }
      }
      topologySummaries.add(topoSumm);
    }
    return topologySummaries;
  }

  public static SupervisorSummary mkSupervisorSummary(
      SupervisorInfo supervisorInfo, String supervisorId) {
    SupervisorSummary summary = new SupervisorSummary(
        supervisorInfo.get_hostname(), (int) supervisorInfo.get_uptime_secs(),
        supervisorInfo.get_meta_size(), supervisorInfo.get_used_ports_size(),
        supervisorId);
    return summary;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getClusterInfo#supervisor-summaries")
  public static List<SupervisorSummary> mkSupervisorSummaries(
      NimbusData nimbusData, IStormClusterState stormClusterState,
      Map<String, SupervisorInfo> supervisorInfos) throws Exception {
    List<SupervisorSummary> ret = new ArrayList<SupervisorSummary>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String supervisorId = entry.getKey();
      SupervisorInfo supervisorInfo = entry.getValue();
      // int num_workers = 0;
      // TODO: need to get the port info about supervisors...
      // in standalone just look at metadata, otherwise just say N/A?
      SupervisorSummary summary = new SupervisorSummary(
          supervisorInfo.get_hostname(), (int) supervisorInfo.get_uptime_secs(),
          supervisorInfo.get_meta_size(), supervisorInfo.get_used_ports_size(),
          supervisorId);
      summary.set_total_resources(supervisorInfo.get_resources_map());
      Map<String, Double[]> nodeIdToResources =
          nimbusData.getNodeIdToResources();
      Double[] resources = nodeIdToResources.get(supervisorId);
      if (resources != null && resources.length >= 4) {
        summary.set_used_mem(resources[2]);
        summary.set_used_cpu(resources[3]);
      }
      String version = supervisorInfo.get_version();
      if (version != null) {
        summary.set_version(version);
      }
      ret.add(summary);
    }
    Collections.sort(ret, new Comparator<SupervisorSummary>() {
      @Override
      public int compare(SupervisorSummary o1, SupervisorSummary o2) {
        return o1.get_host().compareTo(o2.get_host());
      }
    });
    return ret;
  }

  public static ExecutorSummary mkSimpleExecutorSummary(NodeInfo resource,
      int taskId, String component, String host, int uptime) {
    ExecutorSummary ret = new ExecutorSummary();
    ExecutorInfo executor_info = new ExecutorInfo(taskId, taskId);
    ret.set_executor_info(executor_info);
    ret.set_component_id(component);
    ret.set_host(host);
    ret.set_port(resource.get_port_iterator().next().intValue());
    ret.set_uptime_secs(uptime);
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#getTopologyInfo#executor-summaries")
  public static List<ExecutorSummary> mkExecutorSummaries(
      IStormClusterState stormClusterState, Assignment assignment,
      Map<Integer, String> taskToComponent, String topologyId)
          throws Exception {
    List<ExecutorSummary> taskSummaries = new ArrayList<ExecutorSummary>();
    if (null != assignment) {
      Map<List<Long>, NodeInfo> executorToNodePort =
          assignment.get_executor_node_port();
      Map<ExecutorInfo, ExecutorBeat> beats =
          stormClusterState.executorBeats(topologyId, executorToNodePort);
      for (Entry<List<Long>, NodeInfo> entry : executorToNodePort.entrySet()) {
        List<Long> executor = entry.getKey();
        ExecutorInfo executorInfo = new ExecutorInfo(executor.get(0).intValue(),
            executor.get(executor.size() - 1).intValue());
        NodeInfo nodeToPort = entry.getValue();
        Integer port = nodeToPort.get_port_iterator().next().intValue();
        String host = assignment.get_node_host().get(nodeToPort.get_node());
        ExecutorBeat heartbeat = beats.get(executorInfo);
        if (heartbeat == null) {
          LOG.warn("Topology " + topologyId + " Executor " + executor.toString()
              + " hasn't been started");
          continue;
        }
        // Counters counters = heartbeat.getCounters();
        ExecutorStats stats = heartbeat.getStats();
        String componentId = taskToComponent.get(executor.get(0));
        ExecutorSummary executorSummary = new ExecutorSummary(executorInfo,
            componentId, host, port, Utils.getInt(heartbeat.getUptime(), 0));
        executorSummary.set_stats(stats);
        taskSummaries.add(executorSummary);
      }
    }
    return taskSummaries;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-storm-conf")
  public static Map readStormConf(Map conf, String stormId, BlobStore blobStore)
      throws Exception {
    Map<String, Object> ret = Utils.fromCompressedJsonConf(blobStore
        .readBlob(ConfigUtils.masterStormConfKey(stormId), getSubject()));
    if (ret != null) {
      return ret;
    }
    return new HashMap<String, Object>();
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#DISALLOWED-TOPOLOGY-NAME-STRS")
  public static final String DISALLOWED_TOPOLOYG_NAME_STRS[] =
      { "@", "/", ".", ":", "\\" };

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#validate-topology-name!")
  public static void ValidTopologyName(String name)
      throws InvalidTopologyException {
    if (StringUtils.isBlank(name)) {
      throw new InvalidTopologyException("Topology name cannot be blank");
    }
    for (String invalidStr : DISALLOWED_TOPOLOYG_NAME_STRS) {
      if (name.contains(invalidStr)) {
        throw new InvalidTopologyException(
            "Topology name cannot contain any of the following: " + invalidStr);
      }
    }
  }

  /**
   * We will only file at storm_dist_root/topology_id/file to be accessed via
   * Thrift ex., storm-local/nimbus/stormdist/aa-1-1377104853/stormjar.jar
   * 
   * @param conf
   * @param filePath
   * @throws AuthorizationException
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#check-file-access")
  public static void checkFileAccess(Map conf, String filePath)
      throws AuthorizationException {
    LOG.debug("check file access:" + filePath);

    try {
      if (new File(ConfigUtils.masterStormDistRoot(conf))
          .getCanonicalFile() != new File(filePath).getCanonicalFile()
              .getParentFile().getParentFile()) {
        throw new AuthorizationException("Invalid file path: " + filePath);
      }

    } catch (Exception e) {
      throw new AuthorizationException("Invalid file path: " + filePath);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#try-read-storm-conf")
  public static Map tryReadStormConf(Map conf, String stormId,
      BlobStore blobStore)
          throws NotAliveException, AuthorizationException, IOException {
    try {
      return readStormConfAsNimbus(stormId, blobStore);
    } catch (KeyNotFoundException e) {
      throw new NotAliveException(stormId == null ? "" : stormId);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-storm-conf-as-nimbus")
  public static Map readStormConfAsNimbus(String stormId, BlobStore blobStore)
      throws KeyNotFoundException, AuthorizationException, IOException {
    return Utils.fromCompressedJsonConf(blobStore
        .readBlob(ConfigUtils.masterStormConfKey(stormId), nimbusSubject()));
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#try-read-storm-conf-from-name")
  public static Map tryReadStormConfFromName(Map conf, String stormName,
      NimbusData nimbus) throws Exception {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    BlobStore blobStore = nimbus.getBlobStore();
    String id = StormCommon.getStormId(stormClusterState, stormName);
    return tryReadStormConf(conf, id, blobStore);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-storm-topology-as-nimbus")
  public static StormTopology readStormTopologyAsNimbus(String stormId,
      BlobStore blobStore)
          throws KeyNotFoundException, AuthorizationException, IOException {
    return Utils.deserialize(blobStore
        .readBlob(ConfigUtils.masterStormCodeKey(stormId), nimbusSubject()),
        StormTopology.class);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#try-read-storm-topology")
  public static StormTopology tryReadStormTopology(String stormId,
      BlobStore blobStore)
          throws NotAliveException, AuthorizationException, IOException {
    try {
      return readStormTopologyAsNimbus(stormId, blobStore);
    } catch (KeyNotFoundException e) {
      throw new NotAliveException(stormId);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-executors")
  private static List<ExecutorInfo> computeExecutors(NimbusData nimbusData,
      String stormId) throws Exception {
    // Map conf = nimbusData.getConf();
    StormBase stormBase =
        nimbusData.getStormClusterState().stormBase(stormId, null);
    // componentToExecutors={count=12, __acker=3, spout=5, __system=0,
    // split=8}
    List<ExecutorInfo> executors = new ArrayList<ExecutorInfo>();
    if (null == stormBase) {
      return executors;
    }
    Map<String, Integer> componentToExecutors =
        stormBase.get_component_executors();

    Map stormConf = readStormConfAsNimbus(stormId, nimbusData.getBlobStore());
    StormTopology topology =
        readStormTopologyAsNimbus(stormId, nimbusData.getBlobStore());

    Map<Integer, String> taskIdToComponent =
        StormCommon.stormTaskInfo(topology, stormConf);
    Map<String, List<Integer>> componentToTaskIds =
        Utils.reverseMap(taskIdToComponent);

    for (Entry<String, List<Integer>> entry : componentToTaskIds.entrySet()) {
      List<Integer> taskIds = entry.getValue();
      Collections.sort(taskIds);
    }

    // join-maps
    Set<String> allKeys = new HashSet<String>();
    allKeys.addAll(componentToExecutors.keySet());
    allKeys.addAll(componentToTaskIds.keySet());

    Map<String, Map<Integer, List<Integer>>> componentIdToExecutorNumsToTaskids =
        new HashMap<String, Map<Integer, List<Integer>>>();
    for (String componentId : allKeys) {
      if (componentToExecutors.containsKey(componentId)) {
        Map<Integer, List<Integer>> executorNumsToTaskIds =
            new HashMap<Integer, List<Integer>>();
        executorNumsToTaskIds.put(componentToExecutors.get(componentId),
            componentToTaskIds.get(componentId));
        componentIdToExecutorNumsToTaskids.put(componentId,
            executorNumsToTaskIds);
      }
    }

    List<List<Integer>> ret = new ArrayList<List<Integer>>();
    for (String componentId : componentIdToExecutorNumsToTaskids.keySet()) {
      Map<Integer, List<Integer>> executorNumToTaskIds =
          componentIdToExecutorNumsToTaskids.get(componentId);
      for (Integer count : executorNumToTaskIds.keySet()) {
        List<List<Integer>> partitionedList = Utils
            .partitionFixed(count.intValue(), executorNumToTaskIds.get(count));
        if (partitionedList != null) {
          ret.addAll(partitionedList);
        }
      }
    }

    for (List<Integer> taskIds : ret) {
      if (taskIds != null && !taskIds.isEmpty()) {
        executors.add(toExecutorId(taskIds));
      }
    }
    return executors;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#to-executor-id")
  private static ExecutorInfo toExecutorId(List<Integer> taskIds) {
    return new ExecutorInfo(taskIds.get(0), taskIds.get(taskIds.size() - 1));

  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-executor->component")
  private static Map<ExecutorInfo, String> computeExecutorToComponent(
      NimbusData nimbusData, String stormId, StormTopology topology,
      Map topologyConf) throws Exception {
    List<ExecutorInfo> executors = computeExecutors(nimbusData, stormId);
    Map<Integer, String> taskToComponent =
        StormCommon.stormTaskInfo(topology, topologyConf);
    Map<ExecutorInfo, String> executorToComponent =
        new HashMap<ExecutorInfo, String>();
    for (ExecutorInfo executor : executors) {
      Integer startTask = executor.get_task_start();
      String component = taskToComponent.get(startTask);
      executorToComponent.put(executor, component);
    }
    return executorToComponent;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-topology-details")
  private static TopologyDetails readTopologyDetails(NimbusData nimbusData,
      String stormId) throws Exception {
    StormBase stormBase =
        nimbusData.getStormClusterState().stormBase(stormId, null);
    if (stormBase == null) {
      throw new NotAliveException(stormId);
    }
    Map topologyConf =
        NimbusUtils.readStormConfAsNimbus(stormId, nimbusData.getBlobStore());
    StormTopology topology =
        readStormTopologyAsNimbus(stormId, nimbusData.getBlobStore());
    Map<ExecutorInfo, String> executorInfoToComponent =
        computeExecutorToComponent(nimbusData, stormId, topology, topologyConf);
    Map<ExecutorDetails, String> executorToComponent =
        new HashMap<ExecutorDetails, String>();
    for (ExecutorInfo executor : executorInfoToComponent.keySet()) {
      int startTask = executor.get_task_start();
      int endTask = executor.get_task_end();
      ExecutorDetails detail = new ExecutorDetails(startTask, endTask);
      executorToComponent.put(detail, executorInfoToComponent.get(executor));
    }
    LOG.debug("New StormBase : " + stormBase.toString());
    TopologyDetails details = new TopologyDetails(stormId, topologyConf,
        topology, stormBase.get_num_workers(), executorToComponent,
        stormBase.get_launch_time_secs());
    return details;
  }

  /**
   * compute a topology-id -> executors map
   * 
   * @param nimbusData
   * @param stormIds
   * @return a topology-id -> executors map
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-topology->executors")
  private static Map<String, Set<ExecutorInfo>> computeTopologyToExecutors(
      NimbusData nimbusData, Set<String> stormIds) throws Exception {
    Map<String, Set<ExecutorInfo>> ret =
        new HashMap<String, Set<ExecutorInfo>>();

    for (String tid : stormIds) {
      List<ExecutorInfo> executors = computeExecutors(nimbusData, tid);
      if (executors.size() > 0) {
        Set<ExecutorInfo> executorSet = new HashSet<ExecutorInfo>(executors);
        ret.put(tid, executorSet);
      }
    }
    return ret;
  }

  /**
   * Does not assume that clocks are synchronized. Executor heartbeat is only
   * used so that nimbus knows when it's received a new heartbeat. All timing is
   * done by nimbus and tracked through heartbeat-cache
   * 
   * @param curr
   * @param hb
   * @param nimTaskTimeOutSecs
   * @return
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#update-executor-cache")
  private static ExecutorCache updateExecutorCache(ExecutorCache curr,
      ExecutorBeat hb, int nimTaskTimeOutSecs) {
    int reportedTime = 0;
    if (hb != null) {
      reportedTime = hb.getTimeSecs();
    }
    int lastNimbusTime = 0;
    int lastReportedTime = 0;
    if (curr != null) {
      lastNimbusTime = curr.getNimbusTime();
      lastReportedTime = curr.getExecutorReportedTime();
    }
    int newReportedTime;
    if (reportedTime != 0) {
      newReportedTime = reportedTime;
    } else if (lastReportedTime != 0) {
      newReportedTime = lastReportedTime;
    } else {
      newReportedTime = 0;
    }
    int nimbusTime;
    if (lastNimbusTime == 0 || lastReportedTime != newReportedTime) {
      nimbusTime = Time.currentTimeSecs();
    } else {
      nimbusTime = lastNimbusTime;
    }
    //
    boolean isTimeOut =
        (0 != nimbusTime && Time.deltaSecs(nimbusTime) >= nimTaskTimeOutSecs);

    return new ExecutorCache(isTimeOut, nimbusTime, newReportedTime, hb);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#update-heartbeat-cache")
  public static Map<ExecutorInfo, ExecutorCache> updateHeartbeatCache(
      Map<ExecutorInfo, ExecutorCache> cache,
      Map<ExecutorInfo, ExecutorBeat> executorBeats,
      Set<ExecutorInfo> allExecutors, int nimTaskTimeOutSecs) {
    Map<ExecutorInfo, ExecutorCache> selectedCache =
        new HashMap<ExecutorInfo, ExecutorCache>();
    if (cache != null) {
      for (Entry<ExecutorInfo, ExecutorCache> entry : cache.entrySet()) {
        ExecutorInfo executorInfo = entry.getKey();
        ExecutorCache executorCache = entry.getValue();
        if (allExecutors.contains(executorInfo)) {
          selectedCache.put(executorInfo, executorCache);
        }
      }
    }
    Map<ExecutorInfo, ExecutorCache> ret =
        new HashMap<ExecutorInfo, ExecutorCache>();
    for (ExecutorInfo executorInfo : allExecutors) {
      ExecutorCache curr = selectedCache.get(executorInfo);
      ret.put(executorInfo, updateExecutorCache(curr,
          executorBeats.get(executorInfo), nimTaskTimeOutSecs));
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#update-heartbeats!")
  public static void updateHeartbeats(NimbusData nimbus, String stormId,
      Set<ExecutorInfo> allExecutors, Assignment existingAssignment)
          throws Exception {
    LOG.debug("Updating heartbeats for {}:{}", stormId,
        allExecutors.toString());
    IStormClusterState stormClusterState = nimbus.getStormClusterState();

    // TODO
    Map<ExecutorInfo, ExecutorBeat> executorBeats = stormClusterState
        .executorBeats(stormId, existingAssignment.get_executor_node_port());

    Map<ExecutorInfo, ExecutorCache> cache =
        nimbus.getExecutorHeartbeatsCache().get(stormId);

    Map conf = nimbus.getConf();
    int taskTimeOutSecs =
        Utils.getInt(conf.get(Config.NIMBUS_TASK_TIMEOUT_SECS), 30);
    Map<ExecutorInfo, ExecutorCache> newCache = updateHeartbeatCache(cache,
        executorBeats, allExecutors, taskTimeOutSecs);

    ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorCache =
        nimbus.getExecutorHeartbeatsCache();
    executorCache.put(stormId, newCache);
    nimbus.setExecutorHeartbeatsCache(executorCache);
  }

  /**
   * update all the heartbeats for all the topologies's executors
   * 
   * @param nimbus
   * @param existingAssignments
   * @param topologyToExecutors
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#update-all-heartbeats!")
  private static void updateAllHeartbeats(NimbusData nimbus,
      Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors) throws Exception {
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      updateHeartbeats(nimbus, tid, allExecutors, assignment);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#alive-executors")
  private static Set<ExecutorInfo> aliveExecutors(NimbusData nimbus,
      TopologyDetails topologyDetails, Set<ExecutorInfo> allExecutors,
      Assignment existingAssignment) {
    LOG.debug("Computing alive executors for " + topologyDetails.getId() + "\n"
        + "Executors: " + allExecutors.toString() + "\n" + "Assignment: "
        + existingAssignment.toString() + "\n" + "Heartbeat cache: "
        + nimbus.getExecutorHeartbeatsCache().get(topologyDetails.getId()));

    // TODO: need to consider all executors associated with a dead executor
    // (in same slot) dead as well, don't just rely on heartbeat being the
    // same
    Map conf = nimbus.getConf();
    String stormId = topologyDetails.getId();
    Set<ExecutorInfo> allAliveExecutors = new HashSet<ExecutorInfo>();
    Map<List<Long>, Long> executorStartTimes =
        existingAssignment.get_executor_start_time_secs();
    Map<ExecutorInfo, Long> executorInfoStartTimes =
        new HashMap<ExecutorInfo, Long>();
    for (Entry<List<Long>, Long> e : executorStartTimes.entrySet()) {
      ExecutorInfo executorInfo = new ExecutorInfo(e.getKey().get(0).intValue(),
          e.getKey().get(e.getKey().size() - 1).intValue());
      executorInfoStartTimes.put(executorInfo, e.getValue());
    }
    Map<ExecutorInfo, ExecutorCache> heartbeatsCache =
        nimbus.getExecutorHeartbeatsCache().get(stormId);
    int taskLaunchSecs =
        Utils.getInt(conf.get(Config.NIMBUS_TASK_LAUNCH_SECS), 30);

    for (ExecutorInfo executor : allExecutors) {
      Integer startTime = executorInfoStartTimes.get(executor).intValue();
      boolean isTaskLaunchNotTimeOut =
          Time.deltaSecs(null != startTime ? startTime : 0) < taskLaunchSecs;
      //
      boolean isExeTimeOut = heartbeatsCache.get(executor).isTimedOut();

      if (startTime != null && (isTaskLaunchNotTimeOut || !isExeTimeOut)) {
        allAliveExecutors.add(executor);
      } else {
        LOG.info("Executor {}:{} not alive", stormId, executor.toString());
      }
    }
    return allAliveExecutors;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-topology->alive-executors")
  private static Map<String, Set<ExecutorInfo>> computeTopologyToAliveExecutors(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Topologies topologies, Map<String, Set<ExecutorInfo>> topologyToExecutors,
      String scratchTopologyID) {
    // "compute a topology-id -> alive executors map"
    Map<String, Set<ExecutorInfo>> ret =
        new HashMap<String, Set<ExecutorInfo>>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      TopologyDetails topologyDetails = topologies.getById(tid);
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      Set<ExecutorInfo> aliveExecutors = new HashSet<ExecutorInfo>();
      if (scratchTopologyID != null && scratchTopologyID == tid) {
        // TODO never execute this code, see mkExistingAssignments
        aliveExecutors = allExecutors;
      } else {
        aliveExecutors = aliveExecutors(nimbusData, topologyDetails,
            allExecutors, assignment);
      }
      ret.put(tid, aliveExecutors);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-supervisor->dead-ports")
  public static Map<String, Set<Integer>> computeSupervisorToDeadports(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {
    Set<NodeInfo> deadSlots = filterDeadSlots(existingAssignments,
        topologyToExecutors, topologyToAliveExecutors);

    Map<String, Set<Integer>> supervisorToDeadports =
        new HashMap<String, Set<Integer>>();
    for (NodeInfo NodeInfo : deadSlots) {
      String nodeId = NodeInfo.get_node();
      Integer port = NodeInfo.get_port_iterator().next().intValue();
      if (supervisorToDeadports.containsKey(nodeId)) {
        Set<Integer> deadportsList = supervisorToDeadports.get(nodeId);
        deadportsList.add(port);
      } else {
        Set<Integer> deadportsList = new HashSet<Integer>();
        deadportsList.add(port);
        supervisorToDeadports.put(nodeId, deadportsList);
      }
    }
    return supervisorToDeadports;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-supervisor->dead-ports#dead-slots")
  private static Set<NodeInfo> filterDeadSlots(
      Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {
    Set<NodeInfo> deadSlots = new HashSet<NodeInfo>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> allExecutors = topologyToExecutors.get(tid);
      Set<ExecutorInfo> aliveExecutors = topologyToAliveExecutors.get(tid);
      Set<ExecutorInfo> deadExecutors =
          CoreUtil.set_difference(allExecutors, aliveExecutors);

      Map<List<Long>, NodeInfo> allExecutorToNodeport =
          assignment.get_executor_node_port();
      for (Entry<List<Long>, NodeInfo> item : allExecutorToNodeport
          .entrySet()) {
        List<Long> executor = item.getKey();
        ExecutorInfo executorInfo = new ExecutorInfo(executor.get(0).intValue(),
            executor.get(executor.size() - 1).intValue());
        NodeInfo deadSlot = item.getValue();
        if (deadExecutors.contains(executorInfo)) {
          deadSlots.add(deadSlot);
        }
      }
    }
    return deadSlots;
  }

  /**
   * convert assignment information in zk to SchedulerAssignment, so it can be
   * used by scheduler api.
   * 
   * @param nimbusData
   * @param existingAssignments
   * @param topologyToAliveExecutors
   * @return
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-topology->scheduler-assignment")
  private static Map<String, SchedulerAssignmentImpl> computeTopologyToSchedulerAssignment(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors) {

    Map<String, SchedulerAssignmentImpl> ret =
        new HashMap<String, SchedulerAssignmentImpl>();
    for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
      String tid = entry.getKey();
      Assignment assignment = entry.getValue();
      Set<ExecutorInfo> aliveExecutors = topologyToAliveExecutors.get(tid);
      Map<List<Long>, NodeInfo> executorToNodeport =
          assignment.get_executor_node_port();
      Map<NodeInfo, WorkerResources> workerToResources =
          assignment.get_worker_resources();
      // making a map from node+port to WorkerSlot with allocated resources
      Map<NodeInfo, WorkerSlot> nodeInfoToWorkerSlot =
          new HashMap<NodeInfo, WorkerSlot>();
      for (Entry<NodeInfo, WorkerResources> entryNW : workerToResources
          .entrySet()) {
        NodeInfo NodeInfo = entryNW.getKey();
        WorkerResources workerResources = entryNW.getValue();
        WorkerSlot workerSlot = new WorkerSlot(NodeInfo.get_node(),
            NodeInfo.get_port_iterator().next().intValue(),
            workerResources.get_mem_on_heap(),
            workerResources.get_mem_off_heap(), workerResources.get_cpu());
        nodeInfoToWorkerSlot.put(NodeInfo, workerSlot);
      }

      Map<ExecutorDetails, WorkerSlot> executorToSlot =
          new HashMap<ExecutorDetails, WorkerSlot>();

      for (Entry<List<Long>, NodeInfo> item : executorToNodeport.entrySet()) {
        List<Long> executor = item.getKey();
        ExecutorInfo executorInfo = new ExecutorInfo(executor.get(0).intValue(),
            executor.get(executor.size() - 1).intValue());
        if (aliveExecutors.contains(executorInfo)) {
          ExecutorDetails executorDetails =
              new ExecutorDetails(executor.get(0).intValue(),
                  executor.get(executor.size() - 1).intValue());
          NodeInfo NodeInfo = item.getValue();
          executorToSlot.put(executorDetails,
              nodeInfoToWorkerSlot.get(NodeInfo));
        }
      }

      ret.put(tid, new SchedulerAssignmentImpl(tid, executorToSlot));
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#num-used-workers")
  public static int numUsedWorkers(SchedulerAssignment schedulerAssignment) {
    if (schedulerAssignment != null) {
      return schedulerAssignment.getSlots().size();
    }
    return 0;
  }

  /**
   * convert {topology-id -> SchedulerAssignment} to {topology-id -> {[node
   * port] [mem-on-heap mem-off-heap cpu]}} Make sure this can deal with other
   * non-RAS schedulers later we may further support map-for-any-resources
   *
   * @param newSchedulerAssignments
   * @return
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#convert-assignments-to-worker->resources")
  public static Map<String, Map<NodeInfo, WorkerResources>> convertAssignmentsToWorkerToResources(
      Map<String, SchedulerAssignment> newSchedulerAssignments) {
    Map<String, Map<NodeInfo, WorkerResources>> ret =
        new HashMap<String, Map<NodeInfo, WorkerResources>>();
    for (Entry<String, SchedulerAssignment> entry : newSchedulerAssignments
        .entrySet()) {
      SchedulerAssignment assignment = entry.getValue();
      Collection<WorkerSlot> workSlots =
          assignment.getExecutorToSlot().values();
      Map<NodeInfo, WorkerResources> workerToResources =
          new HashMap<NodeInfo, WorkerResources>();
      for (WorkerSlot slot : workSlots) {
        NodeInfo NodeInfo = new NodeInfo(slot.getNodeId(),
            Sets.newHashSet(Long.valueOf(slot.getPort())));
        WorkerResources resources = new WorkerResources();
        resources.set_mem_off_heap(slot.getAllocatedMemOffHeap());
        resources.set_mem_on_heap(slot.getAllocatedMemOnHeap());
        resources.set_cpu(slot.getAllocatedCpu());
        workerToResources.put(NodeInfo, resources);
      }
      ret.put(entry.getKey(), workerToResources);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#all-scheduling-slots")
  private static Collection<NodeInfo> allSchedulingSlots(NimbusData nimbusData,
      Topologies topologies, List<String> missingAssignmentTopologies)
          throws Exception {
    IStormClusterState stormClusterState = nimbusData.getStormClusterState();
    INimbus inimbus = nimbusData.getInimubs();

    Map<String, SupervisorInfo> supervisorInfos =
        allSupervisorInfo(stormClusterState);

    List<SupervisorDetails> supervisorDetails =
        new ArrayList<SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String id = entry.getKey();
      SupervisorInfo info = entry.getValue();
      supervisorDetails.add(
          new SupervisorDetails(id, info.get_meta(), info.get_resources_map()));
    }

    Collection<WorkerSlot> ret =
        inimbus.allSlotsAvailableForScheduling(supervisorDetails, topologies,
            new HashSet<String>(missingAssignmentTopologies));

    Collection<NodeInfo> result = new ArrayList<NodeInfo>();
    for (WorkerSlot ws : ret) {
      result.add(new NodeInfo(ws.getNodeId(),
          Sets.newHashSet(Long.valueOf(ws.getPort()))));
    }
    return result;
  }

  /**
   * 
   * @param nimbusData
   * @param allSchedulingSlots
   * @param supervisorToDeadPorts
   * @return return a map: {topology-id SupervisorDetails}
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-all-supervisor-details")
  private static Map<String, SupervisorDetails> readAllSupervisorDetails(
      NimbusData nimbusData, Map<String, Set<Integer>> allSchedulingSlots,
      Map<String, Set<Integer>> supervisorToDeadPorts) throws Exception {

    IStormClusterState stormClusterState = nimbusData.getStormClusterState();
    Map<String, SupervisorInfo> supervisorInfos =
        NimbusUtils.allSupervisorInfo(stormClusterState);

    Map<String, Set<Integer>> nonexistentSupervisorSlots =
        new HashMap<String, Set<Integer>>();
    nonexistentSupervisorSlots.putAll(allSchedulingSlots);
    for (String sid : supervisorInfos.keySet()) {
      if (nonexistentSupervisorSlots.containsKey(sid)) {
        nonexistentSupervisorSlots.remove(sid);
      }
    }

    Map<String, SupervisorDetails> allSupervisorDetails =
        new HashMap<String, SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
      String sid = entry.getKey();
      SupervisorInfo supervisorInfo = entry.getValue();
      String hostname = supervisorInfo.get_hostname();
      Set<Integer> deadPorts = new HashSet<Integer>();
      if (supervisorToDeadPorts.containsKey(sid)) {
        deadPorts = supervisorToDeadPorts.get(sid);
      }
      // hide the dead-ports from the all-ports
      // these dead-ports can be reused in next round of assignments
      Set<Integer> allPortSet = new HashSet<Integer>();
      Set<Integer> metaPorts = new HashSet<Integer>();

      if (allSchedulingSlots != null && allSchedulingSlots.containsKey(sid)) {
        allPortSet.addAll(allSchedulingSlots.get(sid));
        metaPorts.addAll(allSchedulingSlots.get(sid));
      }
      allPortSet.removeAll(deadPorts);

      List<Number> allPorts = new ArrayList<Number>();
      allPorts.addAll(allPortSet);
      allSupervisorDetails.put(sid,
          new SupervisorDetails(sid, hostname,
              supervisorInfo.get_scheduler_meta(), allPorts,
              supervisorInfo.get_resources_map()));
    }

    for (Entry<String, Set<Integer>> entry : nonexistentSupervisorSlots
        .entrySet()) {
      String sid = entry.getKey();
      Set<Number> ports = new HashSet<Number>();
      ports.addAll(entry.getValue());
      allSupervisorDetails.put(sid, new SupervisorDetails(sid, null, ports));
    }

    return allSupervisorDetails;
  }

  /**
   * 
   * @param schedulerAssignments {topology-id -> SchedulerAssignment}
   * @return {topology-id -> {executor [node port]}}
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-topology->executor->node+port")
  private static Map<String, Map<ExecutorDetails, NodeInfo>> computeTopologyToExecutorToNodeport(
      Map<String, SchedulerAssignment> schedulerAssignments) {

    Map<String, Map<ExecutorDetails, NodeInfo>> ret =
        new HashMap<String, Map<ExecutorDetails, NodeInfo>>();
    for (Entry<String, SchedulerAssignment> entry : schedulerAssignments
        .entrySet()) {
      String tid = entry.getKey();
      SchedulerAssignment assignment = entry.getValue();
      Map<ExecutorDetails, WorkerSlot> executorToSlot =
          assignment.getExecutorToSlot();
      Map<ExecutorDetails, NodeInfo> tmp =
          new HashMap<ExecutorDetails, NodeInfo>();
      for (Map.Entry<ExecutorDetails, WorkerSlot> ets : executorToSlot
          .entrySet()) {
        ExecutorDetails ed = ets.getKey();
        WorkerSlot ws = ets.getValue();
        tmp.put(ed, new NodeInfo(ws.getNodeId(),
            Sets.newHashSet(Long.valueOf(ws.getPort()))));
      }
      ret.put(tid, tmp);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-new-topology->executor->node+port")
  public static Map<String, Map<ExecutorDetails, NodeInfo>> computeNewTopologyToExecutorToNodeport(
      Map<String, SchedulerAssignment> newSchedulerAssignments,
      Map<String, Assignment> existingAssignments) {
    // add more information to convert SchedulerAssignment to Assignment
    Map<String, Map<ExecutorDetails, NodeInfo>> newTopologyToExecutorToNodeport =
        computeTopologyToExecutorToNodeport(newSchedulerAssignments);
    // print some useful information
    for (Entry<String, Map<ExecutorDetails, NodeInfo>> entry : newTopologyToExecutorToNodeport
        .entrySet()) {
      Map<ExecutorDetails, NodeInfo> reassignment =
          new HashMap<ExecutorDetails, NodeInfo>();
      String tid = entry.getKey();
      Map<ExecutorDetails, NodeInfo> executorToNodeport = entry.getValue();

      Map<ExecutorInfo, NodeInfo> oldExecutorToNodeport =
          new HashMap<ExecutorInfo, NodeInfo>();
      if (existingAssignments.containsKey(tid)) {
        Map<List<Long>, NodeInfo> existingExeToNodeInfo =
            existingAssignments.get(tid).get_executor_node_port();
        for (Entry<List<Long>, NodeInfo> e : existingExeToNodeInfo.entrySet()) {
          List<Long> list = e.getKey();
          ExecutorInfo executor = new ExecutorInfo(list.get(0).intValue(),
              list.get(list.size() - 1).intValue());
          oldExecutorToNodeport.put(executor, e.getValue());
        }
      }

      for (Entry<ExecutorDetails, NodeInfo> item : executorToNodeport
          .entrySet()) {
        ExecutorDetails executorDetails = item.getKey();
        NodeInfo NodeInfo = item.getValue();
        ExecutorInfo execute = new ExecutorInfo(executorDetails.getStartTask(),
            executorDetails.getEndTask());
        if (oldExecutorToNodeport.containsKey(execute)
            && !NodeInfo.equals(oldExecutorToNodeport.get(execute))) {
          reassignment.put(executorDetails, NodeInfo);
        }
      }
      if (!reassignment.isEmpty()) {
        // delete-repetition
        int newSlotCnt = new HashSet<NodeInfo>(executorToNodeport.values()).size();
        Set<ExecutorDetails> reassignExecutors = reassignment.keySet();

        LOG.info("Reassigning " + tid + " to " + newSlotCnt + " slots");
        LOG.info("Reassign executors:  " + reassignExecutors.toString());
      }
    }
    return newTopologyToExecutorToNodeport;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#compute-new-scheduler-assignments")
  public static Map<String, SchedulerAssignment> computeNewSchedulerAssignments(
      NimbusData nimbusData, Map<String, Assignment> existingAssignments,
      Topologies topologies, String scratchTopologyID) throws Exception {
    // public so it can be mocked out
    Map conf = nimbusData.getConf();
    // topology->executors
    Map<String, Set<ExecutorInfo>> topologyToExecutor =
        computeTopologyToExecutors(nimbusData, existingAssignments.keySet());

    // update the executors heartbeats first.
    updateAllHeartbeats(nimbusData, existingAssignments, topologyToExecutor);

    // topology->alive-executors
    Map<String, Set<ExecutorInfo>> topologyToAliveExecutors =
        computeTopologyToAliveExecutors(nimbusData, existingAssignments,
            topologies, topologyToExecutor, scratchTopologyID);

    // supervisor->dead-ports
    Map<String, Set<Integer>> supervisorToDeadPorts =
        computeSupervisorToDeadports(nimbusData, existingAssignments,
            topologyToExecutor, topologyToAliveExecutors);

    // topology->scheduler-assignment
    Map<String, SchedulerAssignmentImpl> topologyToSchedulerAssignment =
        computeTopologyToSchedulerAssignment(nimbusData, existingAssignments,
            topologyToAliveExecutors);

    // missing-assignment-topologies
    List<String> missingAssignmentTopologies =
        mkMissingAssignmentTopologies(topologies, topologyToExecutor,
            topologyToAliveExecutors, topologyToSchedulerAssignment);

    // all-scheduling-slots
    Collection<NodeInfo> allSchedulingSlotsList =
        allSchedulingSlots(nimbusData, topologies, missingAssignmentTopologies);

    Map<String, Set<Integer>> allSchedulingSlots =
        new HashMap<String, Set<Integer>>();
    for (NodeInfo nodeInfo : allSchedulingSlotsList) {
      String nodeId = nodeInfo.get_node();
      Integer port = (Integer) nodeInfo.get_port_iterator().next().intValue();
      if (allSchedulingSlots.containsKey(nodeId)) {
        Set<Integer> portsSet = allSchedulingSlots.get(nodeId);
        portsSet.add(port);
      } else {
        Set<Integer> portsSet = new HashSet<Integer>();
        portsSet.add(port);
        allSchedulingSlots.put(nodeId, portsSet);
      }
    }

    // supervisors
    Map<String, SupervisorDetails> supervisors = readAllSupervisorDetails(
        nimbusData, allSchedulingSlots, supervisorToDeadPorts);

    Cluster cluster = new Cluster(nimbusData.getInimubs(), supervisors,
        topologyToSchedulerAssignment, conf);
    cluster.setStatusMap(nimbusData.getIdToSchedStatus());
    // call scheduler.schedule to schedule all the topologies
    // the new assignments for all the topologies are in the cluster object.
    nimbusData.getScheduler().schedule(topologies, cluster);
    cluster.setTopologyResourcesMap(nimbusData.getIdToResources());
    // new-scheduler-assignments
    Map<String, SchedulerAssignment> newSchedulerAssignments =
        cluster.getAssignments();
    if (!Utils.getBoolean(conf.get(Config.SCHEDULER_DISPLAY_RESOURCE), false)) {
      cluster.updateAssignedMemoryForTopologyAndSupervisor(topologies);
    }
    // merge with existing statuses
    nimbusData.setIdToSchedStatus(CoreUtil
        .mergeWith(nimbusData.getIdToSchedStatus(), cluster.getStatusMap()));
    nimbusData.setNodeIdToResources(cluster.getSupervisorsResourcesMap());
    nimbusData.setIdToResources(cluster.getTopologyResourcesMap());

    return newSchedulerAssignments;
  }

  private static List<String> mkMissingAssignmentTopologies(
      Topologies topologies, Map<String, Set<ExecutorInfo>> topologyToExecutors,
      Map<String, Set<ExecutorInfo>> topologyToAliveExecutors,
      Map<String, SchedulerAssignmentImpl> topologyToSchedulerAssignment) {

    Collection<TopologyDetails> topologyDetails = topologies.getTopologies();

    List<String> missingAssignmentTopologies = new ArrayList<String>();
    for (TopologyDetails topologyDetailsItem : topologyDetails) {
      String tid = topologyDetailsItem.getId();
      Set<ExecutorInfo> alle = topologyToExecutors.get(tid);
      Set<ExecutorInfo> alivee = topologyToAliveExecutors.get(tid);
      SchedulerAssignment assignment = topologyToSchedulerAssignment.get(tid);
      int numUsedWorkers = numUsedWorkers(assignment);
      int numWorkers = topologies.getById(tid).getNumWorkers();

      if (alle == null || alle.isEmpty() || !alle.equals(alivee)
          || numUsedWorkers < numWorkers) {
        missingAssignmentTopologies.add(tid);
      }
    }
    return missingAssignmentTopologies;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#changed-executors")
  public static List<List<Long>> changedExecutors(
      Map<List<Long>, NodeInfo> executorToNodeport,
      Map<List<Long>, NodeInfo> newExecutorToNodeport) {
    // slot-assigned
    Map<NodeInfo, List<List<Long>>> slotAssigned =
        Utils.reverseMap(executorToNodeport);
    // new-slot-assigned
    Map<NodeInfo, List<List<Long>>> newSlotAssigned =
        Utils.reverseMap(newExecutorToNodeport);
    // brand-new-slots
    // Attenion! DO NOT USE Utils.map_diff
    Map<NodeInfo, List<List<Long>>> brandNewSlots =
        map_diff(slotAssigned, newSlotAssigned);
    List<List<Long>> ret = new ArrayList<List<Long>>();
    for (List<List<Long>> executor : brandNewSlots.values()) {
      ret.addAll(executor);
    }
    return ret;
  }

  // "Returns mappings in m2 that aren't in m1"
  public static Map<NodeInfo, List<List<Long>>> map_diff(
      Map<NodeInfo, List<List<Long>>> m1, Map<NodeInfo, List<List<Long>>> m2) {

    Map<NodeInfo, List<List<Long>>> ret =
        new HashMap<NodeInfo, List<List<Long>>>();
    for (Entry<NodeInfo, List<List<Long>>> entry : m2.entrySet()) {
      NodeInfo key = entry.getKey();
      List<List<Long>> val = entry.getValue();
      if (!m1.containsKey(key)) {
        ret.put(key, val);
      } else if (m1.containsKey(key) && !isListEqual(m1.get(key), val)) {
        ret.put(key, val);
      }
    }
    return ret;
  }

  private static boolean isListEqual(List<List<Long>> list,
      List<List<Long>> val) {
    if (list.size() != val.size()) {
      return false;
    }
    for (List<Long> e : val) {
      boolean contain = false;
      for (List<Long> e2 : list) {
        if (e2.get(0).longValue() != e.get(0).longValue()
            || e2.get(e2.size() - 1).longValue() != e.get(e2.size() - 1)
                .longValue()) {
          continue;
        } else {
          contain = true;
          break;
        }
      }
      if (!contain) {
        return false;
      }
    }
    return true;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#newly-added-slots")
  public static Set<NodeInfo> newlyAddedSlots(Assignment existingAssignment,
      Assignment newAssignment) {
    Set<NodeInfo> oldSlots = new HashSet<NodeInfo>();
    if (existingAssignment != null) {
      oldSlots.addAll(existingAssignment.get_executor_node_port().values());
    }
    Set<NodeInfo> newSlots =
        new HashSet<NodeInfo>(newAssignment.get_executor_node_port().values());

    Set<NodeInfo> newlyAddded = CoreUtil.set_difference(newSlots, oldSlots);
    return newlyAddded;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#basic-supervisor-details-map")
  public static Map<String, SupervisorDetails> basicSupervisorDetailsMap(
      IStormClusterState stormClusterState) throws Exception {
    Map<String, SupervisorInfo> infos = allSupervisorInfo(stormClusterState);
    Map<String, SupervisorDetails> ret =
        new HashMap<String, SupervisorDetails>();
    for (Entry<String, SupervisorInfo> entry : infos.entrySet()) {
      String id = entry.getKey();
      SupervisorInfo info = entry.getValue();
      ret.put(id, new SupervisorDetails(id, info.get_hostname(),
          info.get_scheduler_meta(), null, info.get_resources_map()));
    }
    return ret;
  }

  /**
   * get existing assignment (just the executor->node+port map) -> default to {}
   * . filter out ones which have a executor timeout; figure out available slots
   * on cluster. add to that the used valid slots to get total slots. figure out
   * how many executors should be in each slot (e.g., 4, 4, 4, 5) only keep
   * existing slots that satisfy one of those slots. for rest, reassign them
   * across remaining slots edge case for slots with no executor timeout but
   * with supervisor timeout... just treat these as valid slots that can be
   * reassigned to. worst comes to worse the executor will timeout and won't
   * assign here next time around
   * 
   * @param nimbusData
   * @param scratchTopologyID
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#mk-assignments")
  public static void mkAssignments(NimbusData nimbusData,
      String scratchTopologyID) throws Exception {
    if (!isLeader(nimbusData, false)) {
      LOG.info("not a leader, skipping assignments");
      return;
    }

    Map conf = nimbusData.getConf();
    IStormClusterState stormClusterState = nimbusData.getStormClusterState();
    INimbus inimbus = nimbusData.getInimubs();
    // read all the topologies
    List<String> topologyIds = stormClusterState.activeStorms();
    if (topologyIds == null || topologyIds.isEmpty()) {
      return;
    }

    Map<String, TopologyDetails> topologieDetails =
        new HashMap<String, TopologyDetails>();
    for (String tid : topologyIds) {
      topologieDetails.put(tid, readTopologyDetails(nimbusData, tid));
    }

    Topologies topologies = new Topologies(topologieDetails);
    // read all the assignments
    List<String> assignedTopologyIDs = stormClusterState.assignments(null);
    Map<String, Assignment> existingAssignments = mkExistingAssignments(
        stormClusterState, scratchTopologyID, assignedTopologyIDs);
    // make the new assignments for topologies
    Map<String, SchedulerAssignment> newSchedulerAssignments =
        computeNewSchedulerAssignments(nimbusData, existingAssignments,
            topologies, scratchTopologyID);
    Map<String, Map<ExecutorDetails, NodeInfo>> topologyToExecutorToNodeport =
        computeNewTopologyToExecutorToNodeport(newSchedulerAssignments,
            existingAssignments);
    for (String assignedTopologyID : assignedTopologyIDs) {
      if (!topologyToExecutorToNodeport.containsKey(assignedTopologyID)) {
        topologyToExecutorToNodeport.put(assignedTopologyID, null);
      }
    }
    Map<String, Map<NodeInfo, WorkerResources>> newAssignedWorkerToResources =
        convertAssignmentsToWorkerToResources(newSchedulerAssignments);
    int nowSecs = Time.currentTimeSecs();
    Map<String, SupervisorDetails> basicSupervisorDetailsMap =
        basicSupervisorDetailsMap(stormClusterState);

    Map<String, Assignment> newAssignments = mkNewAssignments(nimbusData, conf,
        existingAssignments, topologyToExecutorToNodeport, nowSecs,
        basicSupervisorDetailsMap, newAssignedWorkerToResources);

    // tasks figure out what tasks to talk to by looking at topology at
    // runtime
    // only log/set when there's been a change to the assignment

    for (Entry<String, Assignment> newAssignmentsEntry : newAssignments
        .entrySet()) {
      String topologyId = newAssignmentsEntry.getKey();
      Assignment assignment = newAssignmentsEntry.getValue();
      Assignment existingAssignment = existingAssignments.get(topologyId);

      if (existingAssignment != null && existingAssignment.equals(assignment)) {
        LOG.debug("Assignment for " + topologyId + " hasn't changed");
        LOG.debug("Assignment: " + assignment.toString());
      } else {
        LOG.info("Setting new assignment for topology id " + topologyId + ": "
            + assignment.toString());
        //
        Map<List<Long>, NodeInfo> tmpEx = assignment.get_executor_node_port();
        if (null != tmpEx && tmpEx.size() > 0) {
          stormClusterState.setAssignment(topologyId, assignment);
        }
      }
    }

    Map<String, Collection<WorkerSlot>> newSlotsByTopologyId =
        new HashMap<String, Collection<WorkerSlot>>();

    for (Entry<String, Assignment> newAssignmentsEntry : newAssignments
        .entrySet()) {
      String tid = newAssignmentsEntry.getKey();
      Assignment assignment = newAssignmentsEntry.getValue();
      Assignment existingAssignment = existingAssignments.get(tid);

      Set<NodeInfo> newlyAddedSlots =
          newlyAddedSlots(existingAssignment, assignment);
      Set<WorkerSlot> slots = new HashSet<WorkerSlot>();
      for (NodeInfo nodeInfo : newlyAddedSlots) {
        slots.add(new WorkerSlot(nodeInfo.get_node(),
            nodeInfo.get_port_iterator().next().intValue()));
      }
      newSlotsByTopologyId.put(tid, slots);
    }
    inimbus.assignSlots(topologies, newSlotsByTopologyId);
  }

  /**
   * construct the final Assignments by adding start-times etc into it
   * 
   * @param nimbusData
   * @param conf
   * @param existingAssignments
   * @param topologyToExecutorToNodeport
   * @param nowSecs
   * @param basicSupervisorDetailsMap
   * @return
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#mk-assignments#new-assignments")
  private static Map<String, Assignment> mkNewAssignments(NimbusData nimbusData,
      Map conf, Map<String, Assignment> existingAssignments,
      Map<String, Map<ExecutorDetails, NodeInfo>> topologyToExecutorToNodeport,
      int nowSecs, Map<String, SupervisorDetails> basicSupervisorDetailsMap,
      Map<String, Map<NodeInfo, WorkerResources>> newAssignedWorkerToResources)
          throws IOException {
    // construct the final Assignments by adding start-times etc into it
    Map<String, Assignment> newAssignments = new HashMap<String, Assignment>();
    for (Entry<String, Map<ExecutorDetails, NodeInfo>> entry : topologyToExecutorToNodeport
        .entrySet()) {

      String topologyId = entry.getKey();
      Map<ExecutorDetails, NodeInfo> executorToNodeport = entry.getValue();
      // existing-assignment
      Assignment existingAssignment = existingAssignments.get(topologyId);
      // all-nodes
      Set<String> allNodes = new HashSet<String>();
      for (NodeInfo NodeInfo : executorToNodeport.values()) {
        allNodes.add(NodeInfo.get_node());
      }
      // node->host
      Map<String, String> nodeToHost = new HashMap<String, String>();
      for (String node : allNodes) {
        String host = nimbusData.getInimubs()
            .getHostName(basicSupervisorDetailsMap, node);
        if (host != null) {
          nodeToHost.put(node, host);
        }
      }
      // all-node->host
      Map<String, String> allNodeToHost = new HashMap<String, String>();
      if (existingAssignment != null) {
        allNodeToHost.putAll(existingAssignment.get_node_host());
      }
      allNodeToHost.putAll(nodeToHost);
      // reassign-executors
      Map<List<Long>, NodeInfo> executorInfoToNodeport =
          new HashMap<List<Long>, NodeInfo>();
      for (Entry<ExecutorDetails, NodeInfo> item : executorToNodeport
          .entrySet()) {
        ExecutorDetails detail = item.getKey();
        executorInfoToNodeport
            .put(Lists.newArrayList(Long.valueOf(detail.getStartTask()),
                Long.valueOf(detail.getEndTask())), item.getValue());
      }
      Map<List<Long>, NodeInfo> existingExecutorToNodeport =
          new HashMap<List<Long>, NodeInfo>();
      if (existingAssignment != null) {
        existingExecutorToNodeport =
            existingAssignment.get_executor_node_port();
      }
      List<List<Long>> reassignExecutors =
          changedExecutors(existingExecutorToNodeport, executorInfoToNodeport);
      // start-times
      Map<List<Long>, Long> existingExecutorToStartTime =
          new HashMap<List<Long>, Long>();
      if (existingAssignment != null) {
        existingExecutorToStartTime =
            existingAssignment.get_executor_start_time_secs();
      }

      Map<List<Long>, Long> reassignExecutorToStartTime =
          new HashMap<List<Long>, Long>();
      for (List<Long> executor : reassignExecutors) {
        reassignExecutorToStartTime.put(executor, Long.valueOf(nowSecs));
      }

      Map<List<Long>, Long> startTimes =
          new HashMap<List<Long>, Long>(existingExecutorToStartTime);
      for (Entry<List<Long>, Long> ressignEntry : reassignExecutorToStartTime
          .entrySet()) {
        startTimes.put(ressignEntry.getKey(), ressignEntry.getValue());
      }

      // worker->resources
      Map<NodeInfo, WorkerResources> workerToResources =
          newAssignedWorkerToResources.get(topologyId);

      Map<String, String> nodeHost =
          CoreUtil.select_keys(allNodeToHost, allNodes);

      Assignment newAssignment = new Assignment(
          Utils.getString(conf.get(Config.STORM_LOCAL_DIR), "storm-local"),
          new ArrayList<ProfileRequest>());
      newAssignment.set_executor_node_port(executorInfoToNodeport);
      newAssignment.set_node_host(nodeHost);
      newAssignment.set_executor_start_time_secs(startTimes);
      newAssignment.set_worker_resources(workerToResources);
      newAssignments.put(topologyId, newAssignment);
    }
    return newAssignments;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#mk-assignments#existing-assignments")
  private static Map<String, Assignment> mkExistingAssignments(
      IStormClusterState stormClusterState, String scratchTopologyId,
      List<String> assignedTopologyIds) throws Exception {
    Map<String, Assignment> existingAssignments =
        new HashMap<String, Assignment>();
    for (String tid : assignedTopologyIds) {
      /*
       * for the topology which wants rebalance (specified by the
       * scratch-topology-id) we exclude its assignment, meaning that all the
       * slots occupied by its assignment will be treated as free slot in the
       * scheduler code.
       */
      if (scratchTopologyId == null || !tid.equals(scratchTopologyId)) {
        Assignment tmpAssignment = stormClusterState.assignmentInfo(tid, null);
        existingAssignments.put(tid, tmpAssignment);
      }
    }
    return existingAssignments;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#assigned-slots")
  private static Map<String, Set<Integer>> assignedSlots(
      IStormClusterState stormClusterState) throws Exception {
    // Returns a map from node-id to a set of ports
    Map<String, Set<Integer>> rtn = new HashMap<String, Set<Integer>>();

    List<String> assignments = stormClusterState.assignments(null);
    for (String a : assignments) {
      Assignment assignment = stormClusterState.assignmentInfo(a, null);
      Map<List<Long>, NodeInfo> executorToNodeport =
          assignment.get_executor_node_port();
      if (executorToNodeport != null) {
        for (Map.Entry<List<Long>, NodeInfo> entry : executorToNodeport
            .entrySet()) {
          NodeInfo np = entry.getValue();
          if (rtn.containsKey(np.get_node())) {
            Set<Integer> ports = rtn.get(np.get_node());
            ports.add(np.get_port_iterator().next().intValue());
          } else {
            Set<Integer> tmp = new HashSet<Integer>();
            tmp.add(np.get_port_iterator().next().intValue());
            rtn.put(np.get_node(), tmp);
          }
        }
      }
    }
    return rtn;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#all-supervisor-info")
  public static Map<String, SupervisorInfo> allSupervisorInfo(
      IStormClusterState stormClusterState) throws Exception {
    return allSupervisorInfo(stormClusterState, null);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#all-supervisor-info")
  public static Map<String, SupervisorInfo> allSupervisorInfo(
      IStormClusterState stormClusterState, RunnableCallback callback)
          throws Exception {
    Map<String, SupervisorInfo> rtn = new TreeMap<String, SupervisorInfo>();
    List<String> supervisorIds = stormClusterState.supervisors(callback);
    if (supervisorIds != null) {
      for (String supervisorId : supervisorIds) {
        SupervisorInfo supervisorInfo =
            stormClusterState.supervisorInfo(supervisorId);
        if (supervisorInfo == null) {
          LOG.warn("Failed to get SupervisorInfo of " + supervisorId);
        } else {
          rtn.put(supervisorId, supervisorInfo);
        }
      }
    } else {
      LOG.info("No alive supervisor");
    }
    return rtn;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#setup-storm-code")
  public static void setupStormCode(NimbusData nimbus, Map<Object, Object> conf,
      String topologyId, String tmpJarLocation, Map stormConf,
      StormTopology topology) throws Exception {
    Subject subject = ReqContext.context().subject();
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    BlobStore blobStore = nimbus.getBlobStore();
    String jarKey = ConfigUtils.masterStormJarKey(topologyId);
    String codeKey = ConfigUtils.masterStormCodeKey(topologyId);
    String confKey = ConfigUtils.masterStormConfKey(topologyId);
    NimbusInfo nimbusHostPortInfo = nimbus.getNimbusHostPortInfo();
    if (tmpJarLocation != null) {
      // in local mode there is no jar
      blobStore.createBlob(jarKey, new FileInputStream(tmpJarLocation),
          new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
      if (blobStore instanceof LocalFsBlobStore) {
        stormClusterState.setupBlobstore(jarKey, nimbusHostPortInfo,
            getVersionForKey(jarKey, nimbusHostPortInfo, conf));
      }
    }

    blobStore.createBlob(confKey, Utils.toCompressedJsonConf(stormConf),
        new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
    if (blobStore instanceof LocalFsBlobStore) {
      stormClusterState.setupBlobstore(confKey, nimbusHostPortInfo,
          getVersionForKey(confKey, nimbusHostPortInfo, conf));
    }

    blobStore.createBlob(codeKey, Utils.serialize(topology),
        new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
    if (blobStore instanceof LocalFsBlobStore) {
      stormClusterState.setupBlobstore(codeKey, nimbusHostPortInfo,
          getVersionForKey(codeKey, nimbusHostPortInfo, conf));
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#read-storm-topology")
  public static StormTopology readStormTopology(String stormId,
      BlobStore blobStore)
          throws IOException, KeyNotFoundException, AuthorizationException {
    return Utils.deserialize(blobStore
        .readBlob(ConfigUtils.masterStormCodeKey(stormId), getSubject()),
        StormTopology.class);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#set-topology-status!")
  public static void setTopologyStatus(NimbusData nimbus, String stormId,
      StormBase status) throws Exception {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    stormClusterState.updateStorm(stormId, status);
    LOG.info("Updated {} with status {} ", stormId, status.get_status().name());
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#validate-topology-size")
  public static void validateTopologySize(Map topoConf, Map nimbusConf,
      StormTopology topology) throws InvalidTopologyException {
    int workersCount = Utils.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 0);
    Integer workersAllowed =
        Utils.getInt(nimbusConf.get(Config.NIMBUS_SLOTS_PER_TOPOLOGY), null);
    Map<String, Object> allComponents = StormCommon.allComponents(topology);
    Map<String, Integer> numExecutors = new HashMap<String, Integer>();
    for (Entry<String, Object> e : allComponents.entrySet()) {
      numExecutors.put(e.getKey(), StormCommon.numStartExecutors(e.getValue()));
    }
    int executorsCount = 0;
    for (Integer val : numExecutors.values()) {
      executorsCount += val;
    }
    Integer executorsAllowed = Utils
        .getInt(nimbusConf.get(Config.NIMBUS_EXECUTORS_PER_TOPOLOGY), null);

    if (executorsAllowed != null
        && executorsCount > executorsAllowed.intValue()) {
      throw new InvalidTopologyException(
          "Failed to submit topology. Topology requests more than "
              + executorsAllowed + " executors.");
    }

    if (workersAllowed != null && workersCount > workersAllowed.intValue()) {
      throw new InvalidTopologyException(
          "Failed to submit topology. Topology requests more than "
              + workersAllowed + " workers.");
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#setup-jar")
  public static void setupJar(Map conf, String tmpJarLocation, String stormroot)
      throws IOException {
    if (ConfigUtils.isLocalMode(conf)) {
      setupLocalJar(conf, tmpJarLocation, stormroot);
    } else {
      setupDistributedJar(conf, tmpJarLocation, stormroot);
    }
  }

  /**
   * distributed implementation
   * 
   * @param conf
   * @param tmpJarLocation
   * @param stormroot
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#setup-jar:distributed")
  private static void setupDistributedJar(Map conf, String tmpJarLocation,
      String stormroot) throws IOException {
    File srcFile = new File(tmpJarLocation);
    if (!srcFile.exists()) {
      throw new IllegalArgumentException(
          tmpJarLocation + " to copy to " + stormroot + " does not exist!");
    }
    String path = ConfigUtils.masterStormJarPath(stormroot);
    File destFile = new File(path);
    FileUtils.copyFile(srcFile, destFile);
  }

  /**
   * local implementation
   * 
   * @param conf
   * @param tmpJarLocation
   * @param stormroot
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#setup-jar:local")
  private static void setupLocalJar(Map conf, String tmpJarLocation,
      String stormroot) throws IOException {
    return;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#thrift-status->kw-status")
  public static TopologyStatus thriftStatusToKwStatus(
      TopologyInitialStatus tStatus) {
    if (tStatus.equals(TopologyInitialStatus.ACTIVE)) {
      return TopologyStatus.ACTIVE;
    } else {
      return TopologyStatus.INACTIVE;
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#start-storm")
  public static void startStorm(NimbusData nimbusData, String stormName,
      String stormId, TopologyStatus topologyInitialStatus) throws Exception {
    IStormClusterState stormClusterState = nimbusData.getStormClusterState();
    Map conf = nimbusData.getConf();
    Map stormConf =
        NimbusUtils.readStormConf(conf, stormId, nimbusData.getBlobStore());
    StormTopology topology = StormCommon.systemTopology(stormConf,
        NimbusUtils.readStormTopology(stormId, nimbusData.getBlobStore()));

    Map<String, Object> allComponents = StormCommon.allComponents(topology);

    Map<String, Integer> componentToExecutors = new HashMap<String, Integer>();
    for (Entry<String, Object> e : allComponents.entrySet()) {
      componentToExecutors.put(e.getKey(),
          StormCommon.numStartExecutors(e.getValue()));
    }

    int topologyWorkers =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_WORKERS), 1);
    String owner =
        Utils.getString(stormConf.get(Config.TOPOLOGY_SUBMITTER_USER),
            System.getProperty("user.name"));
    StormBase stormBase =
        new StormBase(stormName, topologyInitialStatus, topologyWorkers);
    stormBase.set_launch_time_secs(Time.currentTimeSecs());
    stormBase.set_owner(owner);
    stormBase.set_component_executors(componentToExecutors);
    LOG.info("Activating {}:{}", stormName, stormId);
    LOG.info("StormBase: " + stormBase.toString());
    stormClusterState.activateStorm(stormId, stormBase);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#storm-active?")
  private static boolean isStormActive(IStormClusterState stormClusterState,
      String topologyName) throws Exception {
    boolean rtn = false;
    if (StormCommon.getStormId(stormClusterState, topologyName) != null) {
      rtn = true;
    }
    return rtn;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#check-storm-active!")
  public static void checkStormActive(NimbusData nimbus, String stormName,
      boolean isActive) throws Exception {
    if (isStormActive(nimbus.getStormClusterState(), stormName) != isActive) {
      if (isActive) {
        throw new NotAliveException(stormName + " is not alive");
      } else {
        throw new AlreadyAliveException(stormName + " is already active");
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#check-authorization!")
  public static void checkAuthorization(NimbusData nimbus, String stormName,
      Map stormConf, String operation, ReqContext context)
          throws AuthorizationException {
    IAuthorizer aclHandler = nimbus.getAuthorizationHandler();
    IAuthorizer impersonationAuthorizer =
        nimbus.getImpersonationAuthorizationHandler();
    ReqContext ctx = context != null ? context : ReqContext.context();
    Map checkConf = new HashMap();
    if (stormConf != null) {
      checkConf = stormConf;
    } else if (stormName != null) {
      checkConf.put(Config.TOPOLOGY_NAME, stormName);
    }
    ThriftAccessLogger.logAccess(ctx.requestID(), ctx.remoteAddress(),
        ctx.principal(), operation);
    if (ctx.isImpersonating()) {
      LOG.warn("principal: " + ctx.realPrincipal()
          + " is trying to impersonate principal: " + ctx.principal());
      if (impersonationAuthorizer != null) {
        if (!impersonationAuthorizer.permit(ctx, operation, checkConf)) {
          throw new AuthorizationException("principal " + ctx.realPrincipal()
              + " is not authorized to impersonate principal " + ctx.principal()
              + " from host " + ctx.remoteAddress()
              + " Please see SECURITY.MD to learnhow to configure impersonation acls.");
        } else {
          LOG.warn("impersonation attempt but "
              + Config.NIMBUS_IMPERSONATION_AUTHORIZER
              + " has no authorizer configured. potential security risk, please see SECURITY.MD to learn how to configure impersonation authorizer.");
        }
      }
    }

    if (aclHandler != null) {
      if (!aclHandler.permit(ctx, operation, checkConf)) {
        String msg = operation;
        if (stormName != null) {
          msg += " on topology " + stormName;
        }
        msg += " is not authorized";
        throw new AuthorizationException(msg);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#check-authorization!")
  public static void checkAuthorization(NimbusData nimbus, String stormName,
      Map stormConf, String operation) throws AuthorizationException {
    checkAuthorization(nimbus, stormName, stormConf, operation,
        ReqContext.context());
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#do-rebalance")
  public static void doRebalance(NimbusData nimbus, String stormId,
      TopologyStatus status, StormBase stormBase) {
    try {
      RebalanceOptions rebalanceOptions =
          stormBase.get_topology_action_options().get_rebalance_options();
      stormBase.set_topology_action_options(null);
      stormBase.set_component_executors(rebalanceOptions.get_num_executors());
      stormBase.set_num_workers(rebalanceOptions.get_num_workers());

      nimbus.getStormClusterState().updateStorm(stormId, stormBase);
      mkAssignments(nimbus, stormId);
    } catch (Exception e) {
      LOG.error("do-rebalance error!", e);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-version-for-key")
  public static int getVersionForKey(String key, NimbusInfo nimbusHostPortInfo,
      Map conf) throws Exception {
    KeySequenceNumber version = new KeySequenceNumber(key, nimbusHostPortInfo);
    return version.getKeySequenceNumber(conf);
  }

  /**
   * use a outside zkClient, insteadof create CuratorFramework in
   * KeySequenceNumber.getKeySequenceNumber
   * 
   * @param zkClient
   * @param key
   * @param nimbusHostPortInfo
   * @param conf
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-version-for-key")
  public static int getVersionForKey(CuratorFramework zkClient, String key,
      NimbusInfo nimbusHostPortInfo, Map conf) throws Exception {
    KeySequenceNumber version = new KeySequenceNumber(key, nimbusHostPortInfo);
    return version.getKeySequenceNumber(zkClient, conf);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#does-users-group-intersect?")
  public static boolean doesUsersGroupIntersect(String user,
      List<String> groupsToCheck, Map conf) throws Exception {
    // "Check to see if any of the users groups intersect with the list of
    // groups passed in"
    if (groupsToCheck.size() == 0) {
      return false;
    }
    Set<String> groups = userGroups(user, conf);
    if (groups != null) {
      for (String group : groups) {
        if (groupsToCheck.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#user-groups")
  private static Set<String> userGroups(String user, Map conf) {
    if (StringUtils.isEmpty(user)) {
      try {
        return igroupMapper(conf).getGroups(user);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
    return null;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#igroup-mapper")
  private static IGroupMappingServiceProvider igroupMapper(Map conf) {
    return AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#set-logger-timeouts")
  public static void setLoggerTimeOuts(LogLevel logConfig) {
    int timeoutSecs = logConfig.get_reset_log_level_timeout_secs();
    int timeout = Time.currentTimeSecs() + timeoutSecs;
    if (timeout > Time.currentTimeSecs()) {
      logConfig.set_reset_log_level_timeout_epoch(timeout);
    } else {
      logConfig.unset_reset_log_level_timeout_epoch();
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-subject")
  private static Subject getSubject() {
    ReqContext req = ReqContext.context();
    return req.subject();
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-nimbus-subject")
  public static Subject getNimbusSubject() {
    Subject subject = new Subject();
    NimbusPrincipal principal = new NimbusPrincipal();
    Set<Principal> principals = subject.getPrincipals();
    principals.add(principal);
    return subject;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-nimbus-subject")
  public static Subject nimbusSubject() {
    return getNimbusSubject();
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-key-list-from-id")
  public static List<String> getKeyListFromId(Map conf, String id) {
    StringBuffer tmpSb = new StringBuffer();
    tmpSb.append("{").append(ConfigUtils.masterStormCodeKey(id)).append(", ")
        .append(ConfigUtils.masterStormJarKey(id)).append(", ")
        .append(ConfigUtils.masterStormConfKey(id)).append("}");
    LOG.debug("set keys id = {} set = {}",
        new Object[] { id, tmpSb.toString() });
    if (ConfigUtils.isLocalMode(conf)) {
      return Arrays.asList(ConfigUtils.masterStormCodeKey(id),
          ConfigUtils.masterStormConfKey(id));
    }
    return Arrays.asList(ConfigUtils.masterStormCodeKey(id),
        ConfigUtils.masterStormJarKey(id), ConfigUtils.masterStormConfKey(id));
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#is-leader")
  public static boolean isLeader(NimbusData nimbus, boolean throwException) {
    boolean isLeader;
    try {
      isLeader = nimbus.getLeaderElector().isLeader();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (isLeader) {
      return true;
    } else if (throwException) {
      String leaderAddress =
          nimbus.getLeaderElector().getLeader().toHostPortString();
      throw new RuntimeException(
          "not a leader, current leader is " + leaderAddress);
    }
    return false;
  }

  /**
   * Sets up blobstore state for all current keys.
   * 
   * @param nimbus
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#setup-blobstore")
  public static void setupBlobstore(NimbusData nimbus) throws Exception {
    LOG.debug("Deleting keys not on the zookeeper");
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    BlobStore blobStore = nimbus.getBlobStore();
    Set<String> localSetOfKeys = getKeySeqFromBlobStore(blobStore);
    List<String> activeKeys = stormClusterState.activeKeys();
    Set<String> allkeys = new HashSet<String>();
    for (String activeKey : activeKeys) {
      allkeys.add(activeKey);
    }
    NimbusInfo nimbusHostPortInfo = nimbus.getNimbusHostPortInfo();
    Set<String> locallyAvailableActiveKeys =
        CoreUtil.SetIntersection(localSetOfKeys, allkeys);
    Set<String> keysToDelete = CoreUtil.set_difference(localSetOfKeys, allkeys);

    for (String key : keysToDelete) {
      blobStore.deleteBlob(key, nimbusSubject());
    }
    LOG.debug("Creating list of key entries for blobstore inside zookeeper"
        + allkeys + "local" + locallyAvailableActiveKeys);

    CuratorFramework zkClient = BlobStoreUtils.createZKClient(nimbus.getConf());
    try {
      for (String key : locallyAvailableActiveKeys) {
        stormClusterState.setupBlobstore(key, nimbusHostPortInfo,
            getVersionForKey(zkClient, key, nimbusHostPortInfo,
                nimbus.getConf()));
      }
    } finally {
      zkClient.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-key-seq-from-blob-store")
  public static Set<String> getKeySeqFromBlobStore(BlobStore blobStore)
      throws Exception {
    Iterator<String> keyIter = blobStore.listKeys();
    Set<String> keyset = new HashSet<String>();
    while (keyIter.hasNext()) {
      keyset.add(keyIter.next());
    }
    return keyset;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#create-tology-action-notifier")
  public static ITopologyActionNotifierPlugin createTologyActionNotifier(
      Map conf) {
    String actionNotifierPlugin = Utils.getString(
        conf.get(Config.NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN), null);
    try {
      if (actionNotifierPlugin != null) {
        ITopologyActionNotifierPlugin instance =
            ReflectionUtils.newInstance(actionNotifierPlugin);
        instance.prepare(conf);
        return instance;
      }
    } catch (Exception e) {
      LOG.error("Ingoring exception, Could not initialize {}",
          actionNotifierPlugin);
    }
    return null;

  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#inbox")
  public static String inbox(NimbusData nimbus) throws Exception {
    return ConfigUtils.masterInbox(nimbus.getConf());
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#get-blob-replication-count")
  public static Integer getBlobReplicationCount(String blobKey,
      NimbusData nimbus) throws Exception {
    BlobStore blobStore = nimbus.getBlobStore();
    if (blobStore != null) {
      return blobStore.getBlobReplication(blobKey, NimbusUtils.nimbusSubject());
    } else {
      return null;
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#wait-for-desired-code-replication")
  public static void waitForDesiredCodeReplication(NimbusData nimbus, Map conf,
      String stormId) throws Exception {
    int minReplicationCount =
        Utils.getInt(conf.get(Config.TOPOLOGY_MIN_REPLICATION_COUNT), 1);
    int maxReplicationWaitTime = Utils
        .getInt(conf.get(Config.TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC), 60);
    AtomicInteger currentReplicationCountJar;
    String stormjarKey = ConfigUtils.masterStormJarKey(stormId);
    boolean isLocalMode = ConfigUtils.isLocalMode(conf);
    if (!isLocalMode) {
      currentReplicationCountJar =
          new AtomicInteger(getBlobReplicationCount(stormjarKey, nimbus));
    } else {
      currentReplicationCountJar = new AtomicInteger(minReplicationCount);
    }
    String stormCodeKey = ConfigUtils.masterStormCodeKey(stormId);
    AtomicInteger currentReplicationCountCode =
        new AtomicInteger(getBlobReplicationCount(stormCodeKey, nimbus));
    String stormConfKey = ConfigUtils.masterStormConfKey(stormId);
    AtomicInteger currentReplicationCountConf =
        new AtomicInteger(getBlobReplicationCount(stormConfKey, nimbus));
    AtomicInteger totalWaitTime = new AtomicInteger(0);

    if (nimbus.getBlobStore() != null) {
      while ((minReplicationCount > currentReplicationCountJar.get()
          || minReplicationCount > currentReplicationCountCode.get()
          || minReplicationCount > currentReplicationCountConf.get())
          && (maxReplicationWaitTime < 0
              || totalWaitTime.get() < maxReplicationWaitTime)) {
        CoreUtil.sleepSecs(1);
        LOG.debug(
            "waiting for desired replication to be achieved. min-replication-count ="
                + minReplicationCount + " max-replication-wait-time = "
                + maxReplicationWaitTime
                + (!isLocalMode ? ("current-replication-count for jar key = "
                    + currentReplicationCountJar.get()) : "")
                + " current-replication-count for code key = "
                + currentReplicationCountCode.get()
                + " current-replication-count for conf key = "
                + currentReplicationCountConf.get() + " total-wait-time "
                + totalWaitTime.get());

        totalWaitTime.incrementAndGet();
        if (!isLocalMode) {
          currentReplicationCountJar
              .set(getBlobReplicationCount(stormjarKey, nimbus));
        }
        currentReplicationCountCode
            .set(getBlobReplicationCount(stormCodeKey, nimbus));
        currentReplicationCountConf
            .set(getBlobReplicationCount(stormConfKey, nimbus));
      }
    }
    if (minReplicationCount <= currentReplicationCountConf.get()
        && minReplicationCount <= currentReplicationCountCode.get()
        && minReplicationCount <= currentReplicationCountJar.get()) {
      LOG.info(
          "desired replication count of {}  "
              + "not achieved but we have hit the max wait time {}  "
              + "so moving on with replication count for conf key ={} "
              + " for code key = {} " + "for jar key = {}",
          new Object[] { minReplicationCount, maxReplicationWaitTime,
              currentReplicationCountConf.get(),
              currentReplicationCountCode.get(),
              currentReplicationCountJar.get() });
    } else {
      LOG.info(
          "desired replication count {}  achieved, "
              + "current-replication-count for conf key = {} "
              + "current-replication-count for code key = {}"
              + "current-replication-count for jar key {}= ",
          new Object[] { minReplicationCount, currentReplicationCountConf.get(),
              currentReplicationCountCode.get(),
              currentReplicationCountJar.get() });
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#notify-topology-action-listener")
  public static void notifyTopologyActionListener(NimbusData nimbus,
      String stormId, String action) throws Exception {
    ITopologyActionNotifierPlugin topologyActionNotifier =
        nimbus.getNimbusTopologyActionNotifier();
    if (topologyActionNotifier != null) {
      try {
        topologyActionNotifier.notify(stormId, action);
      } catch (Exception e) {
        LOG.error(
            "Ignoring exception from Topology action notifier for storm-Id ",
            stormId, e);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#add-topology-to-history-log")
  public static void addTopologyToHistoryLog(String stormId, NimbusData nimbus,
      Map topologyConf) {
    LOG.info("Adding topo to history log:{}", stormId);
    synchronized (nimbus.getTopologyHistoryLock()) {
      LocalState topoHistorystate = nimbus.getTopoHistorystate();
      List<String> users = ConfigUtils.getTopoLogsUsers(topologyConf);
      List<String> groups = ConfigUtils.getTopoLogsGroups(topologyConf);
      List<LSTopoHistory> currHistory =
          LocalStateUtils.getLSTopoList(topoHistorystate);
      List<LSTopoHistory> newHistory =
          new ArrayList<LSTopoHistory>(currHistory);
      LSTopoHistory history =
          new LSTopoHistory(stormId, System.currentTimeMillis(), users, groups);
      newHistory.add(history);
      LocalStateUtils.putLSTopoList(topoHistorystate, newHistory);
    }
  }

  public static void checkVersion(NimbusData nimbus,
      NimbusSummary nimbusSummary) {
    // check nimbus version
    List<NimbusSummary> nimbusSummaries =
        nimbus.getStormClusterState().nimbuses();
    NimbusSummary nimbusLeader = null;
    for (NimbusSummary summary : nimbusSummaries) {
      if (summary.is_isLeader()) {
        nimbusLeader = summary;
        break;
      }
    }
    if (nimbusLeader != null) {
      if (!nimbusSummary.get_version().equals(nimbusLeader.get_version())) {
        throw new RuntimeException(
            "nimbus version :" + nimbusSummary.get_version()
                + " not match  nimubus leader :" + nimbusLeader.toString());
      }
    }

  }
}
