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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.data.ACL;

import com.esotericsoftware.minlog.Log;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorCache;
import com.tencent.jstorm.daemon.nimbus.transitions.StateTransitions;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 10:37:41 AM Mar 17, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.nimbus#nimbus-data")
public class NimbusData implements Serializable {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("rawtypes")
  private Map conf;
  private NimbusInfo nimbusHostPortInfo;
  private List<ACL> acls = null;
  private IStormClusterState stormClusterState;
  private ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorHeartbeatsCache;
  private FileCacheMap<Object, Object> downloaders;
  private FileCacheMap<Object, Object> uploaders;
  private ITopologyValidator validator;
  private AtomicInteger submittedCount;
  private Object submitLock;
  private Object credUpdateLock;
  private StateTransitions statusTransition;
  private final INimbus inimubs;
  private final IAuthorizer authorizationHandler;
  private final IAuthorizer impersonationAuthorizationHandler;
  private final IScheduler scheduler;
  private Map<String, String> idToSchedStatus;
  // resources of supervisors
  private Map<String, Double[]> nodeIdToResources;
  // resources of topologies
  private Map<String, Double[]> idToResources;
  private Collection<ICredentialsRenewer> credRenewers;
  private Collection<INimbusCredentialPlugin> nimbusAutocredPlugins;
  private MkBlobCacheMap<Object, Object> blobUploaders;
  private MkBlobCacheMap<Object, Object> blobDownloaders;
  private BlobStore blobStore;
  private MkBloblistCacheMap<Object, Object> blobListers;
  private UptimeComputer uptime;
  private LocalState topoHistorystate;
  private ILeaderElector leaderElector;
  private Object topologyHistoryLock;
  private ITopologyActionNotifierPlugin nimbusTopologyActionNotifier;
  private StormTimer timer;

  @SuppressWarnings({ "rawtypes" })
  public NimbusData(Map conf, INimbus inimbus) throws Exception {
    this.conf = conf;
    this.inimubs = inimbus;
    this.authorizationHandler = StormCommon.mkAuthorizationHandler(
        (String) conf.get(Config.NIMBUS_AUTHORIZER), conf);
    this.impersonationAuthorizationHandler = StormCommon.mkAuthorizationHandler(
        (String) conf.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
    this.submittedCount = new AtomicInteger(0);
    if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
      this.acls = NimbusUtils.nimbusZKAcls();
    }
    this.stormClusterState = ClusterUtils.mkStormClusterState(conf, acls,
        new ClusterStateContext(DaemonType.NIMBUS));
    this.submitLock = new Object();
    this.credUpdateLock = new Object();
    this.executorHeartbeatsCache =
        new ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>>();
    this.downloaders = new FileCacheMap<Object, Object>(conf);
    this.uploaders = new FileCacheMap<Object, Object>(conf);
    this.statusTransition = new StateTransitions(this);
    this.validator = NimbusUtils.mkTopologyValidator(conf);
    this.timer = CoreUtil.mkHaltingTimer("timer");
    this.scheduler = NimbusUtils.mkScheduler(conf, inimbus);
    this.idToSchedStatus = new ConcurrentHashMap<String, String>();
    this.nodeIdToResources = new ConcurrentHashMap<String, Double[]>();
    this.idToResources = new ConcurrentHashMap<String, Double[]>();
    this.credRenewers = AuthUtils.GetCredentialRenewers(conf);
    this.nimbusAutocredPlugins = AuthUtils.getNimbusAutoCredPlugins(conf);
    this.blobUploaders = new MkBlobCacheMap<Object, Object>(conf);
    this.blobDownloaders = new MkBlobCacheMap<Object, Object>(conf);
    this.setBlobStore(
        Utils.getNimbusBlobStore(conf, NimbusInfo.fromConf(conf)));
    this.blobListers = new MkBloblistCacheMap<Object, Object>(conf);
    this.uptime = Utils.makeUptimeComputer();
    // TODO
    // Integer port = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT), -1);
    // if (port < 0) {
    // port = Integer.valueOf(NetWorkUtils.assignServerPort(port.intValue()));
    // conf.put(Config.NIMBUS_THRIFT_PORT, port);
    // }
    this.setNimbusHostPortInfo(NimbusInfo.fromConf(conf));
    this.setTopoHistorystate(ConfigUtils.nimbusTopoHistoryState(conf));
    this.leaderElector = Zookeeper.zkLeaderElector(conf);
    this.topologyHistoryLock = new Object();
    this.nimbusTopologyActionNotifier =
        NimbusUtils.createTologyActionNotifier(conf);
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public void setConf(Map conf) {
    this.conf = conf;
  }

  public IStormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public void setStormClusterState(IStormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> getExecutorHeartbeatsCache() {
    return this.executorHeartbeatsCache;
  }

  public void setExecutorHeartbeatsCache(
      ConcurrentHashMap<String, Map<ExecutorInfo, ExecutorCache>> executorCache) {
    this.executorHeartbeatsCache = executorCache;
  }

  public FileCacheMap<Object, Object> getDownloaders() {
    return downloaders;
  }

  public void setDownloaders(FileCacheMap<Object, Object> downloaders) {
    this.downloaders = downloaders;
  }

  public ILeaderElector getLeaderElector() {
    return leaderElector;
  }

  public void setLeaderElector(ILeaderElector leaderElector) {
    this.leaderElector = leaderElector;
  }

  public UptimeComputer getUptime() {
    return uptime;
  }

  public void setUptime(UptimeComputer uptime) {
    this.uptime = uptime;
  }

  public IAuthorizer getImpersonationAuthorizationHandler() {
    return impersonationAuthorizationHandler;
  }

  public Collection<INimbusCredentialPlugin> getNimbusAutocredPlugins() {
    return nimbusAutocredPlugins;
  }

  public void setNimbusAutocredPlugins(
      Collection<INimbusCredentialPlugin> nimbusAutocredPlugins) {
    this.nimbusAutocredPlugins = nimbusAutocredPlugins;
  }

  public Map<String, String> getIdToSchedStatus() {
    return idToSchedStatus;
  }

  public void setIdToSchedStatus(Map<String, String> idToSchedStatus) {
    this.idToSchedStatus = idToSchedStatus;
  }

  public Map<String, Double[]> getNodeIdToResources() {
    return nodeIdToResources;
  }

  public void setNodeIdToResources(Map<String, Double[]> nodeIdToResources) {
    this.nodeIdToResources = nodeIdToResources;
  }

  public Map<String, Double[]> getIdToResources() {
    return idToResources;
  }

  public void setIdToResources(Map<String, Double[]> idToResources) {
    this.idToResources = idToResources;
  }

  public FileCacheMap<Object, Object> getUploaders() {
    return uploaders;
  }

  public void setUploaders(FileCacheMap<Object, Object> uploaders) {
    this.uploaders = uploaders;
  }

  public IAuthorizer getAuthorizationHandler() {
    return authorizationHandler;
  }

  public Collection<ICredentialsRenewer> getCredRenewers() {
    return credRenewers;
  }

  public void setCredRenewers(Collection<ICredentialsRenewer> credRenewers) {
    this.credRenewers = credRenewers;
  }

  public AtomicInteger getSubmittedCount() {
    return submittedCount;
  }

  public void setSubmittedCount(AtomicInteger submittedCount) {
    this.submittedCount = submittedCount;
  }

  public Object getSubmitLock() {
    return submitLock;
  }

  public Object getCredUpdateLock() {
    return credUpdateLock;
  }

  public void setCredUpdateLock(Object credUpdateLock) {
    this.credUpdateLock = credUpdateLock;
  }

  public StateTransitions getStatusTransition() {
    return statusTransition;
  }

  public void cleanup() {
    try {
      stormClusterState.disconnect();
    } catch (Exception e) {
      Log.error("Error when disconnect StormClusterState for "
          + CoreUtil.stringifyError(e));
    }
  }

  public INimbus getInimubs() {
    return inimubs;
  }

  public IScheduler getScheduler() {
    return scheduler;
  }

  public ITopologyValidator getValidator() {
    return validator;
  }

  public void setValidator(ITopologyValidator validator) {
    this.validator = validator;
  }

  public MkBlobCacheMap<Object, Object> getBlobUploaders() {
    return blobUploaders;
  }

  public void setBlobUploaders(MkBlobCacheMap<Object, Object> blobUploaders) {
    this.blobUploaders = blobUploaders;
  }

  public MkBlobCacheMap<Object, Object> getBlobDownloaders() {
    return blobDownloaders;
  }

  public void setBlobDownloaders(
      MkBlobCacheMap<Object, Object> blobDownloaders) {
    this.blobDownloaders = blobDownloaders;
  }

  public BlobStore getBlobStore() {
    return blobStore;
  }

  public void setBlobStore(BlobStore blobStore) {
    this.blobStore = blobStore;
  }

  public MkBloblistCacheMap<Object, Object> getBlobListers() {
    return blobListers;
  }

  public void setBlobListers(MkBloblistCacheMap<Object, Object> blobListers) {
    this.blobListers = blobListers;
  }

  public NimbusInfo getNimbusHostPortInfo() {
    return nimbusHostPortInfo;
  }

  public void setNimbusHostPortInfo(NimbusInfo nimbusHostPortInfo) {
    this.nimbusHostPortInfo = nimbusHostPortInfo;
  }

  public LocalState getTopoHistorystate() {
    return topoHistorystate;
  }

  public void setTopoHistorystate(LocalState topoHistorystate) {
    this.topoHistorystate = topoHistorystate;
  }

  public Object getTopologyHistoryLock() {
    return topologyHistoryLock;
  }

  public void setTopologyHistoryLock(Object topologyHistoryLock) {
    this.topologyHistoryLock = topologyHistoryLock;
  }

  public ITopologyActionNotifierPlugin getNimbusTopologyActionNotifier() {
    return nimbusTopologyActionNotifier;
  }

  public void setNimbusTopologyActionNotifier(
      ITopologyActionNotifierPlugin nimbusTopologyActionNotifier) {
    this.nimbusTopologyActionNotifier = nimbusTopologyActionNotifier;
  }

  public StormTimer getTimer() {
    return timer;
  }

}
