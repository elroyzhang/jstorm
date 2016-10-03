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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.BeginDownloadResult;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyBackpressure;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerJvmInfo;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.BufferFileInputStream;
import org.apache.storm.utils.BufferInputStream;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.validation.ConfigValidation;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorCache;
import com.tencent.jstorm.daemon.nimbus.threads.CleanInboxRunnable;
import com.tencent.jstorm.daemon.nimbus.threads.MonitorRunnable;
import com.tencent.jstorm.localstate.LocalStateUtils;
import com.tencent.jstorm.stats.Stats;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 2:36:12 PM Mar 9, 2016
 */
@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
@ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler")
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
  private final static Logger LOG =
      LoggerFactory.getLogger(ServiceHandler.class);
  public final static int THREAD_NUM = 64;
  private NimbusData nimbus;
  private Map conf;
  private BlobStore blobStore;
  private IPrincipalToLocal principalToLocal;
  private List<String> adminUsers;

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler")
  public ServiceHandler(Map conf, INimbus inimbus) throws Exception {
    this.conf = conf;
    inimbus.prepare(conf, ConfigUtils.masterInimbusDir(conf));
    LOG.info("Starting Nimbus with conf {} ", conf.toString());
    this.nimbus = new NimbusData(conf, inimbus);
    this.blobStore = nimbus.getBlobStore();
    this.principalToLocal = AuthUtils.GetPrincipalToLocalPlugin(conf);
    this.adminUsers = CoreUtil.parseList(conf.get(Config.NIMBUS_ADMINS),
        new ArrayList<String>());

    nimbus.getValidator().prepare(conf);

    // add to nimbuses
    NimbusInfo nimbusInfo = nimbus.getNimbusHostPortInfo();
    NimbusSummary nimbusSummary =
        new NimbusSummary(nimbusInfo.getHost(), nimbusInfo.getPort(),
            Time.currentTimeSecs(), false, VersionInfo.getVersion());
    // check version
    NimbusUtils.checkVersion(nimbus, nimbusSummary);
    nimbus.getStormClusterState().addNimbusHost(nimbusInfo.toHostPortString(),
        nimbusSummary, nimbus.getLeaderElector());
    nimbus.getLeaderElector().addToLeaderLockQueue();

    // wait for a nimbus gain leader. Otherwise may cause a bug ,nimbus cannot
    // clean corrput topologies for isleader check is false.
    Time.sleepSecs(
        Utils.getInt(conf.get(Config.NIMBUS_GAIN_LEADER_WAIT_SECS), 1));

    BlobSyncRunnableCallback blobSync =
        new BlobSyncRunnableCallback(conf, nimbus);
    if (blobStore instanceof LocalFsBlobStore) {
      // register call back for blob-store
      nimbus.getStormClusterState().blobstore(blobSync);
      NimbusUtils.setupBlobstore(nimbus);
    }

    // clean up old topologies
    NimbusUtils.cleanupCorruptTopologies(nimbus);

    if (NimbusUtils.isLeader(nimbus, false)) {
      List<String> activeStormIds =
          nimbus.getStormClusterState().activeStorms();
      if (activeStormIds != null) {
        for (String stormId : activeStormIds) {
          NimbusUtils.transition(nimbus, stormId, false,
              TopologyStatus.START_UP);
        }
      }
      LOG.info("Successfully init topology status");
    }

    // 1 Schedule Nimbus monitor mkAssignments
    scheduleNimbusMonitor(conf);
    // 2 Schedule Nimbus inbox cleaner
    scheduleNimbusInboxCleaner(conf);
    // 3 Schedule nimbus code sync thread to sync code from other nimbuses.
    scheduleNimbusCodeSyncThread(conf, blobStore);
    // 4 Schedule topology history cleaner
    scheduleTopologyHistoryCleaner(conf);
    // 5 Schedule renew credentials
    scheduleRenewCredentials(conf);
    // TODO Metrics
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#submitTopologyWithOpts")
  public void submitTopologyWithOpts(String stormName,
      String uploadedJarLocation, String serializedConf, StormTopology topology,
      SubmitOptions submitOptions)
          throws AlreadyAliveException, InvalidTopologyException, TException {
    LOG.info("Received topology submission for " + stormName
        + ", uploadedJarLocation:" + uploadedJarLocation + " with conf "
        + serializedConf);

    try {
      // throw exception when is not leader
      NimbusUtils.isLeader(nimbus, true);
      assert submitOptions != null;
      NimbusUtils.ValidTopologyName(stormName);
      NimbusUtils.checkAuthorization(nimbus, stormName, null, "submitTopology");
      NimbusUtils.checkStormActive(nimbus, stormName, false);
      Map topoConf = (Map) CoreUtil.from_json(serializedConf);
      try {
        ConfigValidation.validateFields(topoConf);
      } catch (IllegalArgumentException ex) {
        throw new InvalidTopologyException(ex.getMessage());

      }
      nimbus.getValidator().validate(stormName, topoConf, topology);
      String stormId =
          stormName + "-" + nimbus.getSubmittedCount().incrementAndGet() + "-"
              + Time.currentTimeSecs();
      Credentials creds = submitOptions.get_creds();
      Map<String, String> credentials = null;
      if (creds != null) {
        credentials = creds.get_creds();
      }

      topoConf.put(Config.STORM_ID, stormId);
      topoConf.put(Config.TOPOLOGY_NAME, stormName);
      Map stormConfSubmitted =
          NimbusUtils.normalizeConf(conf, topoConf, topology);

      ReqContext req = ReqContext.context();
      Principal principal = req.principal();

      Set<String> topoAcl = new HashSet<String>();
      String submitterPrincipal = null;
      if (principal != null) {
        submitterPrincipal = principal.toString();
        topoAcl.add(submitterPrincipal);
      }
      String submitterUser = principalToLocal.toLocal(principal);
      if (submitterUser != null) {
        topoAcl.add(submitterUser);
      }
      String systemUser = System.getProperty("user.name");
      List<String> topologyUsers =
          (ArrayList<String>) stormConfSubmitted.get(Config.TOPOLOGY_USERS);
      if (topologyUsers != null) {
        topoAcl.addAll(topologyUsers);
      }

      Map stormConf = new HashMap(stormConfSubmitted);
      if (submitterPrincipal != null) {
        stormConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, submitterPrincipal);
      } else {
        stormConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, "");
      }

      // Don't let the user set who we launch as
      if (submitterUser != null) {
        stormConf.put(Config.TOPOLOGY_SUBMITTER_USER, submitterUser);
      } else {
        stormConf.put(Config.TOPOLOGY_SUBMITTER_USER, systemUser);
      }

      stormConf.put(Config.TOPOLOGY_USERS, topoAcl);
      stormConf.put(Config.STORM_ZOOKEEPER_SUPERACL,
          conf.get(Config.STORM_ZOOKEEPER_SUPERACL));
      if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
        stormConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        stormConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
      }
      Map totalStormConf = new HashMap(conf);
      totalStormConf.putAll(stormConf);
      StormTopology normalizedTopology =
          NimbusUtils.normalizeTopology(totalStormConf, topology);
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      if (credentials != null) {
        for (INimbusCredentialPlugin nimbusAutocredPlugin : nimbus
            .getNimbusAutocredPlugins()) {
          nimbusAutocredPlugin.populateCredentials(credentials,
              Collections.unmodifiableMap(stormConf));

        }
      }
      if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER),
          false) && (submitterUser == null || submitterUser.trim().isEmpty())) {
        throw new AuthorizationException(
            "Could not determine the user to run this topology as.");
      }
      // this validates the structure of the topology
      StormCommon.systemTopology(totalStormConf, normalizedTopology);
      NimbusUtils.validateTopologySize(topoConf, conf, normalizedTopology);
      if (Utils.isZkAuthenticationConfiguredStormServer(conf)
          && !Utils.isZkAuthenticationConfiguredTopology(stormConf)) {
        throw new IllegalArgumentException(
            "The cluster is configured for zookeeper authentication, but no payload was provided.");
      }

      LOG.info("Received topology submission for {} with conf {}",
          new Object[] { stormId, stormConf });
      // lock protects against multiple topologies being submitted at once and
      // cleanup thread killing topology in b/w assignment and starting the
      // topology
      synchronized (nimbus.getSubmitLock()) {
        NimbusUtils.checkStormActive(nimbus, stormName, false);
        // cred-update-lock is not needed here because creds are being added for
        // the first time.
        if (credentials == null) {
          credentials = new HashMap<String, String>();
        }
        stormClusterState.setCredentials(stormId, new Credentials(credentials),
            stormConf);
        LOG.info("uploadedJar {}", uploadedJarLocation);
        NimbusUtils.setupStormCode(nimbus, conf, stormId, uploadedJarLocation,
            totalStormConf, normalizedTopology);
        NimbusUtils.waitForDesiredCodeReplication(nimbus, totalStormConf,
            stormId);
        stormClusterState.setupHeatbeats(stormId);
        stormClusterState.setupBackpressure(stormId);
        NimbusUtils.notifyTopologyActionListener(nimbus, stormId,
            "submitTopology");
        TopologyStatus status = NimbusUtils
            .thriftStatusToKwStatus(submitOptions.get_initial_status());
        NimbusUtils.startStorm(nimbus, stormName, stormId, status);
      }
    } catch (Throwable e) {
      LOG.error("Topology submission exception. (topology name='{}') for {}",
          stormName, CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new InvalidTopologyException(CoreUtil.stringifyError(e));
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#submitTopology")
  public void submitTopology(String stormName, String uploadedJarLocation,
      String serializedConf, StormTopology topology)
          throws AlreadyAliveException, InvalidTopologyException, TException {
    SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
    this.submitTopologyWithOpts(stormName, uploadedJarLocation, serializedConf,
        topology, options);
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#killTopology")
  public void killTopology(String name) throws NotAliveException, TException {
    this.killTopologyWithOpts(name, new KillOptions());
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#killTopologyWithOpts")
  public void killTopologyWithOpts(String stormName, KillOptions options)
      throws NotAliveException, TException {
    try {
      NimbusUtils.checkStormActive(nimbus, stormName, true);
      Map topologyConf =
          NimbusUtils.tryReadStormConfFromName(conf, stormName, nimbus);
      String operation = "killTopology";
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          operation);
      Integer wait_amt = null;
      if (options.is_set_wait_secs()) {
        wait_amt = options.get_wait_secs();
      }
      NimbusUtils.transitionName(nimbus, stormName, true, TopologyStatus.KILL,
          wait_amt);
      NimbusUtils.notifyTopologyActionListener(nimbus, stormName, operation);

      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      String stormId = StormCommon.getStormId(stormClusterState, stormName);
      NimbusUtils.addTopologyToHistoryLog(stormId, nimbus, topologyConf);
    } catch (Throwable e) {
      String errMsg = "Failed to kill topology " + stormName;
      LOG.error(errMsg, e);
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#rebalance")
  public void rebalance(String stormName, RebalanceOptions options)
      throws NotAliveException, TException, InvalidTopologyException {
    try {
      NimbusUtils.checkStormActive(nimbus, stormName, true);
      Map topologyConf =
          NimbusUtils.tryReadStormConfFromName(conf, stormName, nimbus);
      String operation = "rebalance";
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          operation);
      Integer waitAmt = null;
      Integer numWorkers = null;
      Map<String, Integer> executorOverrides = null;
      if (options != null) {
        if (options.is_set_wait_secs()) {
          waitAmt = options.get_wait_secs();
        }
        if (options.is_set_num_workers()) {
          numWorkers = options.get_num_workers();
        }
        if (options.is_set_num_executors()) {
          executorOverrides = options.get_num_executors();
        }
      }
      if (executorOverrides != null) {
        for (Integer numExecutor : executorOverrides.values()) {
          if (numExecutor <= 0) {
            throw new InvalidTopologyException(
                "Number of executors must be greater than 0");
          }
        }
      }
      NimbusUtils.transitionName(nimbus, stormName, true,
          TopologyStatus.REBALANCE, waitAmt, numWorkers, executorOverrides);
      NimbusUtils.notifyTopologyActionListener(nimbus, stormName, operation);
    } catch (Throwable e) {
      String errMsg = "Failed to rebalance topology " + stormName;
      LOG.error(errMsg, e);
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#activate")
  public void activate(String stormName) throws NotAliveException, TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConfFromName(conf, stormName, nimbus);
      String operation = "activate";
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          operation);
      NimbusUtils.transitionName(nimbus, stormName, true,
          TopologyStatus.ACTIVATE);
      NimbusUtils.notifyTopologyActionListener(nimbus, stormName, operation);
    } catch (Throwable e) {
      String errMsg = "Failed to active topology " + stormName;
      LOG.error(errMsg, e);
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#deactivate")
  public void deactivate(String stormName)
      throws NotAliveException, TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConfFromName(conf, stormName, nimbus);
      String operation = "deactivate";
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          operation);
      NimbusUtils.transitionName(nimbus, stormName, true,
          TopologyStatus.INACTIVATE);
      NimbusUtils.notifyTopologyActionListener(nimbus, stormName, operation);
    } catch (Throwable e) {
      String errMsg = "Failed to deactivate topology " + stormName;
      LOG.error(errMsg, e);
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(errMsg);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#uploadNewCredentials")
  public void uploadNewCredentials(String stormName, Credentials credentials)
      throws NotAliveException, InvalidTopologyException,
      AuthorizationException, TException {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    try {
      String stormId = StormCommon.getStormId(stormClusterState, stormName);
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, stormId, nimbus.getBlobStore());
      Credentials creds = new Credentials();
      if (credentials != null) {
        creds.set_creds(credentials.get_creds());
      } else {
        creds.set_creds(new HashMap<String, String>());
      }
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "uploadNewCredentials");
      synchronized (nimbus.getCredUpdateLock()) {
        stormClusterState.setCredentials(stormId, creds, topologyConf);
      }
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#beginFileUpload")
  public String beginFileUpload() throws TException {
    String fileLoc = null;
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null, "fileUpload");
      fileLoc =
          ConfigUtils.masterInbox(conf) + "/stormjar-" + Utils.uuid() + ".jar";
      nimbus.getUploaders().put(fileLoc,
          Channels.newChannel(new FileOutputStream(fileLoc)));
      LOG.info("Uploading file from client to " + fileLoc);
    } catch (FileNotFoundException e) {
      LOG.error(" file not found " + fileLoc);
      throw new TException(e);
    } catch (IOException e) {
      LOG.error(" IOException  " + fileLoc, e);
      throw new TException(e);
    } catch (Throwable t) {
      String errMsg = "Failed to upload file " + fileLoc;
      LOG.error(errMsg, CoreUtil.stringifyError(t));
      if (t instanceof TException) {
        throw (TException) t;
      }
      throw new TException(t);
    }
    return fileLoc;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#uploadChunk")
  public void uploadChunk(String location, ByteBuffer chunk) throws TException {
    NimbusUtils.checkAuthorization(nimbus, null, null, "fileUpload");
    FileCacheMap<Object, Object> uploaders = nimbus.getUploaders();
    Object obj = uploaders.get(location);
    if (obj == null) {
      throw new TException(
          "File for that location does not exist (or timed out) " + location);
    }
    try {
      if (obj instanceof WritableByteChannel) {
        WritableByteChannel channel = (WritableByteChannel) obj;
        channel.write(chunk);
        uploaders.put(location, channel);
      } else {
        throw new TException(
            "Object isn't WritableByteChannel for " + location);
      }
    } catch (IOException e) {
      String errMsg =
          " WritableByteChannel write filed when uploadChunk " + location;
      LOG.error(errMsg);
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#finishFileUpload")
  public void finishFileUpload(String location) throws TException {
    NimbusUtils.checkAuthorization(nimbus, null, null, "fileUpload");
    FileCacheMap<Object, Object> uploaders = nimbus.getUploaders();
    Object obj = uploaders.get(location);
    if (obj == null) {
      throw new TException(
          "File for that location does not exist (or timed out)");
    }
    try {
      if (obj instanceof WritableByteChannel) {
        WritableByteChannel channel = (WritableByteChannel) obj;
        channel.close();
        uploaders.remove(location);
        LOG.info("Finished uploading file from client: " + location);
      } else {
        throw new TException(
            "Object isn't WritableByteChannel for " + location);
      }
    } catch (IOException e) {
      LOG.error(" WritableByteChannel close failed when finishFileUpload "
          + location);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#beginFileDownload")
  public String beginFileDownload(String file) throws TException {
    NimbusUtils.checkAuthorization(nimbus, null, null, "fileDownload");
    Integer bufferSize = Utils.getInt(nimbus.getConf()
        .get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), 65536);
    BufferInputStream is = new BufferInputStream(
        nimbus.getBlobStore().getBlob(file, null), bufferSize);
    String id = Utils.uuid();
    nimbus.getDownloaders().put(id, is);
    return id;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#downloadChunk")
  public ByteBuffer downloadChunk(String id) throws TException {
    NimbusUtils.checkAuthorization(nimbus, null, null, "fileDownload");
    FileCacheMap<Object, Object> downloaders = nimbus.getDownloaders();
    Object obj = downloaders.get(id);
    if (obj == null || !(obj instanceof BufferFileInputStream)) {
      throw new TException("Could not find input stream for that id");
    }
    BufferFileInputStream is = (BufferFileInputStream) obj;
    byte[] ret = null;
    try {
      ret = is.read();
      if (ret != null) {
        downloaders.put(id, (BufferFileInputStream) is);
      } else {
        downloaders.remove(id);
        return null;
      }
    } catch (IOException e) {
      LOG.error(e + "BufferFileInputStream read failed when downloadChunk ");
      throw new TException(e);
    }
    return ByteBuffer.wrap(ret);
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getClusterInfo")
  public ClusterSummary getClusterInfo() throws TException {
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null, "getClusterInfo");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      Map<String, SupervisorInfo> supervisorInfos =
          NimbusUtils.allSupervisorInfo(stormClusterState);
      // TODO: need to get the port info about supervisors...
      // in standalone just look at metadata, otherwise just say N/A?
      List<SupervisorSummary> supervisorSummaries = NimbusUtils
          .mkSupervisorSummaries(nimbus, stormClusterState, supervisorInfos);
      int nimbusUptime = nimbus.getUptime().upTime();
      Map<String, StormBase> bases =
          StormCommon.topologyBases(stormClusterState);
      List<TopologySummary> topologySummaries =
          NimbusUtils.mkTopologySummaries(nimbus, stormClusterState, bases);

      List<NimbusSummary> nimbuses = stormClusterState.nimbuses();
      // update the isLeader field for each nimbus summary
      NimbusInfo leader = nimbus.getLeaderElector().getLeader();
      String leaderHost = leader.getHost();
      int leaderPort = leader.getPort();
      for (NimbusSummary nimbusSummary : nimbuses) {
        nimbusSummary
            .set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
        nimbusSummary.set_isLeader(nimbusSummary.get_host().equals(leaderHost)
            && nimbusSummary.get_port() == leaderPort);
      }
      ClusterSummary ret =
          new ClusterSummary(supervisorSummaries, topologySummaries, nimbuses);
      ret.set_nimbus_uptime_secs(nimbusUptime);
      return ret;
    } catch (Exception e) {
      LOG.error("Failed to get ClusterSummary ", CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  public NimbusSummary getNimbusLeader()
      throws AuthorizationException, TException {
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null, "getNimbusLeader");
      NimbusInfo leader = nimbus.getLeaderElector().getLeader();
      String leaderHost = leader.getHost();
      int leaderPort = leader.getPort();
      NimbusSummary ret =
          new NimbusSummary(leaderHost, leaderPort, -1, true, "");
      return ret;
    } catch (Exception e) {
      LOG.error("Failed to get NimbusLeader ", CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyInfo")
  public TopologyInfo getTopologyInfo(String stormId)
      throws NotAliveException, TException {
    GetInfoOptions options = new GetInfoOptions();
    options.set_num_err_choice(NumErrorsChoice.ALL);
    return this.getTopologyInfoWithOpts(stormId, options);
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyConf")
  public String getTopologyConf(String id)
      throws NotAliveException, TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, id, nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "getTopologyConf");
      return JSONValue.toJSONString(topologyConf);
    } catch (Exception e) {
      LOG.error("Failed to get getTopologyConf ", e);
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  public String getSupervisorConf(String id)
      throws NotAliveException, TException {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    SupervisorInfo supervisorInfo = null;
    try {
      supervisorInfo = stormClusterState.supervisorInfo(id);
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
    return supervisorInfo.get_supervisor_conf();
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopology")
  public StormTopology getTopology(String id)
      throws NotAliveException, TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, id, nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "getTopology");
      StormTopology stormtopology =
          NimbusUtils.tryReadStormTopology(id, nimbus.getBlobStore());
      return StormCommon.systemTopology(topologyConf, stormtopology);
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down master");
    try {
      nimbus.getTimer().close();
    } catch (Exception e) {
      LOG.error("close time Exception {}", CoreUtil.stringifyError(e));
    }
    try {
      nimbus.getStormClusterState().disconnect();

    } catch (Exception e) {
      LOG.error("disconnect zk Exception {}", CoreUtil.stringifyError(e));
    }
    try {
      nimbus.getDownloaders().cleanup();
    } catch (Exception e) {
      LOG.error("cleanup Downloaders Exception {}", CoreUtil.stringifyError(e));
    }
    try {
      nimbus.getUploaders().cleanup();
    } catch (Exception e) {
      LOG.error("cleanup Uploaders Exception {}", CoreUtil.stringifyError(e));
    }
    try {
      nimbus.getBlobStore().shutdown();
    } catch (Exception e) {
      LOG.error("shutdown BlobStore Exception {}", CoreUtil.stringifyError(e));
    }
    try {
      nimbus.getLeaderElector().close();
    } catch (Exception e) {
      LOG.error("close LeaderElector Exception {}", CoreUtil.stringifyError(e));
    }
    if (nimbus.getNimbusTopologyActionNotifier() != null) {
      nimbus.getNimbusTopologyActionNotifier().cleanup();
    }
    LOG.info("Shut down master");
  }

  @Override
  public boolean isWaiting() {
    return nimbus.getTimer().isTimerWaiting();
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getNimbusConf")
  public String getNimbusConf() throws TException {
    NimbusUtils.checkAuthorization(nimbus, null, null, "getNimbusConf");
    return JSONValue.toJSONString(nimbus.getConf());
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getUserTopology")
  public StormTopology getUserTopology(String id)
      throws NotAliveException, TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, id, nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "getUserTopology");
      return NimbusUtils.tryReadStormTopology(id, nimbus.getBlobStore());
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  /**
   * Schedule Nimbus monitor
   * 
   * @param conf
   * @throws Exception
   */
  private void scheduleNimbusMonitor(Map conf) throws Exception {
    int monitorFreqSecs =
        Utils.getInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
    Runnable monitor = new MonitorRunnable(nimbus);
    nimbus.getTimer().scheduleRecurring(0, monitorFreqSecs, monitor);
    LOG.info("Successfully init Monitor thread");
  }

  /**
   * Schedule Nimbus inbox cleaner Right now, every 600 seconds, nimbus will
   * clean jar under /LOCAL-DIR/nimbus/inbox, which is the uploading topology
   * directory
   * 
   * @param conf
   * @throws IOException
   */
  private void scheduleNimbusInboxCleaner(Map conf) throws IOException {
    // Schedule Nimbus inbox cleaner
    String dirLocation = ConfigUtils.masterInbox(conf);
    int inboxJarExpirationSecs =
        Utils.getInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
    CleanInboxRunnable cleanInBoxRunnable =
        new CleanInboxRunnable(dirLocation, inboxJarExpirationSecs);
    int cleanupInboxFreqSecs =
        Utils.getInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);
    nimbus.getTimer().scheduleRecurring(0, cleanupInboxFreqSecs,
        cleanInBoxRunnable);
    LOG.info("Successfully init " + dirLocation + " cleaner");
  }

  /**
   * Schedule nimbus code sync thread to sync code from other nimbuses.
   * 
   * @param conf
   * @param blobStore
   */
  private void scheduleNimbusCodeSyncThread(Map conf, BlobStore blobStore) {
    if (blobStore instanceof LocalFsBlobStore) {
      BlobSyncRunnableCallback blobSync =
          new BlobSyncRunnableCallback(conf, nimbus);
      int nimbusCodeSyncFreqSecs =
          Utils.getInt(conf.get(Config.NIMBUS_CODE_SYNC_FREQ_SECS), 120);
      nimbus.getTimer().scheduleRecurring(0, nimbusCodeSyncFreqSecs, blobSync);

    }
  }

  /**
   * Schedule topology history cleaner
   * 
   * @param conf
   */
  private void scheduleTopologyHistoryCleaner(Map conf) {
    int logviewerCleanupIntervalSecs =
        Utils.getInt(conf.get(Config.LOGVIEWER_CLEANUP_INTERVAL_SECS), 120);
    Runnable cleanTopologyHistory =
        new CleanTopologyHistoryRunnableCallback(conf, nimbus);
    nimbus.getTimer().scheduleRecurring(0, logviewerCleanupIntervalSecs,
        cleanTopologyHistory);
  }

  /**
   * Schedule renew credentials
   * 
   * @param conf
   */
  private void scheduleRenewCredentials(Map conf) {
    int nimbusCredentialRenewFreqSecs =
        Utils.getInt(conf.get(Config.NIMBUS_CREDENTIAL_RENEW_FREQ_SECS), 600);
    Runnable renewCredentials = new RenewCredentialsrRunnableCallBack(nimbus);
    nimbus.getTimer().scheduleRecurring(0, nimbusCredentialRenewFreqSecs,
        renewCredentials);
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyInfoWithOpts")
  public TopologyInfo getTopologyInfoWithOpts(String stormId,
      GetInfoOptions options)
          throws NotAliveException, AuthorizationException, TException {
    CommonTopoInfo info = getCommonTopoInfo(stormId, "getTopologyInfoWithOpts");
    String stormName = info.getStormName();
    IStormClusterState stormClusterState = info.getStormClusterState();
    Set<String> allComponents = info.getAllComponents();
    int launchTimeSecs = info.getLaunchTimeSecs();
    Assignment assignment = info.getAssignment();
    Map<ExecutorInfo, ExecutorCache> beats = info.getBeats();
    Map<Integer, String> taskToComponent = info.getTaskToComponent();
    StormBase base = info.getBase();

    NumErrorsChoice numErrChoice = options.get_num_err_choice();
    if (numErrChoice == null) {
      numErrChoice = NumErrorsChoice.ALL;
    }

    try {
      Map<String, List<ErrorInfo>> componentToErrors =
          stormClusterState.errorsByStormId(stormId);
      Map<String, ErrorInfo> componentToLastError =
          stormClusterState.lastErrorByStormId(stormId);
      Map<String, List<ErrorInfo>> errors =
          new HashMap<String, List<ErrorInfo>>();
      for (String c : allComponents) {
        errors.put(c, NimbusUtils.errorsFnByStormId(numErrChoice, c,
            componentToErrors, componentToLastError));
      }
      Map<List<Long>, NodeInfo> executorToNodeports =
          assignment.get_executor_node_port();
      List<ExecutorSummary> executorSummaries =
          new ArrayList<ExecutorSummary>();
      if (beats != null) {
        for (Map.Entry<List<Long>, NodeInfo> executorToNodeport : executorToNodeports
            .entrySet()) {
          List<Long> list = executorToNodeport.getKey();
          ExecutorInfo executor = new ExecutorInfo(list.get(0).intValue(),
              list.get(list.size() - 1).intValue());
          NodeInfo nodeToPort = executorToNodeport.getValue();
          String host = assignment.get_node_host().get(nodeToPort.get_node());
          ExecutorCache heartbeat = beats.get(executor);
          if (heartbeat.getHeartbeat() == null) {
            ExecutorSummary executorSummary = new ExecutorSummary(executor,
                taskToComponent.get(executor.get_task_start()), host,
                nodeToPort.get_port_iterator().next().intValue(), 0);
            executorSummaries.add(executorSummary);
          } else {
            ExecutorStats executorStats = heartbeat.getHeartbeat().getStats();
            ExecutorSummary executorSummary = new ExecutorSummary(executor,
                taskToComponent.get(executor.get_task_start()), host,
                nodeToPort.get_port_iterator().next().intValue(),
                heartbeat.getHeartbeat().getUptime());
            executorSummary.set_stats(executorStats);
            executorSummaries.add(executorSummary);
          }
        }
      }

      TopologyInfo topoInfo =
          new TopologyInfo(stormId, stormName, Time.deltaSecs(launchTimeSecs),
              executorSummaries, NimbusUtils.extractStatusStr(base), errors);
      topoInfo.set_owner(base.get_owner());
      String schedStatus = nimbus.getIdToSchedStatus().get(stormId);
      if (schedStatus != null) {
        topoInfo.set_sched_status(schedStatus);
      }

      Double[] resources = nimbus.getIdToResources().get(stormId);
      if (resources != null && resources.length >= 6) {
        topoInfo.set_requested_memonheap(resources[0]);
        topoInfo.set_requested_memoffheap(resources[1]);
        topoInfo.set_requested_cpu(resources[2]);
        topoInfo.set_assigned_memonheap(resources[3]);
        topoInfo.set_assigned_memoffheap(resources[4]);
        topoInfo.set_assigned_cpu(resources[5]);
      }

      topoInfo.set_component_debug(base.get_component_debug());
      topoInfo.set_replication_count(NimbusUtils.getBlobReplicationCount(
          ConfigUtils.masterStormCodeKey(stormId), nimbus));
      return topoInfo;
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#setLogConfig")
  public void setLogConfig(String id, LogConfig config) throws TException {
    try {
      Map topologyConf = NimbusUtils.tryReadStormConf(nimbus.getConf(), id,
          nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "setLogConfig");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();

      LogConfig mergedLogConfig = stormClusterState.topologyLogConfig(id, null);
      if (mergedLogConfig == null) {
        mergedLogConfig = new LogConfig();
      }
      Map<String, LogLevel> namedLoggers =
          mergedLogConfig.get_named_logger_level();
      for (Map.Entry<String, LogLevel> tmpNamedLogger : namedLoggers
          .entrySet()) {
        LogLevel level = tmpNamedLogger.getValue();
        level.set_action(LogLevelAction.UNCHANGED);
      }
      for (Map.Entry<String, LogLevel> tmpNamedLogger : config
          .get_named_logger_level().entrySet()) {
        String loggerName = tmpNamedLogger.getKey();
        LogLevel logConfig = tmpNamedLogger.getValue();
        LogLevelAction action = logConfig.get_action();
        if (StringUtils.isBlank(loggerName)) {
          throw new RuntimeException(
              "Named loggers need a valid name. Use root for the root logger");
        }
        if (action == LogLevelAction.UPDATE) {
          NimbusUtils.setLoggerTimeOuts(logConfig);
          mergedLogConfig.put_to_named_logger_level(loggerName, logConfig);
        } else if (action == LogLevelAction.REMOVE) {
          namedLoggers = mergedLogConfig.get_named_logger_level();
          if (namedLoggers != null && namedLoggers.containsKey(loggerName)) {
            namedLoggers.remove(loggerName);
          }
        }
      }
      LOG.info("Setting log config for " + stormName + ":"
          + mergedLogConfig.toString());
      stormClusterState.setTopologyLogConfig(id, mergedLogConfig);
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getLogConfig")
  public LogConfig getLogConfig(String id) throws TException {
    try {
      Map topologyConf = NimbusUtils.tryReadStormConf(nimbus.getConf(), id,
          nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "getLogConfig");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();

      LogConfig logConfig = stormClusterState.topologyLogConfig(id, null);
      if (logConfig == null) {
        logConfig = new LogConfig();
      }
      return logConfig;
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#debug")
  public void debug(String name, String component, boolean enable,
      double samplingPercentage)
          throws NotAliveException, AuthorizationException, TException {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    try {
      String stormId = StormCommon.getStormId(stormClusterState, name);
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, stormId, nimbus.getBlobStore());
      // make sure samplingPct is within bounds.
      double spct = Math.max(Math.min(samplingPercentage, 100.0), 0.0);
      // while disabling we retain the sampling pct.

      DebugOptions debugOptions = new DebugOptions();
      debugOptions.set_enable(enable);
      if (enable) {
        debugOptions.set_samplingpct(spct);
      }
      Map<String, DebugOptions> componentToDebug =
          new HashMap<String, DebugOptions>();
      if (StringUtils.isEmpty(component)) {
        componentToDebug.put(stormId, debugOptions);
      } else {
        componentToDebug.put(component, debugOptions);
      }
      StormBase stormBaseUpdates = new StormBase();
      stormBaseUpdates.set_component_debug(componentToDebug);
      NimbusUtils.checkAuthorization(nimbus, name, topologyConf, "debug");
      if (StringUtils.isEmpty(stormId)) {
        throw new NotAliveException(name);
      }
      String logMsg = "Nimbus setting debug to " + enable + " for storm-name '"
          + name + "' storm-id '" + stormId + "' sampling pct '" + spct + "'";
      if (!StringUtils.isBlank(component)) {
        logMsg += " component-id " + component;
      }
      LOG.info(logMsg);
      synchronized (nimbus.getSubmitLock()) {
        stormClusterState.updateStorm(stormId, stormBaseUpdates);
      }
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#setWorkerProfiler")
  public void setWorkerProfiler(String id, ProfileRequest profileRequest)
      throws TException {
    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, id, nimbus.getBlobStore());
      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);
      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          "setWorkerProfiler");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      stormClusterState.setWorkerProfileRequest(id, profileRequest);
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getComponentPendingProfileActions")
  public List<ProfileRequest> getComponentPendingProfileActions(String id,
      String component_id, ProfileAction action) throws TException {
    try {
      CommonTopoInfo info =
          this.getCommonTopoInfo(id, "getComponentPendingProfileActions");
      IStormClusterState stormClusterState = info.getStormClusterState();
      Map<Integer, String> taskToComponent = info.getTaskToComponent();
      Assignment assignment = info.getAssignment();
      Map<List<Long>, NodeInfo> executorToNodePort =
          assignment.get_executor_node_port();
      Map<String, String> nodeToHost = assignment.get_node_host();
      Map<ExecutorInfo, NodeInfo> executorToHostPort =
          new HashMap<ExecutorInfo, NodeInfo>();
      for (Map.Entry<List<Long>, NodeInfo> tmpEntry : executorToNodePort
          .entrySet()) {
        List<Long> list = tmpEntry.getKey();
        ExecutorInfo executor = new ExecutorInfo(list.get(0).intValue(),
            list.get(list.size() - 1).intValue());
        NodeInfo nodeInfo = tmpEntry.getValue();
        NodeInfo newNodeInfo = new NodeInfo();
        newNodeInfo.set_node(nodeToHost.get(nodeInfo.get_node()));
        newNodeInfo.set_port(nodeInfo.get_port());
        executorToHostPort.put(executor, newNodeInfo);
      }
      List<NodeInfo> nodeInfos = Stats.extractNodeinfosFromHbForComp(
          executorToHostPort, taskToComponent, false, component_id);
      List<ProfileRequest> allPendingActionsForTopology =
          stormClusterState.getTopologyProfileRequests(id);
      List<ProfileRequest> latestProfileActions =
          new ArrayList<ProfileRequest>();
      for (NodeInfo nodeInfo : nodeInfos) {
        List<ProfileRequest> fiterList = new ArrayList<ProfileRequest>();
        for (ProfileRequest request : allPendingActionsForTopology) {
          if (nodeInfo.equals(request.get_nodeInfo())
              && (action != null && action.equals(request.get_action()))) {
            fiterList.add(request);
          }
        }
        if (fiterList.size() == 1) {
          latestProfileActions.add(fiterList.get(0));
        } else if (fiterList.size() > 1) {
          Collections.sort(fiterList, new Comparator<ProfileRequest>() {
            @Override
            public int compare(ProfileRequest arg1, ProfileRequest arg2) {
              return 0 - Long.valueOf(arg1.get_time_stamp())
                  .compareTo(arg2.get_time_stamp());
            }
          });
          latestProfileActions.add(fiterList.get(0));
        }
      }
      String logMsg = "Latest profile actions for topology " + id
          + " component " + component_id;
      for (ProfileRequest request : latestProfileActions) {
        logMsg += " " + request.toString();
      }
      LOG.info(logMsg);
      return latestProfileActions;
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#beginCreateBlob")
  public String beginCreateBlob(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyAlreadyExistsException, TException {
    String sessionId = Utils.uuid();
    Subject subject = ReqContext.context().subject();
    nimbus.getBlobUploaders().put(sessionId,
        nimbus.getBlobStore().createBlob(key, meta, subject));
    LOG.info("Created blob for " + key + " with session id" + sessionId);
    return sessionId;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#beginUpdateBlob")
  public String beginUpdateBlob(String key)
      throws AuthorizationException, KeyNotFoundException, TException {
    String sessionId = Utils.uuid();
    Subject subject = ReqContext.context().subject();
    AtomicOutputStream os = nimbus.getBlobStore().updateBlob(key, subject);
    nimbus.getBlobUploaders().put(sessionId, os);
    LOG.info(
        "Created upload session for " + key + " with session id" + sessionId);
    return sessionId;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#uploadBlobChunk")
  public void uploadBlobChunk(String session, ByteBuffer chunk)
      throws AuthorizationException, TException {
    MkBlobCacheMap<Object, Object> uploaders = nimbus.getBlobUploaders();
    AtomicOutputStream os = (AtomicOutputStream) uploaders.get(session);
    byte[] chunkArray = chunk.array();
    int remaining = chunk.remaining();
    int arrayOffSet = chunk.arrayOffset();
    int position = chunk.position();
    try {
      os.write(chunkArray, arrayOffSet + position, remaining);
      uploaders.put(session, os);
    } catch (IOException e) {
      throw new RuntimeException("Blob for session" + session
          + "does not exist or time out " + e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#finishBlobUpload")
  public void finishBlobUpload(String session)
      throws AuthorizationException, TException {
    AtomicOutputStream os =
        (AtomicOutputStream) nimbus.getBlobUploaders().get(session);
    try {
      os.close();
      LOG.info("Finished uploading blob for session " + session
          + ". Closing session.");
      nimbus.getBlobUploaders().remove(session);
    } catch (IOException e) {
      throw new RuntimeException("Blob for session" + session
          + "does not exist or time out " + e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#cancelBlobUpload")
  public void cancelBlobUpload(String session)
      throws AuthorizationException, TException {
    AtomicOutputStream os =
        (AtomicOutputStream) nimbus.getBlobUploaders().get(session);
    try {
      os.cancel();
      LOG.info("Canceled uploading blob for session " + session
          + ". Closing session.");
      nimbus.getBlobUploaders().remove(session);
    } catch (IOException e) {
      throw new RuntimeException("Blob for session" + session
          + "does not exist or time out " + e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getBlobMeta")
  public ReadableBlobMeta getBlobMeta(String key)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject subject = ReqContext.context().subject();
    ReadableBlobMeta blobMeta = nimbus.getBlobStore().getBlobMeta(key, subject);
    return blobMeta;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#setBlobMeta")
  public void setBlobMeta(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject subject = ReqContext.context().subject();
    nimbus.getBlobStore().setBlobMeta(key, meta, subject);
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#beginBlobDownload")
  public BeginDownloadResult beginBlobDownload(String key)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject who = ReqContext.context().subject();
    InputStreamWithMeta is = nimbus.getBlobStore().getBlob(key, who);
    String sessionId = Utils.uuid();
    BeginDownloadResult ret = null;
    try {
      ret = new BeginDownloadResult(is.getVersion(), sessionId);
      long setMetaSize = is.getFileLength();
      ret.set_data_size(setMetaSize);
      int size = Utils.getInt(
          conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES),
          65536);
      nimbus.getBlobDownloaders().put(sessionId,
          new BufferInputStream(is, size));
      LOG.info("Created download session for " + key + " with id " + sessionId);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
    return ret;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#downloadBlobChunk")
  public ByteBuffer downloadBlobChunk(String session)
      throws AuthorizationException, TException {
    MkBlobCacheMap downloaders = nimbus.getBlobDownloaders();
    BufferInputStream is = (BufferInputStream) downloaders.get(session);
    if (is == null) {
      throw new RuntimeException(
          "Could not find input stream for session " + session);
    }
    try {
      byte[] ret = is.read();
      downloaders.put(session, is);
      if (ret == null || ret.length < 1) {
        is.close();
        downloaders.remove(session);
      }
      boolean isDebug =
          Utils.getBoolean(conf.get(Config.TOPOLOGY_DEBUG), false);
      if (isDebug) {
        LOG.info("Sending " + ret.length + " bytes");
      }
      return ByteBuffer.wrap(ret);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#deleteBlob")
  public void deleteBlob(String key)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject who = ReqContext.context().subject();
    BlobStore blobStore = nimbus.getBlobStore();
    blobStore.deleteBlob(key, who);
    if (blobStore instanceof LocalFsBlobStore) {
      try {
        nimbus.getStormClusterState().removeBlobstoreKey(key);
        nimbus.getStormClusterState().removeKeyVersion(key);
        LOG.info("Deleted blob for key " + key);
      } catch (Exception e) {
        if (e instanceof TException) {
          throw (TException) e;
        }
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#listBlobs")
  public ListBlobsResult listBlobs(String session) throws TException {
    MkBloblistCacheMap listers = nimbus.getBlobListers();
    Iterator<String> keysIt = null;
    // Create a new session id if the user gave an empty session string.
    // This is the use case when the user wishes to list blobs
    // starting from the beginning.
    if (StringUtils.isBlank(session)) {
      keysIt = nimbus.getBlobStore().listKeys();
      session = Utils.uuid();
      LOG.info("Creating new session for downloading list " + session);
    } else {
      keysIt = (Iterator<String>) listers.get(session);
      if (keysIt == null) {
        throw new RuntimeException("Blob list for session " + session
            + " does not exist (or timed out)");
      }
    }
    ListBlobsResult blobResult = null;
    List listChunk = new ArrayList();
    if (!keysIt.hasNext()) {
      listers.remove(session);
      LOG.info("No more blobs to list for session " + session);
      // A blank result communicates that there are no more blobs.
      blobResult = new ListBlobsResult(new ArrayList<String>(0), session);
    } else {
      int count = 0;
      // Limit to next 100 keys
      while (keysIt.hasNext() && count < 100) {
        listChunk.add(keysIt.next());
        count++;
      }
      LOG.info(session + " downloading " + listChunk.size() + " entries");
      listers.put(session, keysIt);
      blobResult = new ListBlobsResult(listChunk, session);
    }
    return blobResult;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getBlobReplication")
  public int getBlobReplication(String key)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject who = ReqContext.context().subject();
    try {
      return nimbus.getBlobStore().getBlobReplication(key, who);
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#updateBlobReplication")
  public int updateBlobReplication(String key, int replication)
      throws AuthorizationException, KeyNotFoundException, TException {
    Subject who = ReqContext.context().subject();
    try {
      return nimbus.getBlobStore().updateBlobReplication(key, replication, who);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#createStateInZookeeper")
  public void createStateInZookeeper(String key) throws TException {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    BlobStore blobStore = nimbus.getBlobStore();
    NimbusInfo nimbusHostPortInfo = nimbus.getNimbusHostPortInfo();
    Map conf = nimbus.getConf();
    if (blobStore instanceof LocalFsBlobStore) {
      try {
        stormClusterState.setupBlobstore(key, nimbusHostPortInfo,
            NimbusUtils.getVersionForKey(key, nimbusHostPortInfo, conf));
        LOG.debug("Created state in zookeeper " + stormClusterState.toString()
            + blobStore.toString() + nimbusHostPortInfo.toHostPortString());
      } catch (Exception e) {
        if (e instanceof TException) {
          throw (TException) e;
        }
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyPageInfo")
  public TopologyPageInfo getTopologyPageInfo(String stormId, String window,
      boolean isIncludeSys)
          throws NotAliveException, AuthorizationException, TException {
    CommonTopoInfo info = getCommonTopoInfo(stormId, "getTopologyPageInfo");

    if (info.getAssignment() == null) {
      throw new NotAliveException(
          "Topology is not alive now, wait until alive! name:"
              + info.getStormName() + " id:" + stormId);
    }
    if (info.getBeats() == null) {
      throw new NotAliveException(
          "Topology has not stats now, wait until has stats! name:"
              + info.getStormName() + " id:" + stormId);
    }

    Map<List<Long>, NodeInfo> execToNodePort =
        info.getAssignment().get_executor_node_port();

    // 1 aggregate executor statics into topology page info.
    TopologyPageInfo topologyPageInfo = new TopologyPageInfo(stormId);
    Stats.aggTopoExecsStats(topologyPageInfo, stormId, nimbus, execToNodePort,
        info.getTaskToComponent(), info.getBeats(), info.getTopology(), window,
        isIncludeSys);

    // 2 owner, scheduler status, resource, name, status, uptime, config,
    // replication, debug options
    if (info.getBase() != null && info.getBase().get_owner() != null) {
      topologyPageInfo.set_owner(info.getBase().get_owner());
    }

    String schedStatus = nimbus.getIdToSchedStatus().get(stormId);
    if (schedStatus != null) {
      topologyPageInfo.set_sched_status(schedStatus);
    }

    Double[] resources = nimbus.getIdToResources().get(stormId);
    if (resources != null && resources.length >= 6) {
      topologyPageInfo.set_requested_memonheap(resources[0]);
      topologyPageInfo.set_requested_memoffheap(resources[1]);
      topologyPageInfo.set_requested_cpu(resources[2]);
      topologyPageInfo.set_assigned_memonheap(resources[3]);
      topologyPageInfo.set_assigned_memoffheap(resources[4]);
      topologyPageInfo.set_assigned_cpu(resources[5]);
    }

    topologyPageInfo.set_name(info.getStormName());
    topologyPageInfo.set_status(NimbusUtils.extractStatusStr(info.getBase()));
    topologyPageInfo.set_uptime_secs(Time.deltaSecs(info.getLaunchTimeSecs()));

    Map topologyConf;
    try {
      topologyConf =
          NimbusUtils.tryReadStormConf(conf, stormId, nimbus.getBlobStore());
    } catch (IOException e) {
      throw new TException(e);
    }
    topologyPageInfo.set_topology_conf(JSONValue.toJSONString(topologyConf));

    try {
      topologyPageInfo
          .set_replication_count(NimbusUtils.getBlobReplicationCount(
              ConfigUtils.masterStormCodeKey(stormId), nimbus));
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }

    Map<String, DebugOptions> componentDebug =
        info.getBase().get_component_debug();
    if (componentDebug != null && componentDebug.containsKey(stormId)) {
      topologyPageInfo.set_debug_options(componentDebug.get(stormId));
    }

    return topologyPageInfo;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getComponentPageInfo")
  public ComponentPageInfo getComponentPageInfo(String topologyId,
      String component_id, String window, boolean isIncludeSys)
          throws NotAliveException, AuthorizationException, TException {

    CommonTopoInfo info = getCommonTopoInfo(topologyId, "getComponentPageInfo");
    Assignment assignment = info.getAssignment();
    Map<List<Long>, NodeInfo> executorToNodePort =
        assignment.get_executor_node_port();
    Map<ExecutorInfo, NodeInfo> executorToHostPort =
        new HashMap<ExecutorInfo, NodeInfo>();
    Map<String, String> nodeToHost = assignment.get_node_host();
    for (Map.Entry<List<Long>, NodeInfo> tmpEntry : executorToNodePort
        .entrySet()) {
      List<Long> list = tmpEntry.getKey();
      ExecutorInfo executor = new ExecutorInfo(list.get(0).intValue(),
          list.get(list.size() - 1).intValue());
      NodeInfo nodeInfo = tmpEntry.getValue();
      NodeInfo newNodeInfo = new NodeInfo();
      newNodeInfo.set_node(nodeToHost.get(nodeInfo.get_node()));
      newNodeInfo.set_port(nodeInfo.get_port());
      executorToHostPort.put(executor, newNodeInfo);
    }

    ComponentPageInfo componentPageInfo = new ComponentPageInfo();
    Stats.aggCompExecsStats(componentPageInfo, topologyId, executorToNodePort,
        info.getTaskToComponent(), info.getBeats(), info.getTopology(), window,
        isIncludeSys, component_id);

    componentPageInfo.set_topology_name(info.getStormName());

    List<ErrorInfo> errors;
    try {
      errors = NimbusUtils.getErrors(info.getStormClusterState(), topologyId,
          component_id);
    } catch (Exception e1) {
      throw new TException(e1);
    }
    componentPageInfo.set_errors(errors);

    componentPageInfo
        .set_topology_status(NimbusUtils.extractStatusStr(info.getBase()));

    componentPageInfo.set_debug_options(
        info.getBase().get_component_debug().get(component_id));
    Map<String, List<Integer>> componentToTask =
        Utils.reverseMap(info.getTaskToComponent());
    if (componentToTask.containsKey(StormCommon.EVENTLOGGER_COMPONENT_ID)) {
      List<Integer> eventloggerTasks =
          componentToTask.get(StormCommon.EVENTLOGGER_COMPONENT_ID);
      int taskIndex =
          Math.abs(TupleUtils.listHashCode(Arrays.asList(component_id))
              % eventloggerTasks.size());
      int taskId = eventloggerTasks.get(taskIndex);
      for (Map.Entry<ExecutorInfo, NodeInfo> entry : executorToHostPort
          .entrySet()) {
        ExecutorInfo executorInfo = entry.getKey();
        NodeInfo nodeInfo = entry.getValue();
        if (executorInfo.get_task_start() <= taskId
            && executorInfo.get_task_end() >= taskId) {
          componentPageInfo.set_eventlog_host(nodeInfo.get_node());
          componentPageInfo.set_eventlog_port(
              nodeInfo.get_port().iterator().next().intValue());
          break;
        }
      }
    }
    return componentPageInfo;
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#getTopologyHistory")
  public TopologyHistoryInfo getTopologyHistory(String user)
      throws AuthorizationException, TException {
    TopologyHistoryInfo topologyHistoryInfo = null;
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    try {
      List<String> assignedTopologyIds = stormClusterState.assignments(null);
      List<String> activeIdsForUsers = new ArrayList<String>();
      // user-group-match-fn
      for (String topoId : assignedTopologyIds) {
        Map topologyConf =
            NimbusUtils.tryReadStormConf(conf, topoId, nimbus.getBlobStore());
        List<String> groups = ConfigUtils.getTopoLogsGroups(topologyConf);
        List<String> topoLogsUsers = ConfigUtils.getTopoLogsUsers(topologyConf);
        if (StringUtils.isEmpty(user)
            || NimbusUtils.doesUsersGroupIntersect(user, groups, conf)
            || adminUsers.contains(user) || (topoLogsUsers.contains(user))) {
          activeIdsForUsers.add(topoId);
        }
      }
      List<String> topoHistoryList =
          this.readTopologyHistory(nimbus, user, adminUsers);
      topoHistoryList.addAll(activeIdsForUsers);
      topologyHistoryInfo =
          new TopologyHistoryInfo(CoreUtil.distinctList(topoHistoryList));
    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new RuntimeException(e.getMessage());
    }
    return topologyHistoryInfo;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#read-topology-history")
  private List<String> readTopologyHistory(NimbusData nimbus, String user,
      List<String> adminUsers) throws Exception {
    LocalState topoHistoryState = nimbus.getTopoHistorystate();
    List<LSTopoHistory> currHistory =
        LocalStateUtils.getLSTopoList(topoHistoryState);
    List<String> topoHistoryList = new ArrayList<String>();
    for (LSTopoHistory tmpCurrHistory : currHistory) {
      if (StringUtils.isEmpty(user)) {
        topoHistoryList.add(tmpCurrHistory.get_topology_id());
      } else if (adminUsers.contains(user)
          || NimbusUtils.doesUsersGroupIntersect(user,
              tmpCurrHistory.get_groups(), this.conf)
          || tmpCurrHistory.get_users().contains(user)) {
        topoHistoryList.add(tmpCurrHistory.get_topology_id());
      }
    }
    return topoHistoryList;
  }

  @ClojureClass(className = "org.apache.storm.daemon.nimbus#service-handler#get-common-topo-info")
  private CommonTopoInfo getCommonTopoInfo(String stormId, String operation)
      throws TException {
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    CommonTopoInfo commonTopoInfo = new CommonTopoInfo();

    try {
      Map topologyConf =
          NimbusUtils.tryReadStormConf(conf, stormId, nimbus.getBlobStore());

      String stormName =
          Utils.getString(topologyConf.get(Config.TOPOLOGY_NAME), null);

      NimbusUtils.checkAuthorization(nimbus, stormName, topologyConf,
          operation);

      StormTopology topology =
          NimbusUtils.tryReadStormTopology(stormId, nimbus.getBlobStore());

      Map<Integer, String> taskToComponent =
          StormCommon.stormTaskInfo(topology, topologyConf);

      StormBase base = stormClusterState.stormBase(stormId, null);
      int launchTimeSecs = 1;
      if (base != null) {
        launchTimeSecs = base.get_launch_time_secs();
      } else {
        throw new NotAliveException(stormId);
      }

      Assignment assignment = stormClusterState.assignmentInfo(stormId, null);
      Map<ExecutorInfo, ExecutorCache> beats =
          nimbus.getExecutorHeartbeatsCache().get(stormId);

      Set<String> allComponents = Sets.newHashSet(taskToComponent.values());
      commonTopoInfo.setStormName(stormName);
      commonTopoInfo.setStormClusterState(stormClusterState);
      commonTopoInfo.setAllComponents(allComponents);
      commonTopoInfo.setLaunchTimeSecs(launchTimeSecs);
      commonTopoInfo.setAssignment(assignment);
      commonTopoInfo.setBeats(beats);
      commonTopoInfo.setTopology(topology);
      commonTopoInfo.setTaskToComponent(taskToComponent);
      commonTopoInfo.setBase(base);

    } catch (Exception e) {
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }

    return commonTopoInfo;
  }

  @Override
  @ClojureClass(className = "ui show worker jvm info, see detail in http://git.code.oa.com/trc/jstorm/issues/8")
  public List<WorkerJvmInfo> getWorkerJvmInfos(String topologyId)
      throws TException {
    List<WorkerJvmInfo> retList = new ArrayList<WorkerJvmInfo>();
    String[] topologyArr = null;
    if (!StringUtils.isEmpty(topologyId)) {
      topologyArr = topologyId.split(",");
    }
    IStormClusterState stormClusterState = nimbus.getStormClusterState();
    try {
      List<String> workerJvmNodes = stormClusterState.workerJvmNodes();
      for (String node : workerJvmNodes) {
        String[] nodeArr = node.split(":");
        String host = nodeArr[0];
        Long port = Long.valueOf(nodeArr[1]);
        WorkerJvmInfo workerJvmInfo =
            stormClusterState.getWorkerJvmInfo(host, port);
        if (topologyArr != null) {
          for (String tmpTopoId : topologyArr) {
            if (tmpTopoId.equals(workerJvmInfo.get_topologyId())) {
              retList.add(workerJvmInfo);
            }
          }
        } else {
          retList.add(workerJvmInfo);
        }
      }
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
    return retList;
  }

  @Override
  public TopologyBackpressure getTopologyBackpressure(String topologyName)
      throws AuthorizationException, TException {
    String topologyId =
        StormCommon.getStormId(nimbus.getStormClusterState(), topologyName);
    return nimbus.getStormClusterState().getTopologyBackpressure(topologyId);
  }

  @ClojureClass(className = "split getClusterInfo for http://git.code.oa.com/trc/jstorm/issues/73")
  @Override
  public List<TopologySummary> getTopologySummarys()
      throws AuthorizationException, TException {
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null, "getTopologySummarys");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      Map<String, StormBase> bases =
          StormCommon.topologyBases(stormClusterState);
      List<TopologySummary> topologySummaries =
          NimbusUtils.mkTopologySummaries(nimbus, stormClusterState, bases);

      return topologySummaries;
    } catch (Exception e) {
      LOG.error("Failed to get TopologySummarys ", CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @ClojureClass(className = "split getClusterInfo for http://git.code.oa.com/trc/jstorm/issues/73")
  @Override
  public List<NimbusSummary> getNimbusSummarys()
      throws AuthorizationException, TException {
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null, "getNimbusSummarys");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      List<NimbusSummary> nimbuses = stormClusterState.nimbuses();
      // update the isLeader field for each nimbus summary
      NimbusInfo leader = nimbus.getLeaderElector().getLeader();
      String leaderHost = leader.getHost();
      int leaderPort = leader.getPort();
      for (NimbusSummary nimbusSummary : nimbuses) {
        nimbusSummary
            .set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
        nimbusSummary.set_isLeader(nimbusSummary.get_host().equals(leaderHost)
            && nimbusSummary.get_port() == leaderPort);
      }
      return nimbuses;
    } catch (Exception e) {
      LOG.error("Failed to get NimbusSummarys ", CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }

  @ClojureClass(className = "split getClusterInfo for http://git.code.oa.com/trc/jstorm/issues/73")
  @Override
  public List<SupervisorSummary> getSupervisorSummarys()
      throws AuthorizationException, TException {
    try {
      NimbusUtils.checkAuthorization(nimbus, null, null,
          "getSupervisorSummarys");
      IStormClusterState stormClusterState = nimbus.getStormClusterState();
      Map<String, SupervisorInfo> supervisorInfos =
          NimbusUtils.allSupervisorInfo(stormClusterState);
      // TODO: need to get the port info about supervisors...
      // in standalone just look at metadata, otherwise just say N/A?
      List<SupervisorSummary> supervisorSummaries = NimbusUtils
          .mkSupervisorSummaries(nimbus, stormClusterState, supervisorInfos);
      return supervisorSummaries;
    } catch (Exception e) {
      LOG.error("Failed to get SupervisorSummarys ",
          CoreUtil.stringifyError(e));
      if (e instanceof TException) {
        throw (TException) e;
      }
      throw new TException(e);
    }
  }
}
