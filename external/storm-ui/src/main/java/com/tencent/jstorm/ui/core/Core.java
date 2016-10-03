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
package com.tencent.jstorm.ui.core;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.storm.Config;
import org.apache.storm.Thrift;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.ClientJarTransformerRunner;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyBackpressure;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerJvmInfo;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.thrift.TException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.stats.Pair;
import com.tencent.jstorm.stats.Stats;
import com.tencent.jstorm.stats.StatsFields;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.ui.helpers.ComponentNode;
import com.tencent.jstorm.ui.helpers.Helpers;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.MapKeyConstants;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 11:39:27 AM Feb 26, 2016
 */
@ClojureClass(className = "org.apache.storm.ui.core")
public class Core {

  private static Logger LOG = LoggerFactory.getLogger(Core.class);

  private static int maxErrortime = 1800;
  private static int maxErrornum = 200;
  public static final String SPOUT_STR = "spout";
  public static final String BOLT_STR = "bolt";
  public static final String SITE_JOB_DIR_PREFIX = "site-";
  public static final FastDateFormat FAST_DATE_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm");

  @ClojureClass(className = "org.apache.storm.ui.core#*STORM-CONF*")
  @SuppressWarnings("rawtypes")
  public static final Map STORM_CONF = Helpers.readStormConfig();

  private static final String SYM_REGEX = "(?![A-Za-z_\\-:\\.]).";

  private static final Pattern REFIND_P = Pattern.compile("^[A-Za-z]");

  public static final FastDateFormat CMT_DATE_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

  @ClojureClass(className = "org.apache.storm.ui.core#assert-authorized-user")
  public static void assertAuthorizedUser(String op)
      throws AuthorizationException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    assertAuthorizedUser(op, null);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "org.apache.storm.ui.core#assert-authorized-user")
  public static void assertAuthorizedUser(String op, Map topologyConf)
      throws AuthorizationException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    ReqContext context = ReqContext.context();
    if (context.isImpersonating()) {
      IAuthorizer UI_IMPERSONATION_HANDLER = StormCommon.mkAuthorizationHandler(
          (String) STORM_CONF.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER),
          STORM_CONF);
      if (UI_IMPERSONATION_HANDLER != null) {
        if (!UI_IMPERSONATION_HANDLER.permit(context, op, topologyConf)) {
          Principal principal = context.principal();
          String user = "unknown";
          if (principal != null) {
            user = principal.getName();
          }

          Principal realPrincipal = context.realPrincipal();
          String realUser = "unknown";
          if (realPrincipal != null) {
            realUser = realPrincipal.getName();
          }

          InetAddress remoteAddress = context.remoteAddress();

          throw new AuthorizationException("user '" + realUser
              + "' is not authorized to impersonate user '" + user
              + "' from host '" + remoteAddress
              + "'. Please see SECURITY.MD to learn how to configure impersonation ACL.");
        }
      }
    }

    IAuthorizer UI_ACL_HANDLER = StormCommon.mkAuthorizationHandler(
        (String) STORM_CONF.get(Config.NIMBUS_AUTHORIZER), STORM_CONF);
    if (UI_ACL_HANDLER != null) {
      if (!UI_ACL_HANDLER.permit(context, op, topologyConf)) {
        Principal principal = context.principal();
        String user = "unknown";
        if (principal != null) {
          user = principal.getName();
        }

        throw new AuthorizationException("UI request '" + op + "' for '" + user
            + "' user is not authorized");
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#assert-authorized-profiler-action")
  public static void assertAuthorizedProfilerAction(String op)
      throws AuthorizationException {
    if (!Utils.getBoolean(STORM_CONF.get(Config.WORKER_PROFILER_ENABLED),
        false)) {
      throw new AuthorizationException(
          "UI request for profiler action '" + op + "' is disabled.");
    }
  }

  /**
   * executorSummaryType
   * 
   * @param topology
   * @param s
   * @return executor type; ComponentType.SPOUT or ComponentType.BOLT
   */
  @ClojureClass(className = "org.apache.storm.ui.core#executor-summary-type")
  public static ComponentType executorSummaryType(StormTopology topology,
      ExecutorSummary s) {
    return componentType(topology, s.get_component_id());
  }

  @ClojureClass(className = "org.apache.storm.ui.core#is-ack-stream")
  public static boolean isAckStream(String stream) {
    if (stream.equals(Acker.ACKER_INIT_STREAM_ID)
        || stream.equals(Acker.ACKER_ACK_STREAM_ID)
        || stream.equals(Acker.ACKER_FAIL_STREAM_ID)) {
      return true;
    }
    return false;
  }

  /**
   * check ExecutorSummary if Spout type
   * 
   * @param topology
   * @param s
   * @return true if is spout, else false
   */
  @ClojureClass(className = "org.apache.storm.ui.core#spout-summary?")
  public static boolean isSpoutSummary(StormTopology topology,
      ExecutorSummary s) {
    return executorSummaryType(topology, s).equals(ComponentType.SPOUT);
  }

  /**
   * isBoltSummary
   * 
   * @param topology
   * @param s
   * @return true if is bolt, else false
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-summary?")
  public static boolean isBoltSummary(StormTopology topology,
      ExecutorSummary s) {
    return executorSummaryType(topology, s).equals(ComponentType.BOLT);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#http-creds-handler")
  private static IHttpCredentialsPlugin httpcredshandler =
      AuthUtils.GetUiHttpCredentialsPlugin(STORM_CONF);

  /**
   * Populate the Storm RequestContext from an servletRequest. This should be
   * called in each handler
   * 
   * @param servletRequest
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#populate-context!")
  public static ReqContext populateContext(HttpServletRequest servletRequest) {
    ReqContext reqContext = null;
    if (httpcredshandler != null) {
      reqContext = httpcredshandler.populateContext(ReqContext.context(),
          servletRequest);
    }

    return reqContext;
  }

  @ClojureClass(className = "org.apache.storm.ui.core#get-user-name")
  public static String getUserName(HttpServletRequest servletRequest) {

    String userName = null;
    if (httpcredshandler != null) {
      userName = httpcredshandler.getUserName(servletRequest);
    }
    return userName;

  }

  /**
   * Get nimbus client
   * 
   * @return NimbusClient
   */
  @ClojureClass(className = "org.apache.storm.ui.core#main-routes#with-nimbus")
  public static NimbusClient withNimbus() {
    return NimbusClient.getConfiguredClient(STORM_CONF);
  }

  /**
   * Get Storm Version
   * 
   * @return storm version string
   */
  @ClojureClass(className = "org.apache.storm.ui.core#read-storm-version")
  public static String readStormVersion() {
    return VersionInfo.getVersion() + "-r" + VersionInfo.getRevision();
  }

  /**
   * Get the component type
   * 
   * @param topology topology
   * @param id component id
   * @return Returns the component type (either :bolt or :spout) for a given
   *         topology and component id. Returns nil if not found.
   */
  @ClojureClass(className = "org.apache.storm.ui.core#component-type")
  public static ComponentType componentType(StormTopology topology, String id) {
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    ComponentType type = null;
    if (bolts.containsKey(id)) {
      type = ComponentType.BOLT;
    } else if (spouts.containsKey(id)) {
      type = ComponentType.SPOUT;
    }
    return type;
  }

  /**
   * addPairs
   * 
   * @param pairs1
   * @param pairs2
   * @return Pair
   */
  @ClojureClass(className = "org.apache.storm.ui.core#add-pairs")
  public static Pair<Object, Object> addPairs(Pair<Object, Object> pairs1,
      Pair<Object, Object> pairs2) {

    if (null == pairs1 && null == pairs2) {
      return new Pair<Object, Object>(0, 0);
    } else if (null == pairs1) {
      return pairs2;
    } else if (null == pairs2) {
      return pairs1;
    } else {
      return new Pair<Object, Object>(
          CoreUtil.add(pairs1.getFirst(), pairs2.getFirst()),
          CoreUtil.add(pairs1.getSecond(), pairs2.getSecond()));
    }
  }

  /**
   * expandAverages
   * 
   * @param avg
   * @param counts
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.ui.core#expand-averages")
  public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAverages(
      Map<K1, Map<K2, M1>> avg, Map<K1, Map<K2, M2>> counts) {
    Map<K1, Map<K2, Pair<Object, Object>>> ret =
        new HashMap<K1, Map<K2, Pair<Object, Object>>>();
    for (Map.Entry<K1, Map<K2, M2>> entry : counts.entrySet()) {
      K1 slice = entry.getKey();
      Map<K2, M2> streams = entry.getValue();
      Map<K2, Pair<Object, Object>> tmpValue =
          new HashMap<K2, Pair<Object, Object>>();
      for (Map.Entry<K2, M2> se : streams.entrySet()) {
        M2 count = se.getValue();
        K2 stream = se.getKey();
        tmpValue.put(se.getKey(), new Pair<Object, Object>(
            CoreUtil.multiply(count, avg.get(slice).get(stream)), count));
      }
      ret.put(slice, tmpValue);
    }

    return ret;
  }

  /**
   * expandAveragesSeq
   * 
   * @param averageSeq
   * @param countsSeq
   * @return Map
   */
  @ClojureClass(className = "org.apache.storm.ui.core#expand-averages-seq")
  public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAveragesSeq(
      List<Map<K1, Map<K2, M1>>> averageSeq,
      List<Map<K1, Map<K2, M2>>> countsSeq) {

    Iterator<Map<K1, Map<K2, M1>>> avgItr = averageSeq.iterator();
    Iterator<Map<K1, Map<K2, M2>>> couItr = countsSeq.iterator();

    Map<K1, Map<K2, Pair<Object, Object>>> ret =
        new HashMap<K1, Map<K2, Pair<Object, Object>>>();

    while (avgItr.hasNext() && couItr.hasNext()) {
      Map<K1, Map<K2, Pair<Object, Object>>> tmp =
          expandAverages(avgItr.next(), couItr.next());

      for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : tmp
          .entrySet()) {
        K1 key = entry.getKey();
        Map<K2, Pair<Object, Object>> value = entry.getValue();
        if (ret.containsKey(key)) {
          Map<K2, Pair<Object, Object>> original = ret.get(key);
          for (Map.Entry<K2, Pair<Object, Object>> v : value.entrySet()) {
            K2 vk = v.getKey();
            Pair<Object, Object> vv = v.getValue();
            Pair<Object, Object> tmpPair =
                original.containsKey(vk) ? addPairs(original.get(vk), vv) : vv;
            original.put(vk, tmpPair);
          }
        } else {
          ret.put(key, value);
        }
      }
    }
    return ret;
  }

  /**
   * valAvg
   * 
   * @param pair
   * @return valavg
   */
  @ClojureClass(className = "org.apache.storm.ui.core#val-avg")
  public static Object valAvg(Pair<Object, Object> pair) {
    if (pair == null) {
      return 0;
    }
    return CoreUtil.divide(pair.getFirst(), pair.getSecond());
  }

  /**
   * groupByComp
   * 
   * @param summs
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.ui.core#group-by-comp")
  public static Map<String, List<ExecutorSummary>> groupByComp(
      List<ExecutorSummary> summs) {
    Map<String, List<ExecutorSummary>> sortedMap =
        new TreeMap<String, List<ExecutorSummary>>();

    for (ExecutorSummary summ : summs) {
      String componentId = summ.get_component_id();
      if (sortedMap.containsKey(componentId)) {
        sortedMap.get(componentId).add(summ);
      } else {
        List<ExecutorSummary> sums = new ArrayList<ExecutorSummary>();
        sums.add(summ);
        sortedMap.put(componentId, sums);
      }
    }
    return sortedMap;
  }

  /**
   * Format http://%s/%s/logs/%s is for docker Gaia. e.g.<br>
   * <p>
   * <code>http://10.254.99.68/port_47799/logs/workers-artifacts%2Fa1-1-1465987613%2F47793%2Fworker.log</code>
   * </p>
   * 
   * @param host
   * @param rPort related port , e.g.worker port 47799, or nimbus thrift port
   *          36053
   * @param fname
   * @param secure
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#logviewer-link")
  public static String logviewerLink(String host, Integer rPort, String fname,
      boolean secure) {
    if (secure && STORM_CONF.get(Config.LOGVIEWER_HTTPS_PORT) != null) {
      Integer port =
          Utils.getInt(STORM_CONF.get(Config.LOGVIEWER_HTTPS_PORT), -1);
      String proxyPath = getProxyPath(host, rPort.longValue());
      if (proxyPath == null) {
        return Helpers.urlFormat("https://%s:%s/log?file=%s", host, port,
            fname);
      }

      return Helpers.urlFormat("https://%s/%s/log?file=%s", host, proxyPath,
          fname);
    } else {
      Integer port = Utils.getInt(STORM_CONF.get(Config.LOGVIEWER_PORT), 8081);
      String proxyPath = getProxyPath(host, rPort.longValue());
      if (proxyPath == null) {
        return Helpers.urlFormat("http://%s:%s/log?file=%s", host, port, fname);
      }
      return Helpers.urlFormat("http://%s/%s/log?file=%s", host, proxyPath,
          fname);
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#logviewer-link")
  public static String logviewerLink(String host, Integer rPort, String fname) {
    return logviewerLink(host, rPort, fname, false);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#event-log-link")
  public static String eventLogLink(String topologyId, int componentId,
      String host, Integer port, boolean secure) {
    if(host == null || host.length() == 0){
      return null;
    }
    return logviewerLink(host, port,
        CoreUtil.eventLogsFilename(topologyId, port), secure);
  }

  /**
   * errorSubset
   * 
   * @param errorStr
   * @return string
   */
  @ClojureClass(className = "org.apache.storm.ui.core#error-subset")
  public static String errorSubset(String errorStr) {
    return errorStr.substring(0, 200);
  }

  /**
   * mostRecentError
   * 
   * @param errors
   * @return the most recent ErrorInfo
   */
  @ClojureClass(className = "org.apache.storm.ui.core#most-recent-error")
  public static ErrorInfo mostRecentError(List<ErrorInfo> errors) {
    int max = 0;
    ErrorInfo result = new ErrorInfo();
    for (ErrorInfo error : errors) {
      int errorTime = error.get_error_time_secs();
      if (errorTime > max) {
        max = errorTime;
        result = error;
      }
    }
    return result;
  }

  /**
   * componentTaskSumms
   * 
   * @param summ
   * @param topology
   * @param id
   * @return list
   */
  @ClojureClass(className = "org.apache.storm.ui.core#component-task-summs")
  public static List<ExecutorSummary> componentTaskSumms(TopologyInfo summ,
      StormTopology topology, String id) {
    List<ExecutorSummary> executorSumms = summ.get_executors();
    List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
    List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();

    if (CollectionUtils.isNotEmpty(executorSumms)) {
      for (ExecutorSummary es : executorSumms) {
        if (isSpoutSummary(topology, es)) {
          spoutSumms.add(es);
        } else if (isBoltSummary(topology, es)) {
          boltSumms.add(es);
        }
      }
    }
    Map<String, List<ExecutorSummary>> spoutCompSumms = groupByComp(spoutSumms);
    Map<String, List<ExecutorSummary>> boltCompSumms = groupByComp(boltSumms);

    List<ExecutorSummary> ret = new ArrayList<ExecutorSummary>();
    if (spoutCompSumms.containsKey(id)) {
      ret = spoutCompSumms.get(id);
    } else {
      ret = boltCompSumms.get(id);
    }

    if (null != ret) {
      Collections.sort(ret, new Comparator<ExecutorSummary>() {

        @Override
        public int compare(ExecutorSummary o1, ExecutorSummary o2) {
          return o1.get_executor_info().get_task_start()
              - o2.get_executor_info().get_task_start();
        }

      });
    }
    return ret;
  }

  /**
   * workerLogLink
   * 
   * @param host
   * @param workerPort
   * @return worker log link string eg:
   *         http://localhost:8081/log?file=stormId/6701/worker.log
   */
  @ClojureClass(className = "org.apache.storm.ui.core#worker-log-link")
  public static String workerLogLink(String host, int port, String topologyId,
      boolean secure) {
    return logviewerLink(host, port, CoreUtil.logsFilename(topologyId, port),
        secure);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#worker-log-link")
  public static String workerLogLink(String host, int port, String topologyId) {
    return logviewerLink(host, port, CoreUtil.logsFilename(topologyId, port),
        false);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#nimbus-log-link")
  public static String nimbusLogLink(String host, int port) {
    Integer portI = Utils.getInt(STORM_CONF.get(Config.LOGVIEWER_PORT), 8081);
    String proxyPath = getProxyPath(host, Long.valueOf(port));

    if (proxyPath == null) {
      return Helpers.urlFormat("http://%s:%s/daemonlog?file=nimbus.log", host,
          portI);
    }

    return Helpers.urlFormat("http://%s/%s/daemonlog?file=nimbus.log", host,
        proxyPath);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#supervisor-log-link")
  public static String supervisorLogLink(String host, String supervisorId) {
    Integer portI = Utils.getInt(STORM_CONF.get(Config.LOGVIEWER_PORT), 8081);

    Long firstSlot = UIServer.getSupervisorIdToFirstSlot().get(supervisorId);
    if (firstSlot == null) {
      LOG.error("supervisorLogLink error : supervisor id {} not in {}",
          supervisorId, UIServer.getSupervisorIdToFirstSlot());
      firstSlot = -1l;
    }
    String proxyPath = getProxyPath(host, firstSlot);
    if (proxyPath == null) {
      return Helpers.urlFormat("http://%s:%s/daemonlog?file=supervisor.log",
          host, portI);
    }

    return Helpers.urlFormat("http://%s/%s/daemonlog?file=supervisor.log", host,
        proxyPath);
  }

  @ClojureClass(className = "org.apache.storm.ui.core#get-error-json")
  private static void getErrorJson(String topoId, ErrorInfo errorInfo,
      boolean isSecure, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    String host = getErrorHost(errorInfo);
    int port = getErrorPort(errorInfo);

    dumpGenerator.writeStartObject();
    dumpGenerator.writeStringField("lastError", getErrorData(errorInfo));
    dumpGenerator.writeNumberField("errorTime", getErrorTime(errorInfo));
    dumpGenerator.writeStringField("errorHost", host);
    dumpGenerator.writeNumberField("errorPort", port);
    dumpGenerator.writeNumberField("errorLapsedSecs", getErrorTime(errorInfo));
    dumpGenerator.writeStringField("errorWorkerLogLink",
        workerLogLink(host, port, topoId, isSecure));
    dumpGenerator.writeEndObject();
  }

  /**
   * getErrorTimeDelta
   *
   * @param error
   * @return error time delta
   */
  @ClojureClass(className = "org.apache.storm.ui.core#get-error-time")
  public static int getErrorTimeDelta(ErrorInfo error) {
    if (error != null) {
      return Time.deltaSecs(error.get_error_time_secs());
    }
    return 0;
  }

  /**
   * getErrorTime
   * 
   * @param error
   * @return error time
   */
  @ClojureClass(className = "org.apache.storm.ui.core#get-error-time")
  public static int getErrorTime(ErrorInfo error) {
    if (error != null) {
      return error.get_error_time_secs();
    }
    return 0;
  }

  /**
   * getErrorData
   * 
   * @param error
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#get-error-data")
  public static String getErrorData(ErrorInfo error) {
    if (error != null) {
      return errorSubset(error.get_error());
    }
    return null;
  }

  /**
   * getErrorPort
   * 
   * @param error
   * @param errorHost
   * @param topId
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#get-error-port")
  public static int getErrorPort(ErrorInfo error) {
    if (error != null) {
      return error.get_port();
    }
    return 0;
  }

  /**
   * getErrorHost
   * 
   * @param error
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#get-error-host")
  public static String getErrorHost(ErrorInfo error) {
    if (error != null) {
      return error.get_host();
    }
    return "";
  }

  @ClojureClass(className = "org.apache.storm.ui.core#worker-dump-link")
  public static String workerDumpLink(String host, int port,
      String topologyId) {
    return Helpers.urlFormat("http://%s:%s/dumps?topo-id=%s&host-port=%s", host,
        port, topologyId, host + ":" + port);
  }

  /**
   * 
   * @param statsMap
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#stats-times")
  public static Set<Integer> statsTimes(Map<Object, Object> statsMap) {
    Set<Integer> windows = new TreeSet<Integer>();
    for (Object window : statsMap.keySet()) {
      windows.add(Utils.getInt(window));
    }
    return windows;
  }

  /**
   * 
   * @param window
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#window-hint")
  public static String windowHint(String window) {
    int wind = Integer.valueOf(window).intValue();
    return windowHint(wind);
  }

  public static String windowHint(int window) {
    if (window == 0) {
      return "All time";
    }
    return Helpers.prettyUptimeSec(window);
  }

  /**
   * sanitizeStreamName
   * 
   * @param name
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#sanitize-stream-name")
  public static String sanitizeStreamName(String name) {
    if (REFIND_P.matcher(name).matches()) {
      name = name.replaceAll(SYM_REGEX, "_");
    } else {
      name = ("s" + name).replaceAll(SYM_REGEX, "_");
    }
    return String.valueOf(name.hashCode());
  }

  @ClojureClass(className = "org.apache.storm.ui.core#sanitize-transferred")
  public static Map<String, Map<String, Long>> sanitizeTransferred(
      Map<String, Map<String, Long>> transferred) {
    Map<String, Map<String, Long>> sanitizeTransferred =
        new HashMap<String, Map<String, Long>>();
    for (Map.Entry<String, Map<String, Long>> entry : transferred.entrySet()) {
      String time = entry.getKey();
      Map<String, Long> streamMap = entry.getValue();
      Map<String, Long> sanitizeStreamMap = new HashMap<String, Long>();
      for (Map.Entry<String, Long> stream : streamMap.entrySet()) {
        sanitizeStreamMap.put(sanitizeStreamName(stream.getKey()),
            stream.getValue());
      }
      sanitizeTransferred.put(time, sanitizeStreamMap);
    }
    return sanitizeTransferred;
  }

  /**
   * sanitizeTransferred
   * 
   * @param transferred
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#sanitize-transferred")
  @Deprecated
  public static void sanitizeTransferred(
      Map<String, Map<String, Long>> transferred, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    dumpGenerator.writeFieldName(":transferred");
    dumpGenerator.writeStartArray();
    for (Map.Entry<String, Map<String, Long>> entry : transferred.entrySet()) {
      String time = entry.getKey();
      Map<String, Long> streamMap = entry.getValue();
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName(time);
      dumpGenerator.writeStartArray();
      for (Map.Entry<String, Long> stream : streamMap.entrySet()) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeNumberField(sanitizeStreamName(stream.getKey()),
            stream.getValue());
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * visualizationData
   *
   * @param spouts
   * @param bolts
   * @param spoutCompSumms
   * @param boltCompSumms
   * @param window
   * @param stormId
   */
  @ClojureClass(className = "org.apache.storm.ui.core#visualization-data")
  public static Map<String, Map<String, Object>> visualizationData(
      Map<String, Object> spoutbolt,
      Map<String, List<ExecutorSummary>> spoutCompSumms,
      Map<String, List<ExecutorSummary>> boltCompSumms, String window,
      String stormId) {
    Map<String, Map<String, Object>> components =
        new HashMap<String, Map<String, Object>>();
    for (Entry<String, Object> entry : spoutbolt.entrySet()) {
      String id = entry.getKey();
      Object spec = entry.getValue();

      List<ExecutorSummary> boltSumms = boltCompSumms.get(id);
      List<ExecutorSummary> spoutSumms = spoutCompSumms.get(id);

      Map<GlobalStreamId, Grouping> inputs = null;
      if (null != boltSumms) {
        inputs = ((Bolt) spec).get_common().get_inputs();
      } else {
        inputs = ((SpoutSpec) spec).get_common().get_inputs();
      }
      double boltCap =
          null != boltSumms ? Stats.computeBoltCapacity(boltSumms) : 0d;
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("type", null != boltSumms ? "bolt" : "spout");
      map.put("capacity", String.valueOf(boltCap));
      String latency = null != boltSumms
          ? String.valueOf(Stats.boltStreamsStats(boltSumms, true)
              .get(StatsFields.process_latencies).get(window))
          : String.valueOf(Stats.spoutStreamsStats(spoutSumms, true)
              .get(StatsFields.complete_latencies).get(window));
      map.put("latency", latency);
      Object transferred = null;
      if (null != spoutSumms) {
        transferred = Stats.spoutStreamsStats(spoutSumms, true)
            .get(StatsFields.transferred).get(window);
      }
      if (null == transferred) {
        transferred = Stats.boltStreamsStats(boltSumms, true)
            .get(StatsFields.transferred).get(window);
      }
      map.put("transferred", Utils.getString(transferred, "0"));
      if (null != boltSumms) {
        map.put("stats", mapFn(boltSumms));
      } else {
        map.put("stats", mapFn(spoutSumms));
      }

      map.put("link", Helpers.urlFormat("/component.html?id=%s&topology_id=%s",
          id, stormId));

      // inputs
      Set<Map<String, String>> inputMaps = new HashSet<Map<String, String>>();
      for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
        GlobalStreamId gId = input.getKey();
        String streamId = gId.get_streamId();
        Map<String, String> inputMap = new HashMap<String, String>();
        inputMap.put("component", gId.get_componentId());
        inputMap.put("stream", streamId);
        inputMap.put("sani-stream", sanitizeStreamName(streamId));
        inputMap.put("grouping",
            Thrift.groupingType(input.getValue()).getFieldName());
        inputMaps.add(inputMap);
      }
      map.put("inputs", inputMaps);
      components.put(id, map);
    }
    return components;
  }

  /**
   * visualizationData
   * 
   * @param spouts
   * @param bolts
   * @param spoutCompSumms
   * @param boltCompSumms
   * @param window
   * @param stormId
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#visualization-data")
  @Deprecated
  public static void visualizationData(Map<String, SpoutSpec> spouts,
      Map<String, Bolt> bolts,
      Map<String, List<ExecutorSummary>> spoutCompSumms,
      Map<String, List<ExecutorSummary>> boltCompSumms, String window,
      String stormId, boolean streamBoxes, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {

    dumpGenerator.writeStartArray();
    Set<Map<String, String>> boxes = new HashSet<Map<String, String>>();
    Map<String, Map<GlobalStreamId, Grouping>> mergeMap =
        new HashMap<String, Map<GlobalStreamId, Grouping>>();
    for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
      mergeMap.put(entry.getKey(), entry.getValue().get_common().get_inputs());
    }
    for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
      mergeMap.put(entry.getKey(), entry.getValue().get_common().get_inputs());
    }
    for (Map.Entry<String, Map<GlobalStreamId, Grouping>> entry : mergeMap
        .entrySet()) {
      String id = entry.getKey();
      if (Utils.isSystemId(id)) {
        continue;
      }
      Map<GlobalStreamId, Grouping> inputs = entry.getValue();
      if (!streamBoxes) {
        List<ExecutorSummary> boltSumms = boltCompSumms.get(id);
        List<ExecutorSummary> spoutSumms = spoutCompSumms.get(id);
        // when worker is killed
        if (boltSumms == null && spoutSumms == null) {
          continue;
        }
        double boltCap =
            null != boltSumms ? Stats.computeBoltCapacity(boltSumms) : 0d;
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField(":component", id);
        dumpGenerator.writeStringField(":type",
            null != boltSumms ? "bolt" : "spout");
        dumpGenerator.writeStringField(":capacity", String.valueOf(boltCap));
        if (null != boltSumms) {
          dumpGenerator.writeStringField(":latency",
              String.valueOf(Stats.boltStreamsStats(boltSumms, true)
                  .get(StatsFields.process_latencies).get(window)));
          dumpGenerator.writeNumberField(":transferred",
              CoreUtil.getLong(Stats.boltStreamsStats(boltSumms, true)
                  .get(StatsFields.transferred).get(window), 0L));
        } else {
          dumpGenerator.writeStringField(":latency",
              String.valueOf(Stats.spoutStreamsStats(spoutSumms, true)
                  .get(StatsFields.complete_latencies).get(window)));
          dumpGenerator.writeNumberField(":transferred",
              CoreUtil.getLong(Stats.spoutStreamsStats(spoutSumms, true)
                  .get(StatsFields.transferred).get(window), 0L));
        }

        dumpGenerator.writeFieldName(":stats");
        if (null != boltSumms) {
          mapFn(boltSumms, dumpGenerator);
        } else {
          mapFn(spoutSumms, dumpGenerator);
        }
        // inputs
        dumpGenerator.writeStringField(":link", Helpers
            .urlFormat("/component.html?component=%s&id=%s", id, stormId));
        dumpGenerator.writeFieldName(":inputs");
        dumpGenerator.writeStartArray();
        for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
          GlobalStreamId gId = input.getKey();
          if (Utils.isSystemId(gId.get_componentId())) {
            continue;
          }
          String streamId = gId.get_streamId();
          dumpGenerator.writeStartObject();
          dumpGenerator.writeStringField(":component", gId.get_componentId());
          dumpGenerator.writeStringField(":stream", streamId);
          dumpGenerator.writeStringField(":sani-stream",
              sanitizeStreamName(streamId));
          dumpGenerator.writeStringField(":grouping",
              Thrift.groupingType(input.getValue()).getFieldName());
          dumpGenerator.writeEndObject();
        }
        dumpGenerator.writeEndArray();
        dumpGenerator.writeEndObject();
      } else {
        for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
          Map<String, String> tmp = new HashMap<String, String>();
          GlobalStreamId gId = input.getKey();
          if (Utils.isSystemId(gId.get_componentId())) {
            continue;
          }
          String streamId = gId.get_streamId();
          tmp.put(":stream", streamId);
          tmp.put(":sani-stream", sanitizeStreamName(streamId));
          tmp.put(":checked", String.valueOf(isAckStream(streamId)));
          boxes.add(tmp);
        }
      }
    }
    if (streamBoxes) {
      writeJsonForStreamBox(boxes, dumpGenerator);
    }
    dumpGenerator.writeEndArray();
  }

  private static void writeJsonForStreamBox(Set<Map<String, String>> streams,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName(":row");
    dumpGenerator.writeStartArray();
    for (Map<String, String> map : streams) {
      dumpGenerator.writeStartObject();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
      }
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
  }

  @ClojureClass(className = "org.apache.storm.ui.core#visualization-data#mapfn")
  private static Map<String, Object> mapFn(List<ExecutorSummary> dat) {
    Map<String, Object> map = new HashMap<String, Object>();
    for (ExecutorSummary e : dat) {
      map.put("host", e.get_host());
      map.put("port", e.get_port());
      map.put("uptimes_secs", e.get_uptime_secs());
      ExecutorStats stats = e.get_stats();
      if (null != stats) {
        map.put("transferred", sanitizeTransferred(stats.get_transferred()));
      }
    }
    return map;
  }

  /**
   * 
   * @param dat
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @Deprecated
  @ClojureClass(className = "org.apache.storm.ui.core#visualization-data#mapfn")
  private static void mapFn(List<ExecutorSummary> dat,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {
    dumpGenerator.writeStartArray();
    for (ExecutorSummary e : dat) {
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField(":host", e.get_host());
      dumpGenerator.writeNumberField(":port", e.get_port());
      dumpGenerator.writeNumberField(":uptimes_secs", e.get_uptime_secs());
      ExecutorStats stats = e.get_stats();
      if (null != stats) {
        sanitizeTransferred(stats.get_transferred(), dumpGenerator);
      }
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  @ClojureClass(className = "org.apache.storm.ui.core#get-topology-info")
  private static TopologyInfo getTopologyInfo(String id)
      throws AuthorizationException, TException {
    NimbusClient client = withNimbus();
    try {
      return client.getClient().getTopologyInfo(id);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#get-topology-info")
  private static TopologyInfo getTopologyInfo(String id, GetInfoOptions opts)
      throws AuthorizationException, TException {
    NimbusClient client = withNimbus();
    try {
      return client.getClient().getTopologyInfoWithOpts(id, opts);
    } finally {
      client.close();
    }
  }

  /**
   * mk-visualization-data
   * 
   * @throws TException
   * 
   */
  @ClojureClass(className = "org.apache.storm.ui.core#mk-visualization-data")
  public static void mkVisualizationData(String id, String window,
      boolean includeSys, OutputStreamWriter out) throws TException {

    NimbusClient client = withNimbus();
    try {
      StormTopology topology = client.getClient().getTopology(id);
      Map<String, SpoutSpec> spouts = topology.get_spouts();
      Map<String, Bolt> bolts = topology.get_bolts();
      TopologyInfo summ = client.getClient().getTopologyInfo(id);
      List<ExecutorSummary> execs = summ.get_executors();
      List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
      List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();
      for (ExecutorSummary exec : execs) {
        if (isSpoutSummary(topology, exec)) {
          spoutSumms.add(exec);
        } else {
          boltSumms.add(exec);
        }
      }
      Map<String, List<ExecutorSummary>> spoutCompSumms =
          groupByComp(spoutSumms);
      Map<String, List<ExecutorSummary>> boltCompSumms = groupByComp(boltSumms);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      // dumpGenerator.writeStartObject();
      // dumpGenerator.writeFieldName("visualizationTable");
      visualizationData(spouts, bolts, spoutCompSumms, boltCompSumms, window,
          id, false, dumpGenerator);
      // dumpGenerator.writeEndObject();
      dumpGenerator.flush();

    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }

  }

  /**
   * mk-visualization-data
   * 
   * @throws TException
   * 
   */
  @ClojureClass(className = "org.apache.storm.ui.core#build-visualization")
  public static void buildVisualizationData(String id, String window,
      boolean includeSys, OutputStreamWriter out) throws TException {
    NimbusClient client = withNimbus();
    try {
      StormTopology topology = client.getClient().getTopology(id);
      Map<String, SpoutSpec> spouts = topology.get_spouts();
      Map<String, Bolt> bolts = topology.get_bolts();
      GetInfoOptions options = new GetInfoOptions();
      options.set_num_err_choice(NumErrorsChoice.ONE);
      TopologyInfo summ =
          client.getClient().getTopologyInfoWithOpts(id, options);
      List<ExecutorSummary> execs = summ.get_executors();
      List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
      List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();
      for (ExecutorSummary exec : execs) {
        if (isSpoutSummary(topology, exec)) {
          spoutSumms.add(exec);
        } else {
          boltSumms.add(exec);
        }
      }
      Map<String, List<ExecutorSummary>> spoutCompSumms =
          groupByComp(spoutSumms);
      Map<String, List<ExecutorSummary>> boltCompSumms = groupByComp(boltSumms);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("visualizationTable");
      visualizationData(spouts, bolts, spoutCompSumms, boltCompSumms, window,
          id, true, dumpGenerator);
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();

    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#run-tplg-submit-cmd")
  public static void runTplgSubmitCmd(String tplgJarFile, Map tplgConfig,
      String user) throws TException {
    String tplgMainClass = null;
    String topologyMainClassArgs = null;
    if (tplgConfig != null) {
      tplgMainClass =
          Utils.getString(tplgConfig.get("topologyMainClass"), "").trim();
      topologyMainClassArgs =
          Utils.getString(tplgConfig.get("topologyMainClassArgs"), "");
    }

    String stormHome = System.getProperty("storm.home");
    String stormConfDir = stormHome + Utils.FILE_PATH_SEPARATOR + "conf";
    String stormLogDir = Utils.getString(STORM_CONF.get("storm.log.dir"),
        stormHome + Utils.FILE_PATH_SEPARATOR + "logs");
    String stormLibs = stormHome + Utils.FILE_PATH_SEPARATOR + "lib"
        + Utils.FILE_PATH_SEPARATOR + "*";
    String javaCmd = System.getProperty("java.home") + Utils.FILE_PATH_SEPARATOR
        + "bin" + Utils.FILE_PATH_SEPARATOR + "java";
    String stormCmd = stormHome + Utils.FILE_PATH_SEPARATOR + "bin"
        + Utils.FILE_PATH_SEPARATOR + "storm";
    // TODO
    int tplgCmdResponse = 0; // to-do-something-here
    LOG.info("tplg-cmd-response " + tplgCmdResponse);
    if (tplgCmdResponse == 0) {
      // TODO
    }
    // TODO
  }

  /**
   * Generate Json Cluster Configuration
   * 
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#cluster-configuration")
  public static void clusterConfiguration(OutputStreamWriter out)
      throws TException {
    clusterConfiguration(out, null);
  }

  /**
   * Generate Json Cluster Configuration
   * 
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#cluster-configuration")
  @SuppressWarnings("unchecked")
  public static void clusterConfiguration(OutputStreamWriter out,
      List<String> keys) throws TException {
    NimbusClient client = withNimbus();
    try {
      String jsonConf = client.getClient().getNimbusConf();
      Map<String, Object> config =
          (Map<String, Object>) CoreUtil.from_json(jsonConf);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.flush();
      dumpGenerator.writeStartObject();
      if (keys != null && keys.size() > 0) {
        for (String key : keys) {
          dumpGenerator.writeStringField(key, String.valueOf(config.get(key)));
        }
      } else {
        for (Map.Entry<String, Object> item : config.entrySet()) {
          dumpGenerator.writeStringField(String.valueOf(item.getKey()),
              String.valueOf(config.get((String) item.getKey())));
        }
      }
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  /**
   * Generate Json Cluster Summary
   * 
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#cluster-summary")
  public static void clusterSummary(OutputStreamWriter out) throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("get NimbusClient", operStart);
    try {
      ClusterSummary summ = client.getClient().getClusterInfo();
      logOperTime("getClusterInfo from nimbus", operStart);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.flush();
      synchronized (summ) {
        dumpGenerator.writeStartObject();
        List<SupervisorSummary> sups = summ.get_supervisors();
        List<TopologySummary> tops = summ.get_topologies();
        Integer usedSlots = 0, totalSlots = 0, freeSlots = 0, totalTasks = 0,
            totalExecutors = 0;
        for (SupervisorSummary sup : sups) {
          usedSlots += sup.get_num_used_workers();
          totalSlots += sup.get_num_workers();
        }
        freeSlots = totalSlots - usedSlots;
        for (TopologySummary top : tops) {
          totalTasks += top.get_num_tasks();
          totalExecutors += top.get_num_executors();
        }
        // Storm version
        dumpGenerator.writeStringField("stormVersion", Core.readStormVersion());
        // Shows how long the cluster is running
        dumpGenerator.writeStringField("nimbusUptime",
            Helpers.prettyUptimeSec(summ.get_nimbus_uptime_secs()));
        dumpGenerator.writeStringField("submitTime",
            getAddTimeDate(Calendar.SECOND, -summ.get_nimbus_uptime_secs()));
        // Number of supervisors running
        dumpGenerator.writeNumberField("supervisors", sups.size());
        // Total number of available worker slots
        dumpGenerator.writeNumberField("slotsTotal", totalSlots);
        // Number of worker slots used
        dumpGenerator.writeNumberField("slotsUsed", usedSlots);
        // Number of worker slots available
        dumpGenerator.writeNumberField("slotsFree", freeSlots);
        // Total number of executors
        dumpGenerator.writeNumberField("executorsTotal", totalExecutors);
        // Total tasks
        dumpGenerator.writeNumberField("tasksTotal", totalTasks);

      }
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      logOperTime("Core.clusterSummary", start);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  /**
   * Generate Supervisor Summary
   * 
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#supervisor-summary")
  public static void supervisorSummary(OutputStreamWriter out)
      throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("get NimbusClient", operStart);
    try {
      List<SupervisorSummary> summs =
          client.getClient().getSupervisorSummarys();
      operStart = logOperTime("getSupervisorSummarys form nimbus", operStart);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("supervisors");
      dumpGenerator.writeStartArray();
      dumpGenerator.flush();
      synchronized (summs) {
        for (SupervisorSummary s : summs) {
          dumpGenerator.writeStartObject();
          // Supervisor's id
          dumpGenerator.writeStringField("id", s.get_supervisor_id());
          // Supervisor's host name
          dumpGenerator.writeStringField("host", s.get_host());
          // Shows how long the supervisor is running
          dumpGenerator.writeStringField("uptime",
              Helpers.prettyUptimeSec(s.get_uptime_secs()));
          dumpGenerator.writeNumberField("uptimeSeconds", s.get_uptime_secs());
          dumpGenerator.writeStringField("submitTime",
              getAddTimeDate(Calendar.SECOND, -s.get_uptime_secs()));
          // Total number of available worker slots for this
          // supervisor
          dumpGenerator.writeNumberField("slotsTotal", s.get_num_workers());
          // Number of worker slots used on this supervisor
          dumpGenerator.writeNumberField("slotsUsed", s.get_num_used_workers());

          dumpGenerator.writeNumberField("totalMem", s.get_total_resources()
              .get(Config.SUPERVISOR_MEMORY_CAPACITY_MB));
          dumpGenerator.writeNumberField("totalCpu",
              s.get_total_resources().get(Config.SUPERVISOR_CPU_CAPACITY));
          dumpGenerator.writeNumberField("usedMem", s.get_used_mem());
          dumpGenerator.writeNumberField("usedCpu", s.get_used_cpu());
          dumpGenerator.writeStringField("logLink",
              supervisorLogLink(s.get_host(), s.get_supervisor_id()));
          dumpGenerator.writeStringField("version", s.get_version());

          dumpGenerator.writeEndObject();
        }
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeBooleanField("schedulerDisplayResource",
          Utils.getBoolean(STORM_CONF.get(Config.SCHEDULER_DISPLAY_RESOURCE),
              false));
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      logOperTime("Core.supervisorSummary", operStart);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  public static void supervisorConf(String supervisorId, Writer out)
      throws TException {
    // NimbusClient client = withNimbus();
    //
    // try {
    // String supervisorConf =
    // client.getClient().getSupervisorConf(supervisorId);
    // Map<String, Object> config =
    // (Map<String, Object>) CoreUtil.from_json(supervisorConf);
    // JsonFactory dumpFactory = new JsonFactory();
    // JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    // dumpGenerator.writeStartObject();
    // dumpGenerator.writeFieldName("configuration");
    // synchronized (config) {
    // dumpGenerator.writeStartObject();
    // for (Map.Entry<String, Object> item : config.entrySet()) {
    // dumpGenerator.writeStringField(String.valueOf(item.getKey()),
    // String.valueOf(config.get((String) item.getKey())));
    // }
    // dumpGenerator.writeEndObject();
    // }
    // dumpGenerator.writeEndObject();
    // dumpGenerator.flush();
    // } catch (Exception e) {
    // throw new TException(CoreUtil.stringifyError(e));
    // } finally {
    // if (client != null) {
    // client.close();
    // }
    // }
  }

  public static void supervisorWorkers(String host, OutputStreamWriter out)
      throws TException {
    //
    // NimbusClient client = withNimbus();
    // try {
    // SupervisorWorkers workers =
    // client.getClient().getSupervisorWorkers(host);
    // SupervisorSummary s = null;
    // List<SupervisorSummary> summs =
    // client.getClient().getClusterInfo().get_supervisors();
    // for (SupervisorSummary summ : summs) {
    // if (summ.get_host().equals(host)) {
    // s = summ;
    // }
    // }
    // JsonFactory dumpFactory = new JsonFactory();
    // JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    // dumpGenerator.writeStartObject();
    // if (s != null) {
    // dumpGenerator.writeStringField("id", s.get_supervisor_id());
    // dumpGenerator.writeStringField("host", s.get_host());
    // dumpGenerator.writeNumberField("uptime", s.get_uptime_secs());
    // dumpGenerator.writeNumberField("slotsTotal", s.get_num_workers());
    // dumpGenerator.writeNumberField("slotsUsed",
    // s.get_num_used_workers());
    // }
    // dumpGenerator.writeFieldName("workers");
    // dumpGenerator.writeStartArray();
    // dumpGenerator.flush();
    // synchronized (workers) {
    // for (WorkerSummary e : workers.get_workers()) {
    //
    // StringBuilder taskSB = new StringBuilder();
    // StringBuilder componentSB = new StringBuilder();
    // boolean isFirst = true;
    // int minUptime = 0;
    // for (ExecutorSummary executorSummary : e.get_tasks()) {
    // if (isFirst == false) {
    // taskSB.append(',');
    // componentSB.append(',');
    // } else {
    // minUptime = executorSummary.get_uptime_secs();
    // }
    // taskSB.append(executorSummary.get_executor_info().get_task_start());
    // componentSB.append(executorSummary.get_component_id());
    //
    // if (minUptime < executorSummary.get_uptime_secs()) {
    // minUptime = executorSummary.get_uptime_secs();
    // }
    //
    // isFirst = false;
    // }
    //
    // dumpGenerator.writeStartObject();
    // dumpGenerator.writeNumberField("port", e.get_port());
    // dumpGenerator.writeStringField("uptime",
    // Helpers.prettyUptimeSec(minUptime));
    // dumpGenerator.writeStringField("topology", e.get_topology());
    // dumpGenerator.writeStringField("taskList", taskSB.toString());
    // dumpGenerator.writeStringField("componentList",
    // componentSB.toString());
    // dumpGenerator.writeEndObject();
    // }
    // }
    // dumpGenerator.writeEndArray();
    // dumpGenerator.writeEndObject();
    // dumpGenerator.flush();
    // } catch (Exception e) {
    // throw new TException(CoreUtil.stringifyError(e));
    // } finally {
    // if (client != null) {
    // client.close();
    // }
    // }
  }

  /**
   * allTopologiesSummary
   * 
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#all-topologies-summary")
  public static void allTopologiesSummary(OutputStreamWriter out)
      throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("get NimbusClient", operStart);
    try {
      List<TopologySummary> topologies =
          client.getClient().getTopologySummarys();
      operStart = logOperTime("getTopologySummarys form nimbus ", operStart);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("topologies");
      dumpGenerator.writeStartArray();
      dumpGenerator.flush();
      synchronized (topologies) {
        for (TopologySummary t : topologies) {
          dumpGenerator.writeStartObject();
          // Topology Id
          dumpGenerator.writeStringField("id", t.get_id());
          // Encoded Topology Id
          dumpGenerator.writeStringField("encodedId",
              CoreUtil.urlEncode(t.get_id()));
          dumpGenerator.writeStringField("owner", t.get_owner());
          // Topology Name
          dumpGenerator.writeStringField("name", t.get_name());
          // Topology Status
          dumpGenerator.writeStringField("status", t.get_status());
          // Shows how long the topology is running
          dumpGenerator.writeStringField("uptime",
              Helpers.prettyUptimeSec(t.get_uptime_secs()));
          dumpGenerator.writeStringField("submitTime",
              getAddTimeDate(Calendar.SECOND, -t.get_uptime_secs()));
          dumpGenerator.writeNumberField("uptimeSeconds", t.get_uptime_secs());
          // Total number of tasks for this topology
          dumpGenerator.writeNumberField("tasksTotal", t.get_num_tasks());
          // Number of workers used for this topology
          dumpGenerator.writeNumberField("workersTotal", t.get_num_workers());
          // Number of executors used for this topology
          dumpGenerator.writeNumberField("executorsTotal",
              t.get_num_executors());

          dumpGenerator.writeNumberField("replicationCount",
              t.get_replication_count());
          dumpGenerator.writeStringField("schedulerInfo", t.get_sched_status());
          dumpGenerator.writeNumberField("requestedMemOnHeap",
              t.get_requested_memonheap());
          dumpGenerator.writeNumberField("requestedMemOffHeap",
              t.get_requested_memoffheap());
          dumpGenerator.writeNumberField("requestedTotalMem",
              t.get_requested_memonheap() + t.get_requested_memoffheap());
          dumpGenerator.writeNumberField("requestedCpu", t.get_requested_cpu());
          dumpGenerator.writeNumberField("assignedMemOnHeap",
              t.get_assigned_memonheap());
          dumpGenerator.writeNumberField("assignedMemOffHeap",
              t.get_assigned_memoffheap());
          dumpGenerator.writeNumberField("assignedTotalMem",
              t.get_assigned_memonheap() + t.get_assigned_memoffheap());
          dumpGenerator.writeNumberField("assignedCpu", t.get_assigned_cpu());
          dumpGenerator.writeEndObject();
        }
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeBooleanField("schedulerDisplayResource",
          Utils.getBoolean(STORM_CONF.get(Config.SCHEDULER_DISPLAY_RESOURCE),
              false));
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      logOperTime("Core.allTopologiesSummary", start);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#nimbus-summary")
  public static void nimbusSummary(OutputStreamWriter out) throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("get NimbusClient", operStart);
    try {
      List<NimbusSummary> nimbuses = client.getClient().getNimbusSummarys();
      operStart = logOperTime("getNimbusSummarys form nimbus", operStart);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("nimbuses");
      dumpGenerator.writeStartArray();
      nimbusSummary(out, nimbuses, dumpGenerator);
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      logOperTime("Core.nimbusSummary", start);
    } catch (Exception e) {
      throw new TException(CoreUtil.stringifyError(e));
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#nimbus-summary")
  public static void nimbusSummary(OutputStreamWriter out,
      List<NimbusSummary> nimbuses, JsonGenerator dumpGenerator)
          throws Exception {

    for (NimbusSummary n : nimbuses) {
      int uptime = n.get_uptime_secs();

      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("host", n.get_host());
      dumpGenerator.writeNumberField("port", n.get_port());
      dumpGenerator.writeStringField("nimbusLogLink",
          nimbusLogLink(n.get_host(), n.get_port()));
      if (n.is_isLeader()) {
        dumpGenerator.writeStringField("status", "Leader");
      } else {
        dumpGenerator.writeStringField("status", "Not a Leader");
      }
      dumpGenerator.writeStringField("version", n.get_version());
      dumpGenerator.writeStringField("nimbusUpTime",
          Helpers.prettyUptimeSec(uptime));
      dumpGenerator.writeStringField("nimbusLaunchTime",
          getAddTimeDate(Calendar.SECOND, -uptime));
      dumpGenerator.writeNumberField("nimbusUpTimeSeconds", uptime);
      dumpGenerator.writeEndObject();

    }

  }

  /**
   * Array of all the topology related stats per time window
   * 
   * @param id
   * @param window
   * @param stats
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#topology-stats")
  private static void topologyStats(String id, String window,
      Map<StatsFields, Map<Object, Object>> stats, JsonGenerator dumpGenerator)
          throws IOException, JsonGenerationException {
    Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
    dumpGenerator.writeFieldName("topologyStats");
    dumpGenerator.writeStartArray();
    for (Integer k : times) {
      dumpGenerator.writeStartObject();
      // Duration passed in HH:MM:SS format
      dumpGenerator.writeStringField("windowPretty", windowHint(k.intValue()));
      // User requested time window for metrics
      dumpGenerator.writeNumberField("window", k);
      // Number of messages emitted in given window
      dumpGenerator.writeNumberField("emitted", CoreUtil
          .getLong(stats.get(StatsFields.emitted).get(String.valueOf(k)), 0L));
      // Number messages transferred in given window
      dumpGenerator.writeNumberField("transferred", CoreUtil.getLong(
          stats.get(StatsFields.transferred).get(String.valueOf(k)), 0L));
      // Total latency for processing the message
      dumpGenerator.writeStringField("completeLatency",
          Helpers.floatStr(CoreUtil.getFloat(
              stats.get(StatsFields.complete_latencies).get(String.valueOf(k)),
              0.00f)));
      // Number of messages acked in given window
      dumpGenerator.writeNumberField("acked", CoreUtil
          .getLong(stats.get(StatsFields.acked).get(String.valueOf(k)), 0L));
      // Number of messages failed in given window
      dumpGenerator.writeNumberField("failed", CoreUtil
          .getLong(stats.get(StatsFields.failed).get(String.valueOf(k)), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();

  }

  /**
   * Array of all the spout components in the topology
   * 
   * @param topId
   * @param summMap
   * @param errors
   * @param window
   * @param includeSys
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#spout-comp")
  private static void spoutComp(String topId,
      Map<String, List<ExecutorSummary>> summMap,
      Map<String, List<ErrorInfo>> errors, String window, boolean includeSys,
      TopologyBackpressure topologyBackpressure, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    dumpGenerator.writeFieldName("spouts");
    dumpGenerator.writeStartArray();
    for (Map.Entry<String, List<ExecutorSummary>> e : summMap.entrySet()) {
      String id = e.getKey();
      List<ExecutorSummary> summs = e.getValue();
      List<ExecutorStats> statsSeq = Stats.getFilledStats(summs);
      Map<StatsFields, Map<Object, Object>> stats = Stats.aggregateSpoutStreams(
          Stats.aggregateSpoutStats(statsSeq, includeSys));
      ErrorInfo lastError = mostRecentError(errors.get(id));
      String errorHost = getErrorHost(lastError);
      int errorPort = getErrorPort(lastError);
      dumpGenerator.writeStartObject();
      // Spout id
      dumpGenerator.writeStringField("spoutId", id);
      // Encoded Spout id
      dumpGenerator.writeStringField("encodedSpoutId", CoreUtil.urlEncode(id));
      // Number of executors for the spout
      dumpGenerator.writeNumberField("executors", summs.size());
      // Total number of tasks for the spout
      dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
      // Number of messages emitted in given window
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stats.get(StatsFields.emitted).get(window), 0L));
      // Total number of messages transferred in given window
      dumpGenerator.writeNumberField("transferred",
          CoreUtil.getLong(stats.get(StatsFields.transferred).get(window), 0L));
      // Total latency for processing the message

      dumpGenerator
          .writeStringField("completeLatency",
              Helpers.floatStr(CoreUtil.getFloat(
                  stats.get(StatsFields.complete_latencies).get(window),
                  0.00f)));
      // Number of messages acked
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stats.get(StatsFields.acked).get(window), 0L));
      // Number of messages failed
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stats.get(StatsFields.failed).get(window), 0L));
      // Error worker Hostname
      if (errorHost != null) {
        dumpGenerator.writeStringField("errorHost", errorHost);
      }
      if (errorPort != 0) {
        // Error worker port
        dumpGenerator.writeNumberField("errorPort", errorPort);
        // Link to the worker log that reported the exception
        dumpGenerator.writeStringField("errorWorkerLogLink",
            workerLogLink(errorHost, errorPort, topId));
      }
      if (lastError != null && lastError.get_error() != null
          && !lastError.get_error().isEmpty()) {
        // Number of seconds elapsed since that last error happened in a
        // spout
        dumpGenerator.writeNumberField("errorLapsedSecs",
            getErrorTimeDelta(lastError));
        // Shows the last error happened in a spout
        dumpGenerator.writeStringField("lastError", lastError.get_error());
      }
      dumpGenerator.writeNumberField("bpCount",
          topologyBackpressure.get_componentToBackpressure().get(id) == null ? 0
              : topologyBackpressure.get_componentToBackpressure().get(id));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * Array of bolt components in the topology
   * 
   * @param topId
   * @param summMap
   * @param errors
   * @param window
   * @param includeSys
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-comp")
  private static void boltComp(String topId,
      Map<String, List<ExecutorSummary>> summMap,
      Map<String, List<ErrorInfo>> errors, String window, boolean includeSys,
      TopologyBackpressure topologyBackpressure, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    dumpGenerator.writeFieldName("bolts");
    dumpGenerator.writeStartArray();
    for (Map.Entry<String, List<ExecutorSummary>> e : summMap.entrySet()) {
      String id = e.getKey();
      List<ExecutorSummary> summs = e.getValue();
      if (!includeSys && Utils.isSystemId(id)) {
        continue;
      }
      List<ExecutorStats> statsSeq = Stats.getFilledStats(summs);
      Map<StatsFields, Map<Object, Object>> stats = Stats
          .aggregateBoltStreams(Stats.aggregateBoltStats(statsSeq, includeSys));
      ErrorInfo lastError = mostRecentError(errors.get(id));
      String errorHost = getErrorHost(lastError);
      int errorPort = getErrorPort(lastError);
      dumpGenerator.writeStartObject();
      // Bolt id
      dumpGenerator.writeStringField("boltId", id);
      // Encoded Bolt id
      dumpGenerator.writeStringField("encodedBoltId", CoreUtil.urlEncode(id));
      // Number of executor tasks in the bolt component
      dumpGenerator.writeNumberField("executors", summs.size());
      // Number of instances of bolt
      dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
      // Number of tuples emitted
      Map<Object, Object> res = stats.get(StatsFields.emitted);
      dumpGenerator.writeNumberField("emitted",
          res == null ? 0L : CoreUtil.getLong(res.get(window), 0L));
      // Total number of messages transferred in given window
      res = stats.get(StatsFields.transferred);
      dumpGenerator.writeNumberField("transferred",
          res == null ? 0 : CoreUtil.getLong(res.get(window), 0L));
      // This value indicates number of messages executed * average
      // execute
      // latency / time window

      dumpGenerator.writeStringField("capacity", Helpers.floatStr(CoreUtil
          .getFloat(String.valueOf(Stats.computeBoltCapacity(summs)), 0.00f)));
      // Average time for bolt's execute method
      res = stats.get(StatsFields.execute_latencies);
      dumpGenerator.writeStringField("executeLatency", res == null ? "0.00"
          : Helpers.floatStr(CoreUtil.getFloat(res.get(window), 0.00f)));
      // Total number of messages executed in given window
      res = stats.get(StatsFields.executed);
      dumpGenerator.writeNumberField("executed",
          res == null ? 0 : CoreUtil.getLong(res.get(window), 0L));
      // Bolt's average time to ack a message after it's received
      res = stats.get(StatsFields.process_latencies);
      dumpGenerator.writeStringField("processLatency", res == null ? "0.00"
          : Helpers.floatStr(CoreUtil.getFloat(res.get(window), 0.00f)));
      // Number of tuples acked by the bolt
      res = stats.get(StatsFields.acked);
      dumpGenerator.writeNumberField("acked",
          (res == null) ? 0 : CoreUtil.getLong(res.get(window), 0L));
      // Number of tuples failed by the bolt
      res = stats.get(StatsFields.failed);
      dumpGenerator.writeNumberField("failed",
          (res == null) ? 0 : CoreUtil.getLong(res.get(window), 0L));
      if (errorHost != null) {
        // Error worker Hostname
        dumpGenerator.writeStringField("errorHost", errorHost);
      }
      if (errorPort != 0) {
        // Error worker port
        dumpGenerator.writeNumberField("errorPort", errorPort);
        // Link to the worker log that reported the exception
        dumpGenerator.writeStringField("errorWorkerLogLink",
            workerLogLink(errorHost, errorPort, topId));
      }
      if (lastError != null && lastError.get_error() != null
          && !lastError.get_error().isEmpty()) {

        // Number of seconds elapsed since that last error happened in a
        // bolt
        dumpGenerator.writeNumberField("errorLapsedSecs",
            getErrorTimeDelta(lastError));
        // Shows the last error occurred in the bolt
        dumpGenerator.writeStringField("lastError", lastError.get_error());
      }
      dumpGenerator.writeNumberField("bpCount",
          topologyBackpressure.get_componentToBackpressure().get(id) == null ? 0
              : topologyBackpressure.get_componentToBackpressure().get(id));
      dumpGenerator.writeEndObject();
    }

    dumpGenerator.writeEndArray();
  }

  /**
   * topologySummary
   * 
   * @param summ
   * @param dumpGenerator
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#topology-summary")
  private static void topologySummary(TopologyInfo summ,
      JsonGenerator dumpGenerator) throws TException {
    List<ExecutorSummary> executors = summ.get_executors();
    Set<NodeInfo> workers = new HashSet<NodeInfo>();
    for (ExecutorSummary e : executors) {
      workers.add(new NodeInfo(e.get_host(),
          Sets.newHashSet(Long.valueOf(e.get_port()))));
    }

    try {
      // Topology Id
      dumpGenerator.writeStringField("id", summ.get_id());
      // Encoded Topology Id
      dumpGenerator.writeStringField("encodedId",
          CoreUtil.urlEncode(summ.get_id()));
      // Topology Name
      dumpGenerator.writeStringField("name", summ.get_name());
      // Shows Topology's current status
      dumpGenerator.writeStringField("status", summ.get_status());
      // Shows how long the topology is running
      dumpGenerator.writeStringField("uptime",
          Helpers.prettyUptimeSec(summ.get_uptime_secs()));
      dumpGenerator.writeStringField("submitTime",
          getAddTimeDate(Calendar.SECOND, -summ.get_uptime_secs()));
      // Total number of tasks for this topology
      dumpGenerator.writeNumberField("tasksTotal", Helpers.sumTasks(executors));
      // Number of workers used for this topology
      dumpGenerator.writeNumberField("workersTotal", workers.size());
      // Number of executors used for this topology
      dumpGenerator.writeNumberField("executorsTotal", executors.size());
    } catch (IOException e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    }
  }

  /**
   * spoutSummaryJson
   * 
   * @param topologyId
   * @param id
   * @param stats
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#spout-summary-json")
  public static void spoutSummaryJson(String topologyId, String id,
      Map<StatsFields, Map<Object, Object>> stats, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
    dumpGenerator.writeFieldName("spoutSummary");
    dumpGenerator.writeStartArray();
    for (Integer k : times) {
      String windowStr = String.valueOf(k);

      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("windowPretty", windowHint(k.intValue()));
      dumpGenerator.writeNumberField("window", k);
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stats.get(StatsFields.emitted).get(windowStr), 0L));
      dumpGenerator.writeNumberField("transferred", CoreUtil
          .getLong(stats.get(StatsFields.transferred).get(windowStr), 0L));
      dumpGenerator.writeStringField("completeLatency",
          Helpers.floatStr(CoreUtil.getFloat(
              stats.get(StatsFields.complete_latencies).get(windowStr),
              0.00f)));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stats.get(StatsFields.acked).get(windowStr), 0L));
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stats.get(StatsFields.failed).get(windowStr), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();

  }

  /**
   * Get the set of all worker host/ports
   * 
   * @param id
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "org.apache.storm.ui.core#exec-host-port")
  public static List<Map> execHostPort(List<ExecutorSummary> executors,
      String isIncludesSys) {
    List<Map> ret = new ArrayList<Map>();
    for (ExecutorSummary executor : executors) {
      Map map = new HashMap();
      String host = (String) executor.get_host();
      int port = (int) executor.get_port();
      map.put("host", host);
      map.put("port", port);
      int uptimeSecs = 0;
      List<String> components = new ArrayList<String>();
      for (ExecutorSummary tmpExecutor : executors) {
        if (host.equals(tmpExecutor.get_host())
            && port == tmpExecutor.get_port()) {
          if (!Core.checkIncludeSys(isIncludesSys)
              && Utils.isSystemId(tmpExecutor.get_component_id())) {
            continue;
          }
          if (!components.contains(tmpExecutor.get_component_id())) {
            components.add(tmpExecutor.get_component_id());
          }
          uptimeSecs = tmpExecutor.get_uptime_secs();
        }
      }
      map.put("uptimeSecs", uptimeSecs);
      map.put("component", components);
      if (!ret.contains(map)) {
        ret.add(map);
      }
    }
    Collections.sort(ret, new Comparator<Map>() {
      @Override
      public int compare(Map o1, Map o2) {
        String host1 = (String) o1.get("host");
        String host2 = (String) o2.get("host");
        int comp = host1.compareTo(host2);
        if (comp == 0) {
          comp = (int) o1.get("port") - (int) o2.get("port");
        }
        return comp;
      }
    });
    return ret;
  }

  /**
   * Get the set of all worker host/ports
   * 
   * @param id
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#worker-host-port")
  public static List<Map> workerHostPort(String id, String isIncludesSys)
      throws TException {
    NimbusClient client = withNimbus();
    try {
      TopologyInfo topologyInfo = client.getClient().getTopologyInfo(id);
      return execHostPort(topologyInfo.get_executors(), isIncludesSys);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "org.apache.storm.ui.core#topology-workers")
  public static void topologyWorkers(String topologyId, String isIncludeSys,
      OutputStreamWriter out) throws TException {
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartArray();
      List<Map> hostPortList = workerHostPort(topologyId, isIncludeSys);
      List<WorkerJvmInfo> workerJvmInfos = getWorkerJvmInfos(topologyId);
      for (Map<String, Object> map : hostPortList) {
        String host = (String) map.get("host");
        Integer port = (int) map.get("port");
        Integer uptimeSecs = (int) map.get("uptimeSecs");
        List<String> components = (List<String>) map.get("component");
        String workerLogLink = Core.workerLogLink(host, port, topologyId);
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("topologyId", topologyId);
        dumpGenerator.writeStringField("component", components.toString());
        dumpGenerator.writeStringField("host", host);
        dumpGenerator.writeNumberField("port", port);
        dumpGenerator.writeStringField("workerLogLink", workerLogLink);
        dumpGenerator.writeStringField("uptimeSecs",
            Helpers.prettyUptimeSec(uptimeSecs));
        dumpGenerator.writeStringField("startTime",
            Core.getAddTimeDate(Calendar.SECOND, -uptimeSecs));
        Map<String, Map<String, String>> jvmInfo =
            getJvmInfo(host, port, workerJvmInfos);
        for (Map.Entry<String, Map<String, String>> jvmInfoDetail : jvmInfo
            .entrySet()) {
          dumpGenerator.writeFieldName(jvmInfoDetail.getKey());
          dumpGenerator.writeStartObject();
          for (Map.Entry<String, String> entry : jvmInfoDetail.getValue()
              .entrySet()) {
            if (MapKeyConstants.JVM_TIME.equals(jvmInfoDetail.getKey())) {
              /*
               * String entryKey = entry.getKey(); String entryValue =
               * entry.getValue(); if
               * (MapKeyConstants.START_TIME_SECS.equals(entryKey)) { Double
               * startTimeMills = Double.valueOf(entryValue) * 1000;
               * dumpGenerator.writeStringField(entryKey.replace("Secs", ""),
               * Core.transferLongToDate(startTimeMills.longValue())); } else if
               * (MapKeyConstants.UP_TIME_SECS.equals(entryKey)) {
               * dumpGenerator.writeStringField(entryKey.replace("Secs", ""),
               * Helpers.prettyUptimeSec(
               * Double.valueOf(entryValue).intValue())); }
               */
            } else if (MapKeyConstants.MEMORY_NONHEAP
                .equals(jvmInfoDetail.getKey())
                || MapKeyConstants.MEMORY_HEAP.equals(jvmInfoDetail.getKey())) {
              int value = Integer.valueOf(entry.getValue());
              if (value != -1) {
                value = value / (1024 * 1024);
              }
              dumpGenerator.writeStringField(
                  entry.getKey().replace("Bytes", "MB"), String.valueOf(value));
            } else {
              dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
          }
          dumpGenerator.writeEndObject();
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.flush();
    } catch (IOException e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    }
  }

  /**
   * topologyPage
   * 
   * @param id
   * @param window
   * @param includeSys
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#topology-page")
  @SuppressWarnings("unchecked")
  public static void topologyPage(String id, String window, boolean includeSys,
      OutputStreamWriter out) throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("NimbusClient", operStart);
    try {
      TopologyPageInfo topologyPageInfo =
          client.getClient().getTopologyPageInfo(id, window, includeSys);
      logOperTime("TopologyPageInfo", operStart);
      String topologyConf = topologyPageInfo.get_topology_conf();
      Map<String, Object> stormConf =
          (Map<String, Object>) CoreUtil.from_json(topologyConf);
      unpackTopologyPageInfo(topologyPageInfo, window, false);
      operStart = Time.currentTimeMillis();
      TopologyInfo summ = client.getClient().getTopologyInfo(id);
      operStart = logOperTime("TopologyInfo", operStart);
      StormTopology topology = client.getClient().getTopology(id);
      logOperTime("StormTopology", operStart);
      TopologyBackpressure topologyBackpressure =
          client.getClient().getTopologyBackpressure(summ.get_name());
      List<ExecutorSummary> executorSummarys = summ.get_executors();
      List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
      List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();

      for (ExecutorSummary s : executorSummarys) {
        if (Core.isSpoutSummary(topology, s)) {
          spoutSumms.add(s);
        } else if (Core.isBoltSummary(topology, s)) {
          boltSumms.add(s);
        }
      }
      Map<String, List<ExecutorSummary>> spoutCompSumms =
          Core.groupByComp(spoutSumms);
      Map<String, List<ExecutorSummary>> boltCompSumms =
          Core.groupByComp(boltSumms);
      Map<String, SpoutSpec> spouts = topology.get_spouts();
      Map<String, Bolt> bolts = topology.get_bolts();

      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      topologySummary(summ, dumpGenerator);
      // window param value defalut value is "0"
      dumpGenerator.writeStringField("window", window);
      // window param value in "hh mm ss" format. Default value is
      // "All Time"
      dumpGenerator.writeStringField("windowHint", Core.windowHint(window));
      // Number of seconds a tuple has before the spout considers it
      // failed
      dumpGenerator.writeNumberField("msgTimeout", Utils
          .getInt(stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30));
      // debug info
      if (topologyPageInfo.get_debug_options() != null) {
        dumpGenerator.writeNumberField("samplingPct",
            topologyPageInfo.get_debug_options().get_samplingpct());
        dumpGenerator.writeBooleanField("debug",
            topologyPageInfo.get_debug_options().is_enable());
      } else {
        dumpGenerator.writeNumberField("samplingPct", 10);
        dumpGenerator.writeBooleanField("debug", false);
      }
      int totalBPCount = 0;
      for (int componentBpCount : topologyBackpressure
          .get_componentToBackpressure().values()) {
        totalBPCount += componentBpCount;
      }
      dumpGenerator.writeNumberField("totalBPCount", totalBPCount);
      // topology-stats
      // Array of all the topology related stats per time window
      operStart = Time.currentTimeMillis();
      Map<StatsFields, Map<Object, Object>> stats = Stats.totalAggregateStats(
          spoutSumms, boltSumms, includeSys, dumpGenerator);
      operStart = logOperTime("Stats.totalAggregateStats", operStart);
      topologyStats(id, window, stats, dumpGenerator);
      // spouts
      operStart = logOperTime("Core.topologyStats", operStart);
      spoutComp(id, spoutCompSumms, summ.get_errors(), window, includeSys,
          topologyBackpressure, dumpGenerator);
      // bolts
      operStart = logOperTime("Core.spoutComp", operStart);
      boltComp(id, boltCompSumms, summ.get_errors(), window, includeSys,
          topologyBackpressure, dumpGenerator);
      logOperTime("Core.boltComp", operStart);
      // configuration
      topologyConf(stormConf, dumpGenerator);

      // visualizationTable
      dumpGenerator.writeFieldName("visualizationTable");
      visualizationData(spouts, bolts, spoutCompSumms, boltCompSumms, window,
          id, false, dumpGenerator);
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      logOperTime("Core.topologyPage", start);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      if (e instanceof NotAliveException) {
        throw (NotAliveException) e;
      } else {
        throw new TException(errorMsg);
      }
    } finally {
      client.close();
    }

  }

  /**
   * Unpacks the serialized object to data structures
   * 
   * @param topologyPageInfo
   * @param window
   * @param isSecure
   */
  @ClojureClass(className = "org.apache.storm.ui.core#unpack-topology-page-info")
  private static void unpackTopologyPageInfo(TopologyPageInfo topologyPageInfo,
      String window, boolean isSecure) {
    // TODO Auto-generated method stub
    TopologyStats topologyStats = topologyPageInfo.get_topology_stats();
  }

  /**
   * spoutOutputStats
   * 
   * @param streamSummary
   * @param window
   * @return
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#topology-page#spout-output-stats")
  public static void spoutOutputStats(
      Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary,
      String window, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {

    Map<Object, Map<StatsFields, Map<Object, Object>>> swap =
        Helpers.swapMapOrder(streamSummary);
    Map<Object, Map<Object, Map<StatsFields, Object>>> tmp =
        new HashMap<Object, Map<Object, Map<StatsFields, Object>>>();
    for (Map.Entry<Object, Map<StatsFields, Map<Object, Object>>> entry : swap
        .entrySet()) {
      tmp.put(entry.getKey(), Helpers.swapMapOrder(entry.getValue()));
    }
    dumpGenerator.writeFieldName("outputStats");
    dumpGenerator.writeStartArray();
    Map<Object, Map<StatsFields, Object>> windowsStat = tmp.get(window) == null
        ? new HashMap<Object, Map<StatsFields, Object>>() : tmp.get(window);
    for (Map.Entry<Object, Map<StatsFields, Object>> entry : windowsStat
        .entrySet()) {
      Map<StatsFields, Object> stat = entry.getValue();
      dumpGenerator.writeStartObject();
      dumpGenerator.writeObjectField("stream", entry.getKey());
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stat.get(StatsFields.emitted), 0L));
      dumpGenerator.writeNumberField("transferred",
          CoreUtil.getLong(stat.get(StatsFields.transferred), 0L));
      dumpGenerator.writeStringField("completeLatency", Helpers.floatStr(
          CoreUtil.getFloat(stat.get(StatsFields.complete_latencies), 0.00f)));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stat.get(StatsFields.acked), 0L));
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stat.get(StatsFields.failed), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * spoutExecutorStats
   * 
   * @param topologyId
   * @param executors
   * @param window
   * @param includeSys
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#spout-executor-stats")
  public static void spoutExecutorStats(String topologyId,
      List<ExecutorSummary> executors, String window, boolean includeSys,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {

    dumpGenerator.writeFieldName("executorStats");
    dumpGenerator.writeStartArray();
    for (ExecutorSummary e : executors) {
      Map<StatsFields, Object> stats = new HashMap<StatsFields, Object>();
      ExecutorStats executorStats = e.get_stats();
      if (executorStats == null) {
        continue;
      }
      Map<StatsFields, Map<Object, Object>> spoutStreamsMap =
          Stats.aggregateSpoutStreams(Stats.aggregateSpoutStats(
              Lists.newArrayList(executorStats), includeSys));
      Map<Object, Map<StatsFields, Object>> swapMapOrder =
          Helpers.swapMapOrder(spoutStreamsMap);
      if (swapMapOrder.get(window) != null) {
        stats = swapMapOrder.get(window);
      }

      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("id",
          Helpers.prettyExecutorInfo(e.get_executor_info()));
      dumpGenerator.writeStringField("encodedId", CoreUtil
          .urlEncode(Helpers.prettyExecutorInfo(e.get_executor_info())));
      dumpGenerator.writeStringField("uptime",
          Helpers.prettyUptimeSec(e.get_uptime_secs()));
      dumpGenerator.writeStringField("host", e.get_host());
      dumpGenerator.writeNumberField("port", e.get_port());
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stats.get(StatsFields.emitted), 0L));
      dumpGenerator.writeNumberField("transferred",
          CoreUtil.getLong(stats.get(StatsFields.transferred), 0L));
      dumpGenerator.writeStringField("completeLatency", Helpers.floatStr(
          CoreUtil.getFloat(stats.get(StatsFields.complete_latencies), 0.00f)));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stats.get(StatsFields.acked), 0L));
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stats.get(StatsFields.failed), 0L));
      dumpGenerator.writeStringField("workerLogLink",
          workerLogLink(e.get_host(), e.get_port(), topologyId));
      dumpGenerator.writeEndObject();

    }
    dumpGenerator.writeEndArray();
  }

  /**
   * List of component errors
   * 
   * @param errorsList
   * @param topologyId
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#component-errors")
  public static void componentErrors(List<ErrorInfo> errorsList,
      String topologyId, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    dumpGenerator.writeFieldName("componentErrors");
    dumpGenerator.writeStartArray();

    for (ErrorInfo e : errorsList) {
      dumpGenerator.writeStartObject();
      // Timestamp when the exception occurred
      dumpGenerator.writeNumberField("time", 1000 * e.get_error_time_secs());
      if (e.get_host() != null) {
        // host name for the error
        dumpGenerator.writeStringField("errorHost", e.get_host());
      }
      if (e.get_port() != 0) {
        // port for the error
        dumpGenerator.writeNumberField("errorPort", e.get_port());
        // Link to the worker log that reported the exception
        dumpGenerator.writeStringField("errorWorkerLogLink",
            workerLogLink(e.get_host(), e.get_port(), topologyId));
      }
      if (e.get_error() != null && !e.get_error().isEmpty()) {
        dumpGenerator.writeNumberField("errorLapsedSecs", getErrorTimeDelta(e));
        // Shows the error happened in a component
        dumpGenerator.writeStringField("error", e.get_error());
      }
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();

  }

  /**
   * spoutStats
   * 
   * @param windows
   * @param topologyInfo
   * @param component
   * @param executors
   * @param includeSys
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#spout-stats")
  public static void spoutStats(String windows, TopologyInfo topologyInfo,
      String component, List<ExecutorSummary> executors, boolean includeSys,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {
    // String windowHint = "(" + windowHint(windows) + ")";
    List<ExecutorStats> stats = Stats.getFilledStats(executors);
    Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary =
        Stats.aggregateSpoutStats(stats, includeSys);
    Map<StatsFields, Map<Object, Object>> summary =
        Stats.aggregateSpoutStreams(streamSummary);

    spoutSummaryJson(topologyInfo.get_id(), component, summary, dumpGenerator);
    spoutOutputStats(streamSummary, windows, dumpGenerator);
    spoutExecutorStats(topologyInfo.get_id(), executors, windows, includeSys,
        dumpGenerator);
  }

  /**
   * boltSummary
   * 
   * @param topologyId
   * @param id
   * @param stats
   * @param window
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-summary")
  public static void boltSummary(String topologyId, String id,
      Map<StatsFields, Map<Object, Object>> stats, String window,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {
    Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
    dumpGenerator.writeFieldName("boltStats");
    dumpGenerator.writeStartArray();
    for (Integer k : times) {
      String windowStr = String.valueOf(k);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeNumberField("window", k);
      dumpGenerator.writeStringField("windowPretty", windowHint(k.intValue()));
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stats.get(StatsFields.emitted).get(windowStr), 0L));
      dumpGenerator.writeNumberField("transferred", CoreUtil
          .getLong(stats.get(StatsFields.transferred).get(windowStr), 0L));
      dumpGenerator
          .writeStringField("executeLatency",
              Helpers.floatStr(CoreUtil.getFloat(
                  stats.get(StatsFields.execute_latencies).get(windowStr),
                  0.00f)));
      dumpGenerator.writeNumberField("executed",
          CoreUtil.getLong(stats.get(StatsFields.executed).get(windowStr), 0L));
      dumpGenerator
          .writeStringField("processLatency",
              Helpers.floatStr(CoreUtil.getFloat(
                  stats.get(StatsFields.process_latencies).get(windowStr),
                  0.00f)));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stats.get(StatsFields.acked).get(windowStr), 0L));
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stats.get(StatsFields.failed).get(windowStr), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * boltSummary
   * 
   * @param streamSummay
   * @param window
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-output-stats")
  public static void boltOutputStats(
      Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummay,
      String window, JsonGenerator dumpGenerator)
          throws JsonGenerationException, IOException {
    Map<StatsFields, Map<Object, Object>> statsFields =
        Helpers.swapMapOrder(streamSummay).get(window) == null
            ? new HashMap<StatsFields, Map<Object, Object>>()
            : Helpers.swapMapOrder(streamSummay).get(window);
    Map<Object, Map<StatsFields, Object>> streams = Helpers.swapMapOrder(
        CoreUtil.select_keys(statsFields, new HashSet<StatsFields>(
            Arrays.asList(StatsFields.emitted, StatsFields.transferred))));

    dumpGenerator.writeFieldName("outputStats");
    dumpGenerator.writeStartArray();
    for (Map.Entry<Object, Map<StatsFields, Object>> entry : streams
        .entrySet()) {
      Object s = entry.getKey();
      Map<StatsFields, Object> stats = entry.getValue();

      dumpGenerator.writeStartObject();
      dumpGenerator.writeObjectField("stream", String.valueOf(s));
      dumpGenerator.writeNumberField("emitted",
          CoreUtil.getLong(stats.get(StatsFields.emitted), 0L));
      dumpGenerator.writeNumberField("transferred",
          CoreUtil.getLong(stats.get(StatsFields.transferred), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * boltOutputStats
   * 
   * @param streamSummay
   * @param window
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-input-stats")
  public static void boltInputStats(
      Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummay,
      String window, JsonGenerator dumpGenerator, boolean includeSys)
          throws JsonGenerationException, IOException {
    Map<StatsFields, Map<Object, Object>> statsFields =
        Helpers.swapMapOrder(streamSummay).get(window) == null
            ? new HashMap<StatsFields, Map<Object, Object>>()
            : Helpers.swapMapOrder(streamSummay).get(window);
    Set<StatsFields> selectKeys =
        new HashSet<StatsFields>(Arrays.asList(StatsFields.acked,
            StatsFields.failed, StatsFields.process_latencies,
            StatsFields.executed, StatsFields.execute_latencies));
    Map<Object, Map<StatsFields, Object>> streams =
        Helpers.swapMapOrder(CoreUtil.select_keys(statsFields, selectKeys));

    dumpGenerator.writeFieldName("inputStats");
    dumpGenerator.writeStartArray();
    for (Map.Entry<Object, Map<StatsFields, Object>> entry : streams
        .entrySet()) {
      GlobalStreamId s = (GlobalStreamId) entry.getKey();
      Map<StatsFields, Object> stats = entry.getValue();
      if (!includeSys && Utils.isSystemId(s.get_componentId())) {
        continue;
      }
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("component", s.get_componentId());
      dumpGenerator.writeStringField("encodedComponent",
          CoreUtil.urlDecode(s.get_componentId()));
      dumpGenerator.writeStringField("stream", s.get_streamId());

      dumpGenerator.writeStringField("executeLatency", Helpers.floatStr(
          CoreUtil.getFloat(stats.get(StatsFields.execute_latencies), 0.00f)));
      dumpGenerator.writeStringField("processLatency", Helpers.floatStr(
          CoreUtil.getFloat(stats.get(StatsFields.process_latencies), 0.00f)));
      dumpGenerator.writeNumberField("executed",
          CoreUtil.getLong(stats.get(StatsFields.executed), 0L));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(stats.get(StatsFields.acked), 0L));
      dumpGenerator.writeNumberField("failed",
          CoreUtil.getLong(stats.get(StatsFields.failed), 0L));
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
  }

  /**
   * boltExecutorStats
   * 
   * @param topologyId
   * @param executors
   * @param window
   * @param includeSys
   * @param dumpGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-executor-stats")
  public static void boltExecutorStats(String topologyId,
      List<ExecutorSummary> executors, String window, boolean includeSys,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {
    dumpGenerator.writeFieldName("executorStats");
    dumpGenerator.writeStartArray();
    for (ExecutorSummary e : executors) {
      ExecutorStats stats = e.get_stats();
      Map<StatsFields, Object> statsMap = new HashMap<StatsFields, Object>();
      if (stats != null) {
        Map<StatsFields, Map<Object, Object>> statsFeilds =
            Stats.aggregateBoltStreams(
                Stats.aggregateBoltStats(Arrays.asList(stats), includeSys));
        statsMap = Helpers.swapMapOrder(statsFeilds).get(window);
      }

      dumpGenerator.writeStartObject();
      ExecutorInfo executorInfo = e.get_executor_info();
      String id = Helpers.prettyExecutorInfo(executorInfo);
      dumpGenerator.writeStringField("id", id);
      dumpGenerator.writeStringField("encodeId", CoreUtil.urlEncode(id));
      dumpGenerator.writeStringField("uptime",
          Helpers.prettyUptimeSec(e.get_uptime_secs()));
      dumpGenerator.writeStringField("host", e.get_host());
      dumpGenerator.writeNumberField("port", e.get_port());
      dumpGenerator.writeNumberField("emitted", CoreUtil
          .getLong(CoreUtil.mapValue(statsMap, StatsFields.emitted), 0L));
      dumpGenerator.writeNumberField("transferred", CoreUtil
          .getLong(CoreUtil.mapValue(statsMap, StatsFields.transferred), 0L));
      dumpGenerator.writeStringField("capacity", Helpers.floatStr(CoreUtil
          .getFloat(String.valueOf(Stats.computeExecutorCapacity(e)), 0.00f)));
      dumpGenerator.writeStringField("executeLatency",
          Helpers.floatStr(CoreUtil.getFloat(
              CoreUtil.mapValue(statsMap, StatsFields.execute_latencies),
              0.00f)));
      dumpGenerator.writeNumberField("executed", CoreUtil
          .getLong(CoreUtil.mapValue(statsMap, StatsFields.executed), 0L));
      dumpGenerator.writeStringField("processLatency",
          Helpers.floatStr(CoreUtil.getFloat(
              CoreUtil.mapValue(statsMap, StatsFields.process_latencies),
              0.00f)));
      dumpGenerator.writeNumberField("acked",
          CoreUtil.getLong(CoreUtil.mapValue(statsMap, StatsFields.acked), 0L));
      dumpGenerator.writeNumberField("failed", CoreUtil
          .getLong(CoreUtil.mapValue(statsMap, StatsFields.failed), 0L));
      dumpGenerator.writeStringField("workerLogLink",
          workerLogLink(e.get_host(), e.get_port(), topologyId));

      dumpGenerator.writeEndObject();
    }

    dumpGenerator.writeEndArray();
  }

  /**
   * bolt-stats
   * 
   * @param window
   * @param summ
   * @param component
   * @param summs
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#bolt-stats")
  public static void boltStats(String window, TopologyInfo topologyInfo,
      String component, List<ExecutorSummary> executors, boolean includeSys,
      JsonGenerator dumpGenerator) throws JsonGenerationException, IOException {

    List<ExecutorStats> stats = Stats.getFilledStats(executors);
    Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary =
        Stats.aggregateBoltStats(stats, includeSys);
    Map<StatsFields, Map<Object, Object>> summary =
        Stats.aggregateBoltStreams(streamSummary);

    String topologyId = topologyInfo.get_id();

    boltSummary(topologyId, component, summary, window, dumpGenerator);
    boltInputStats(streamSummary, window, dumpGenerator, includeSys);
    boltOutputStats(streamSummary, window, dumpGenerator);
    boltExecutorStats(topologyId, executors, window, includeSys, dumpGenerator);
  }

  /**
   * Component Page
   * 
   * @param topologyId
   * @param component
   * @param window
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#component-page")
  public static void componentPage(String topologyId, String component,
      String window, String includeSys, OutputStreamWriter out)
          throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("NimbusClient", operStart);
    try {
      TopologyInfo summ = client.getClient().getTopologyInfo(topologyId);
      operStart = logOperTime("TopologyInfo", operStart);
      StormTopology topology = client.getClient().getTopology(topologyId);
      logOperTime("StormTopology", operStart);
      ComponentPageInfo compPageInfo = client.getClient().getComponentPageInfo(
          topologyId, component, window, checkIncludeSys(includeSys));
      boolean debugEnabled = compPageInfo.get_debug_options() != null
          ? compPageInfo.get_debug_options().is_enable() : false;
      double samplingPct = compPageInfo.get_debug_options() != null
          ? compPageInfo.get_debug_options().get_samplingpct() : 0.0d;
      ComponentType type = Core.componentType(topology, component);

      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);

      List<ExecutorSummary> summs =
          componentTaskSumms(summ, topology, component);

      dumpGenerator.writeStartObject();
      // Component's
      dumpGenerator.writeStringField("id", component);
      // Encode Component's
      dumpGenerator.writeStringField("encodedId",
          CoreUtil.urlEncode(component));
      // Topology name
      dumpGenerator.writeStringField("name", summ.get_name());
      // Number of executor tasks in the component
      dumpGenerator.writeNumberField("executors", summs.size());
      // Number of instances of component
      dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
      // topology's id
      dumpGenerator.writeStringField("topologyId", topologyId);
      // encoded topology's id
      dumpGenerator.writeStringField("encodedTopologyId",
          CoreUtil.urlEncode(topologyId));
      dumpGenerator.writeStringField("window", window);
      // component's type SPOUT or BOLT
      dumpGenerator.writeStringField("componentType", type.name());
      // window param value in "hh mm ss" format. Default value is
      // "All Time"
      dumpGenerator.writeStringField("windowHint", windowHint(window));
      dumpGenerator.writeStringField("topologyStatus",
          compPageInfo.get_topology_status());
      dumpGenerator.writeBooleanField("debug", debugEnabled);
      dumpGenerator.writeNumberField("samplingPct", samplingPct);
      dumpGenerator.writeStringField("eventLogLink",
          eventLogLink(topologyId, 0, compPageInfo.get_eventlog_host(),
              compPageInfo.get_eventlog_port(), false));
      dumpGenerator.writeBooleanField("profilingAndDebuggingCapable",
          !Utils.isOnWindows());
      dumpGenerator.writeBooleanField("profileActionEnabled", Utils
          .getBoolean(STORM_CONF.get(Config.WORKER_PROFILER_ENABLED), false));
      if (Utils.getBoolean(STORM_CONF.get(Config.WORKER_PROFILER_ENABLED),
          false)) {
        dumpGenerator.writeFieldName("profilerActive");
        dumpGenerator.writeStartArray();
        Map<String, String> profileAction = getActiveProfileActions(client,
            topologyId, compPageInfo.get_component_id());
        if (!profileAction.isEmpty()) {
          dumpGenerator.writeStartObject();
          for (Map.Entry<String, String> entry : profileAction.entrySet()) {
            if (entry.getKey().equals("timestamp")) {
              dumpGenerator.writeStringField(entry.getKey(),
                  transferLongToDate(Long.valueOf(entry.getValue())));
            } else {
              dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
          }
          dumpGenerator.writeEndObject();
        }
        dumpGenerator.writeEndArray();
      }
      if (type == ComponentType.SPOUT) {
        operStart = Time.currentTimeMillis();
        spoutStats(window, summ, component, summs, checkIncludeSys(includeSys),
            dumpGenerator);
        logOperTime("Core.spoutStats", operStart);
      } else if (type == ComponentType.BOLT) {
        operStart = Time.currentTimeMillis();
        boltStats(window, summ, component, summs, checkIncludeSys(includeSys),
            dumpGenerator);
        logOperTime("Core.boltStats", operStart);
      }
      componentErrors(summ.get_errors().get(component), topologyId,
          dumpGenerator);
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      dumpGenerator.close();
      logOperTime("Core.componentPage", start);
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      throw new TException(errMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#level-to-dict")
  public static Map<String, String> levelToDict(LogLevel level) {
    int timeout = level.get_reset_log_level_timeout_secs();
    long timeoutEpoch = level.get_reset_log_level_timeout_epoch();
    String targetLevel = level.get_target_log_level();
    String resetLevel = level.get_reset_log_level();
    Map<String, String> ret = new HashMap<String, String>();
    ret.put("target_level", targetLevel);
    ret.put("reset_level", resetLevel);
    ret.put("timeout", String.valueOf(timeout));
    ret.put("timeout_epoch", String.valueOf(timeoutEpoch));
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.ui.core#log-config")
  public static void logConfig(String topologyId, JsonGenerator dumpGenerator)
      throws Exception {
    NimbusClient client = withNimbus();
    try {
      LogConfig logConfig = client.getClient().getLogConfig(topologyId);
      Map<String, LogLevel> namedLoggerLvel =
          logConfig.get_named_logger_level();
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("namedLoggerLevels");
      dumpGenerator.writeStartObject();
      for (Map.Entry<String, LogLevel> entry : namedLoggerLvel.entrySet()) {
        dumpGenerator.writeFieldName(entry.getKey());
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, String> level : levelToDict(entry.getValue())
            .entrySet()) {
          dumpGenerator.writeStringField(level.getKey(), level.getValue());
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndObject();
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      throw new TException(errMsg);
    } finally {
      client.close();
    }
  }

  /**
   * dumpGenerator
   * 
   * @param stormConf
   * @param dumpGenerator
   * @throws IOException
   * @throws JsonGenerationException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#topology-page#topology-conf")
  public static void topologyConf(Map<String, Object> stormConf,
      JsonGenerator dumpGenerator) throws IOException, JsonGenerationException {
    dumpGenerator.writeFieldName("configuration");
    synchronized (stormConf) {
      dumpGenerator.writeStartObject();
      for (Map.Entry<String, Object> item : stormConf.entrySet()) {
        dumpGenerator.writeStringField(String.valueOf(item.getKey()),
            String.valueOf(stormConf.get((String) item.getKey())));
      }
      dumpGenerator.writeEndObject();
    }
  }

  /**
   * checkIncludeSys
   * 
   * @param sys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.core#check-include-sys?")
  public static boolean checkIncludeSys(String sys) {
    if (sys != null && sys.equals("true")) {
      return true;
    }
    return false;

  }

  /**
   * exceptionToJson
   * 
   * @param ex
   * @param out
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.ui.core#exception->json")
  public static void exceptionToJson(Exception ex, OutputStreamWriter out)
      throws TException {
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("error", "Internal Server Error");
      dumpGenerator.writeStringField("errorMessage",
          CoreUtil.stringifyError(ex));
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      throw new TException(errMsg);
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#main-routes#kill")
  public static void killTopology(String topologyId, int waitTime)
      throws TException {
    NimbusClient client = withNimbus();
    try {
      TopologyInfo tplg = client.getClient().getTopologyInfo(topologyId);
      CoreUtil.killTopology(tplg.get_name(), waitTime);
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      throw new TException(errMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#debug")
  @Deprecated
  public static void debug(String name, String component, boolean enable,
      double samplingPercentage) throws TException {
    NimbusClient client = withNimbus();
    try {
      client.getClient().debug(name, component, enable, samplingPercentage);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#setWorkerProfiler")
  public static void setWorkerProfiler(String id, ProfileRequest profileRequest)
      throws TException {
    NimbusClient client = withNimbus();
    try {
      client.getClient().setWorkerProfiler(id, profileRequest);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  @ClojureClass(className = "org.apache.storm.ui.core#get-active-profile-actions")
  public static Map<String, String> getActiveProfileActions(NimbusClient nimbus,
      String topologyId, String compoent) throws TException {
    List<ProfileRequest> profileActions =
        nimbus.getClient().getComponentPendingProfileActions(topologyId,
            compoent, ProfileAction.JPROFILE_START);
    Map<String, String> activateActions = new HashMap<String, String>();
    if (profileActions == null || profileActions.size() < 1) {
      return activateActions;
    }
    ProfileRequest proflieAction = profileActions.get(0);
    String host = proflieAction.get_nodeInfo().get_node();
    int port =
        proflieAction.get_nodeInfo().get_port().iterator().next().intValue();
    String dumpLink = workerDumpLink(host, port, topologyId);
    Long timeStamp = proflieAction.get_time_stamp();
    activateActions.put("host", host);
    activateActions.put("port", String.valueOf(port));
    activateActions.put("dumplink", dumpLink);
    activateActions.put("timestamp", String.valueOf(timeStamp));
    return activateActions;
  }

  @ClojureClass(className = "org.apache.storm.ui.core#logconfig")
  public static void logconfig(String topologyId,
      Map<String, Map<String, String>> namedLoggerLevels) throws TException {
    NimbusClient client = withNimbus();
    try {
      for (Map.Entry<String, Map<String, String>> entry : namedLoggerLevels
          .entrySet()) {
        String loggerName = entry.getKey();
        Map<String, String> level = entry.getValue();
        String targetLevel = level.get("target_level");
        String timeoutStr = level.get("timeout") == null ? "0"
            : String.valueOf(level.get("timeout"));
        int timeout = Integer.valueOf(timeoutStr);
        LogConfig newLogConfig = new LogConfig();
        LogLevel namedLoggerLevel = new LogLevel();
        LOG.info("The target level for " + loggerName + " is " + targetLevel);
        if (StringUtils.isEmpty(targetLevel)) {
          namedLoggerLevel.set_action(LogLevelAction.REMOVE);
          namedLoggerLevel.unset_target_log_level();
        } else {
          namedLoggerLevel.set_action(LogLevelAction.UPDATE);
          namedLoggerLevel
              .set_target_log_level(Level.toLevel(targetLevel).name());
          namedLoggerLevel.set_reset_log_level_timeout_secs(timeout);
        }
        LOG.info("Adding this" + loggerName + " " + namedLoggerLevel.toString()
            + " to " + newLogConfig.toString());
        newLogConfig.put_to_named_logger_level(loggerName, namedLoggerLevel);
        LOG.info("Setting topology " + topologyId + " log config "
            + newLogConfig.toString());
        client.getClient().setLogConfig(topologyId, newLogConfig);
      }
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }

  }

  /**
   * submit topology for rest api
   * 
   * @param topologyName
   * @param uploadJarLocation
   * @param mainClass
   * @param args
   * @throws TException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void submitTopologyForRestApi(String uploadJarLocation,
      String mainClass, String args, String stormOptions) throws Exception {
    try {
      String[] argsArr = translateArgs(args, uploadJarLocation);
      File file = new File(uploadJarLocation);
      if (!file.exists()) {
        throw new Exception(
            "Jar file[path: " + uploadJarLocation + "] not exits! ");
      }
      String jartransformerClass = getJartransformerClass(stormOptions);
      String tmpJar = "/tmp/storm-" + Time.currentTimeMillis() + ".jar";
      if (StringUtils.isNotEmpty(jartransformerClass)) {
        String[] tansformerArgs = new String[3];
        tansformerArgs[0] = jartransformerClass;
        tansformerArgs[1] = uploadJarLocation;
        tansformerArgs[2] = tmpJar;
        ClientJarTransformerRunner.main(tansformerArgs);
        uploadJarLocation = tmpJar;
      }
      URL jarurl = new URL("file:" + uploadJarLocation);
      ClassLoader classLoader = new URLClassLoader(new URL[] { jarurl });
      Thread.currentThread().setContextClassLoader(classLoader);
      mainClass = mainClass.trim();
      Class clazz = classLoader.loadClass(mainClass);
      Method mainMethod = clazz.getDeclaredMethod("main", String[].class);
      System.setProperty("storm.jar", uploadJarLocation);
      System.setProperty("storm.options", stormOptions);
      LOG.info("submit topology  jarPath '{}' mainClass '{}' args '{}' ",
          uploadJarLocation, mainClass, args);
      mainMethod.invoke(clazz.newInstance(), (Object) argsArr);
      FileUtils.deleteQuietly(new File(tmpJar));
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    }
  }

  private static String getJartransformerClass(String stormOptions) {
    String jartransformerClass = "";
    if (stormOptions != null) {
      String[] stormOptionsArr = stormOptions.split(",");
      for (String stormOption : stormOptionsArr) {
        if (stormOption.indexOf((String) Config.CLIENT_JAR_TRANSFORMER) > -1) {
          String[] options = stormOption.split("=", 2);
          if (options.length == 2
              && options[0].equals((String) Config.CLIENT_JAR_TRANSFORMER)) {
            jartransformerClass = options[1];
            break;
          }
        }
      }
    }
    if (StringUtils.isEmpty(jartransformerClass)) {
      jartransformerClass =
          (String) STORM_CONF.get(Config.CLIENT_JAR_TRANSFORMER);
    }
    return jartransformerClass;
  }

  private static String[] translateArgs(String args, String uploadJarLocation)
      throws IOException {
    String[] argsArr = args.split(ApiCommon.ARGS_SPLITOR);
    List<String> argsList = new ArrayList<String>();
    for (String tmpArgs : argsArr) {
      tmpArgs = tmpArgs.trim();
      if (tmpArgs.startsWith("-") && tmpArgs.indexOf(" ") > -1) {
        String[] tmpArgsArr = tmpArgs.split(" +");
        argsList.add(tmpArgsArr[0].trim());
        argsList.add(tmpArgsArr[1].trim());
      } else {
        argsList.add(tmpArgs.trim());
      }
    }
    // if arg is a file parameter, then use file path
    File file = new File(uploadJarLocation);
    File paramFileDir = new File(file.getCanonicalFile() + "-param");
    if (paramFileDir.exists()) {
      for (File paramFile : paramFileDir.listFiles()) {
        if (paramFile.isDirectory()) {
          continue;
        }
        for (int i = 0; i < argsList.size(); i++) {
          if (argsList.get(i).equals(paramFile.getName())) {
            argsList.set(i, paramFile.getCanonicalPath());
          }
        }
      }
    }
    String[] retArgsArr = new String[argsList.size()];
    retArgsArr = argsList.toArray(retArgsArr);
    return retArgsArr;
  }

  public static void restApiResponseWrite(HttpServletResponse resp,
      String resultCode, String resultMsg) throws IOException {
    restApiResponseWrite(resp, resultCode, resultMsg, null);
  }

  public static void restApiResponseWrite(HttpServletResponse resp,
      String resultCode, String resultMsg, Map<String, String> respContent)
          throws IOException {
    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField(ApiCommon.RESULT_CODE, resultCode);
      dumpGenerator.writeStringField(ApiCommon.RESULT_MSG, resultMsg);
      if (respContent != null && !respContent.isEmpty()) {
        dumpGenerator.writeFieldName(ApiCommon.RESULT_CONTENT);
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, String> entry : respContent.entrySet()) {
          dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } finally {
      out.close();
    }
  }

  public static void nonEmptyParameterCheck(String parameterName,
      String parameterValue) throws Exception {
    if (StringUtils.isEmpty(parameterValue)) {
      throw new Exception(
          "non-empty parameter '{" + parameterName + "}' missing!");
    }
  }

  public static void positiveParameterCheck(String parameterName,
      String parameterValue) throws Exception {
    if (StringUtils.isEmpty(parameterValue)
        || Integer.valueOf(parameterValue) <= 0) {
      throw new Exception("positive parameter '{" + parameterName
          + "}' unvalid, it must be more than 0!");
    }
  }

  /**
   * supervisor topology detail Page
   * 
   * @param topologyId
   * @param component
   * @param window
   * @param out
   * @throws TException
   */
  public static void supervisorTopologyDetail(String supervisorId,
      boolean isIncludeSys, OutputStreamWriter out) throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("NimbusClient", operStart);
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      ClusterSummary clusterInfo = client.getClient().getClusterInfo();
      SupervisorSummary currentSupervisor =
          getSupervisorById(clusterInfo.get_supervisors(), supervisorId);
      operStart = logOperTime("SupervisorSummary", operStart);
      if (currentSupervisor != null) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("supervisorId", supervisorId);
        dumpGenerator.writeNumberField("slotsUsed",
            currentSupervisor.get_num_used_workers());
        dumpGenerator.writeNumberField("slotsTotal",
            currentSupervisor.get_num_workers());
        dumpGenerator.writeStringField("host", currentSupervisor.get_host());
        dumpGenerator.writeStringField("submitTime", getAddTimeDate(
            Calendar.SECOND, -currentSupervisor.get_uptime_secs()));
        dumpGenerator.writeStringField("uptime",
            Helpers.prettyUptimeSec(currentSupervisor.get_uptime_secs()));

        dumpGenerator.writeFieldName("topologies");
        dumpGenerator.writeStartArray();
        operStart = Time.currentTimeMillis();
        List<TopologySummary> topologies = clusterInfo.get_topologies();
        operStart = logOperTime("TopologySummary", operStart);
        for (TopologySummary topology : topologies) {
          if (topology.get_num_workers() < 1) {
            continue;
          }
          operStart = Time.currentTimeMillis();
          TopologyInfo topologyInfo =
              client.getClient().getTopologyInfo(topology.get_id());
          operStart =
              logOperTime("TopologyInfo" + topology.get_id(), operStart);
          List<ExecutorSummary> executors =
              filterExecutors(topologyInfo.get_executors(), isIncludeSys);
          if (executors.size() < 1) {
            continue;
          }
          for (ExecutorSummary executor : executors) {
            if (currentSupervisor.get_host().equals(executor.get_host())) {
              dumpGenerator.writeStartObject();
              dumpGenerator.writeNumberField("port", executor.get_port());
              dumpGenerator.writeStringField("componentId",
                  executor.get_component_id());
              dumpGenerator.writeStringField("executor",
                  executor.get_executor_info().get_task_start() + "-"
                      + executor.get_executor_info().get_task_end());
              dumpGenerator.writeStringField("uptime",
                  Helpers.prettyUptimeSec(executor.get_uptime_secs()));
              dumpGenerator.writeStringField("topologyId",
                  topologyInfo.get_id());
              dumpGenerator.writeStringField("topologyName",
                  topologyInfo.get_name());
              dumpGenerator.writeEndObject();
            }
          }
        }
        dumpGenerator.writeEndArray();
        dumpGenerator.writeEndObject();
      } else {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("supervisorTopologyDetail", "not found");
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.flush();
      dumpGenerator.close();
      operStart = logOperTime("Core.supervisorDetail", start);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  private static SupervisorSummary getSupervisorById(
      List<SupervisorSummary> supervisors, String supervisorId) {
    for (SupervisorSummary supervisor : supervisors) {
      if (supervisorId.equals(supervisor.get_supervisor_id())) {
        return supervisor;
      }
    }
    return null;
  }

  private static List<ExecutorSummary> filterExecutors(
      List<ExecutorSummary> executors, boolean isIncludeSys) {
    List<ExecutorSummary> ret = new ArrayList<ExecutorSummary>();
    for (ExecutorSummary executor : executors) {
      if (!isIncludeSys && Utils.isSystemId(executor.get_component_id())) {
        continue;
      }
      ret.add(executor);
    }
    return ret;
  }

  /**
   * get executors in topology
   * 
   * @param topologyId
   * @param isIncludeSys
   * @param out
   * @throws TException
   */
  public static void topologyExecutors(String topologyId, boolean isIncludeSys,
      OutputStreamWriter out) throws TException {
    long start = Time.currentTimeMillis();
    long operStart = start;
    NimbusClient client = withNimbus();
    operStart = logOperTime("NimbusClient", operStart);
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      TopologyInfo topologyInfo =
          client.getClient().getTopologyInfo(topologyId);
      List<ExecutorSummary> executors =
          filterExecutors(topologyInfo.get_executors(), isIncludeSys);
      operStart = logOperTime("TopologyInfo", operStart);
      List<SupervisorSummary> supervisors =
          client.getClient().getSupervisorSummarys();
      operStart = logOperTime("getSupervisorSummarys from nimbus", operStart);
      Set<NodeInfo> workers = new HashSet<NodeInfo>();
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("topologyId", topologyId);
      dumpGenerator.writeStringField("topologyName", topologyInfo.get_name());
      // Total number of tasks for this topology
      dumpGenerator.writeNumberField("tasksTotal", Helpers.sumTasks(executors));
      // Number of executors used for this topology
      dumpGenerator.writeNumberField("executorsTotal", executors.size());
      dumpGenerator.writeStringField("status", topologyInfo.get_status());
      dumpGenerator.writeStringField("uptime",
          Helpers.prettyUptimeSec(topologyInfo.get_uptime_secs()));
      dumpGenerator.writeFieldName("executors");
      dumpGenerator.writeStartArray();
      for (ExecutorSummary executor : executors) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("host", executor.get_host());
        dumpGenerator.writeNumberField("port", executor.get_port());
        dumpGenerator.writeStringField("executor",
            executor.get_executor_info().get_task_start() + "-"
                + executor.get_executor_info().get_task_end());
        dumpGenerator.writeStringField("uptime",
            Helpers.prettyUptimeSec(executor.get_uptime_secs()));
        dumpGenerator.writeStringField("componentId",
            executor.get_component_id());
        // dumpGenerator.writeStringField("topologyId", topologyId);
        SupervisorSummary supervisor =
            getSupervisorByHost(supervisors, executor.get_host());
        if (supervisor != null) {
          dumpGenerator.writeStringField("supervisorId",
              supervisor.get_supervisor_id());
        }
        dumpGenerator.writeEndObject();
        // use for calculate workersTotal
        workers.add(new NodeInfo(executor.get_host(),
            Sets.newHashSet(Long.valueOf(executor.get_port()))));
      }
      dumpGenerator.writeEndArray();
      // Number of workers used for this topology
      dumpGenerator.writeNumberField("workersTotal", workers.size());
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      dumpGenerator.close();
      logOperTime("Core.topologyExecutor", start);
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    } finally {
      client.close();
    }
  }

  /**
   * get supervisorsummay in cluster by host
   * 
   * @param host
   * @return
   * @throws Exception
   */
  private static SupervisorSummary getSupervisorByHost(
      List<SupervisorSummary> supervisors, String host) throws Exception {
    for (SupervisorSummary supervisor : supervisors) {
      if (StringUtils.equals(host, supervisor.get_host())) {
        return supervisor;
      }
    }
    return null;
  }

  public static String getAddTimeDate(int timeType, int timeValue) {
    Calendar cal = Calendar.getInstance();
    cal.add(timeType, timeValue);
    return CMT_DATE_FORMAT.format(cal.getTime());
  }

  /**
   * streamId -> outputField -> componentList
   * 
   * @param topologyId
   * @return
   * @throws TException
   */
  public static Map<String, Map<String, List<ComponentNode>>> topologyDAG(
      String topologyId) throws TException {
    NimbusClient client = withNimbus();
    try {
      StormTopology topology = client.getClient().getTopology(topologyId);
      TopologyInfo topologyInfo =
          client.getClient().getTopologyInfo(topologyId);
      Map<String, SpoutSpec> spouts = topology.get_spouts();
      Map<String, Bolt> bolts = topology.get_bolts();

      Map<String, Map<String, List<ComponentNode>>> spoutToComponent =
          new HashMap<String, Map<String, List<ComponentNode>>>();
      for (Map.Entry<String, SpoutSpec> spout : spouts.entrySet()) {
        Map<String, StreamInfo> streams =
            spout.getValue().get_common().get_streams();
        String component = spout.getKey();
        for (Map.Entry<String, StreamInfo> entry : streams.entrySet()) {
          String key = entry.getKey();
          if (Utils.isSystemId(key)) {
            continue;
          }
          List<String> outputFields = entry.getValue().get_output_fields();
          Map<String, List<ComponentNode>> outputToCompoent =
              new HashMap<String, List<ComponentNode>>();
          for (String outputField : outputFields) {
            ComponentNode node =
                new ComponentNode("spout", component, key, "-1", outputField,
                    getComponentExecutorNum(topologyInfo, component));
            List<ComponentNode> componentList = new ArrayList<ComponentNode>();
            componentList.add(node);
            getNextComponent(componentList, key, component, bolts,
                topologyInfo);
            outputToCompoent.put(outputField, componentList);
          }
          spoutToComponent.put(key, outputToCompoent);
        }
      }
      return spoutToComponent;
    } catch (Exception e) {
      String errorMsg = CoreUtil.stringifyError(e);
      LOG.error(errorMsg);
      throw new TException(errorMsg);
    }
  }

  private static void getNextComponent(List<ComponentNode> componentList,
      String streamId, String prevComponent, Map<String, Bolt> bolts,
      TopologyInfo topologyInfo) {
    for (Map.Entry<String, Bolt> bolt : bolts.entrySet()) {
      String component = bolt.getKey();
      if (Utils.isSystemId(component)) {
        continue;
      }
      for (Entry<GlobalStreamId, Grouping> input : bolt.getValue().get_common()
          .get_inputs().entrySet()) {
        if (streamId.equals(input.getKey().get_streamId())
            && prevComponent.equals(input.getKey().get_componentId())) {
          input.getValue().getFieldValue();
          Map<String, StreamInfo> streams =
              bolt.getValue().get_common().get_streams();
          for (Map.Entry<String, StreamInfo> entry : streams.entrySet()) {
            if (Utils.isSystemId(entry.getKey())) {
              continue;
            }
            List<String> outputFields = entry.getValue().get_output_fields();
            for (String outputField : outputFields) {
              ComponentNode node = new ComponentNode("bolt", component,
                  streamId, prevComponent, outputField,
                  getComponentExecutorNum(topologyInfo, component));
              componentList.add(node);
              getNextComponent(componentList, streamId, component, bolts,
                  topologyInfo);
            }
          }
        }
      }
    }
  }

  private static int getComponentExecutorNum(TopologyInfo topologyInfo,
      String component) {
    int count = 0;
    for (ExecutorSummary executor : topologyInfo.get_executors()) {
      if (component.equals(executor.get_component_id())) {
        count++;
      }
    }
    return count;
  }

  private static long logOperTime(String oper, long operStart) {
    long now = Time.currentTimeMillis();
    long cost = now - operStart;
    LOG.info("PROCESS " + oper + " COST TIME : " + cost + " NOW : " + now);
    return now;
  }

  public static File getTopologyJarDir(String jarType) throws TException {
    String jarTypeDir = "";
    if ("topology.jar".equalsIgnoreCase(jarType)) {
      jarTypeDir = Utils.getString(STORM_CONF.get(Config.TOPOLOGY_JAR_PATH),
          "topology-jar");
    } else if ("tdspider.jar".equalsIgnoreCase(jarType)) {
      jarTypeDir = Utils.getString(STORM_CONF.get(Config.TDSPIDER_JAR_PATH),
          "tdspider-jar");
    } else {
      throw new TException("Save jar dir :" + jarType
          + " not config, config it in storm.yaml please!!!");
    }
    if (!Utils.isAbsolutePath(jarTypeDir)) {
      jarTypeDir = System.getProperty("storm.home") + Utils.FILE_PATH_SEPARATOR
          + jarTypeDir;
    }
    File jarTypeFile = new File(jarTypeDir);
    if (!jarTypeFile.exists()) {
      jarTypeFile.mkdirs();
    }
    return jarTypeFile;
  }

  public static String transferLongToDate(Long millSec) {
    Date date = new Date(millSec);
    return FAST_DATE_FORMAT.format(date);
  }

  public static void uploadTopologyFile(String jarType, String mainJarName,
      String paramFileName, String siteId, HttpServletRequest request)
          throws Exception {
    DiskFileItemFactory dfif = new DiskFileItemFactory();
    dfif.setSizeThreshold(4096);
    ServletFileUpload sfu = new ServletFileUpload(dfif);
    List<FileItem> fileItems = sfu.parseRequest(request);
    if (fileItems == null || fileItems.size() == 0) {
      return;
    }
    FileItem fileItem = fileItems.get(0);
    File topologyJarDir = getTopologyJarDir(jarType);
    File jar = new File(topologyJarDir.getCanonicalFile()
        + Utils.FILE_PATH_SEPARATOR + mainJarName);
    if (!StringUtils.isEmpty(siteId)) {
      File siteJarDir = new File(topologyJarDir.getCanonicalFile()
          + Utils.FILE_PATH_SEPARATOR + SITE_JOB_DIR_PREFIX + siteId);
      if (!siteJarDir.exists()) {
        FileUtils.forceMkdir(siteJarDir);
      }
      jar = new File(siteJarDir.getCanonicalFile() + Utils.FILE_PATH_SEPARATOR
          + mainJarName);
    }
    if (StringUtils.isNotEmpty(paramFileName)) {
      File paramFileDir = new File(topologyJarDir.getCanonicalFile()
          + Utils.FILE_PATH_SEPARATOR + mainJarName + "-param");
      FileUtils.forceMkdir(paramFileDir);
      jar = new File(paramFileDir.getCanonicalFile() + Utils.FILE_PATH_SEPARATOR
          + paramFileName);
    }
    fileItem.write(jar);
  }

  public static List<WorkerJvmInfo> getWorkerJvmInfos(String topologyIds)
      throws TException {
    NimbusClient client = withNimbus();
    try {
      return client.getClient().getWorkerJvmInfos(topologyIds);
    } finally {
      client.close();
    }
  }

  /**
   * generate host to real ports to logviewer real port, for logviewer in
   * docker. e.g.
   * <p>
   * 172.17.0.1:32900 -> {node:172.17.0.1, ports:[32766, 32767, ..., 32788]} =>
   * <br>
   * 172.17.0.1 -> {32766->32900, 32767->32900, ..., 32788->32900}
   * <p>
   * 32900 is real port for logviewer port, 32766 ... is real ports for all
   * expose port.
   * 
   * @param hostToRealHostPorts
   * @return
   * @throws TException
   */
  public static Map<String, Map<Long, String>> hostToRPortsToLvRPort(
      HashMap<String, NodeInfo> hostToRealHostPorts) {
    Map<String, Map<Long, String>> ret =
        new HashMap<String, Map<Long, String>>();

    for (Entry<String, NodeInfo> e : hostToRealHostPorts.entrySet()) {
      String[] splits = e.getKey().split(":");

      String host = splits[0];
      String lvRealport = splits[1];

      Map<Long, String> map;
      if (ret.containsKey(host)) {
        map = ret.get(host);
      } else {
        map = new HashMap<Long, String>();
      }
      for (Long port : e.getValue().get_port()) {
        map.put(port, lvRealport);
      }

      ret.put(host, map);
    }
    return ret;
  }

  /***
   * Generate 9d7ea22c-777c-4836-8ac6-319eda72c1de->37241 (37241 is the real
   * port for the first slot in supervisor)
   * 
   * @param supervisorInfos
   * @return
   */
  public static Map<String, Long> supervisorIdToFirstSlot(
      Map<String, SupervisorInfo> supervisorInfos) {
    Map<String, Long> ret = new HashMap<String, Long>();

    for (Entry<String, SupervisorInfo> e : supervisorInfos.entrySet()) {
      ret.put(e.getKey(), e.getValue().get_meta().get(0));
    }
    return ret;
  }

  private static Map<String, Map<String, String>> getJvmInfo(String host,
      int port, List<WorkerJvmInfo> workerJvmInfos) {
    Map<String, Map<String, String>> ret =
        new HashMap<String, Map<String, String>>();
    for (WorkerJvmInfo workerJvmInfo : workerJvmInfos) {
      Map<String, String> workerInfo =
          workerJvmInfo.get_value().get(MapKeyConstants.WROKER_INFO);
      if (host.equals(workerInfo.get("host"))
          && String.valueOf(port).equals(workerInfo.get("port"))) {
        ret = workerJvmInfo.get_value();
        break;
      }
    }
    return ret;
  }

  /**
   * For docker Gaia.
   * 
   * @param node
   * @param port
   * @return port_1234
   */
  public static String getProxyPath(String node, Long port) {
    Map<Long, String> map = UIServer.getHostToRealPortsToLvPort().get(node);
    if (map == null) {
      return null;
    }
    String realPort = map.get(port);
    if (realPort != null && realPort.length() != 0) {
      return "port_" + realPort;
    }
    return null;
  }

  public static File[] listTopologyJar(String jarType, String siteId)
      throws Exception {
    File topologyJarDir = Core.getTopologyJarDir(jarType);
    File[] listFile = null;
    if (!StringUtils.isEmpty(siteId)) {
      File siteJarDir = new File(topologyJarDir.getCanonicalFile()
          + Utils.FILE_PATH_SEPARATOR + SITE_JOB_DIR_PREFIX + siteId);
      if (siteJarDir.exists()) {
        listFile = siteJarDir.listFiles();
      } else {
        listFile = new File[0];
      }
    } else {
      listFile = topologyJarDir.listFiles();
    }
    return listFile;
  }

  public static void deleteTopologyJar(String jarType, String jarName,
      String siteId) throws Exception {
    File topologyJarDir = Core.getTopologyJarDir(jarType);
    if (!StringUtils.isEmpty(siteId)) {
      topologyJarDir = new File(topologyJarDir.getCanonicalFile()
          + Utils.FILE_PATH_SEPARATOR + SITE_JOB_DIR_PREFIX + siteId);
    }
    File jar = new File(topologyJarDir.getCanonicalFile()
        + Utils.FILE_PATH_SEPARATOR + jarName);
    if (jar.exists()) {
      FileUtils.deleteQuietly(jar);
    }
    File paramFileDir = new File(topologyJarDir.getCanonicalFile()
        + Utils.FILE_PATH_SEPARATOR + jarName + "-param");
    if (paramFileDir.exists()) {
      FileUtils.deleteQuietly(paramFileDir);
    }
  }

}
