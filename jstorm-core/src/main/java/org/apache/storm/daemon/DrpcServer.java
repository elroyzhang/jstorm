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
package org.apache.storm.daemon;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.Servlet;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.ui.FilterConfiguration;
import org.apache.storm.ui.IConfigurator;
import org.apache.storm.ui.UIHelpers;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.thrift.TException;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.drpc.DRPCServerInfo;
import com.tencent.jstorm.daemon.drpc.api.DRPCFuncServlet;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.CoreUtil;

public class DrpcServer implements DistributedRPC.Iface, DistributedRPCInvocations.Iface, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DrpcServer.class);
    private final Long timeoutCheckSecs = 5L;

    private Map conf;
    /** added by JStorm developer*/
    private List<ACL> acls = null;
    private IStormClusterState stormClusterState;

    private ThriftServer handlerServer;
    private ThriftServer invokeServer;
    private IHttpCredentialsPlugin httpCredsHandler;

    private Thread clearThread;

    private IAuthorizer authorizer;

    private Servlet httpServlet;

    private AtomicInteger ctr = new AtomicInteger(0);
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();


    private static class InternalRequest {
        public final Semaphore sem;
        public final int startTimeSecs;
        public final String function;
        public final DRPCRequest request;
        public volatile Object result;

        public InternalRequest(String function, DRPCRequest request) {
            sem = new Semaphore(0);
            startTimeSecs = Time.currentTimeSecs();
            this.function = function;
            this.request = request;
        }
    }

    private ConcurrentHashMap<String, InternalRequest> outstandingRequests = new ConcurrentHashMap<>();

    private final static Meter meterHttpRequests = StormMetricsRegistry.registerMeter("drpc:num-execute-http-requests");
    private final static Meter meterExecuteCalls = StormMetricsRegistry.registerMeter("drpc:num-execute-calls");
    private final static Meter meterResultCalls = StormMetricsRegistry.registerMeter("drpc:num-result-calls");
    private final static Meter meterFailRequestCalls = StormMetricsRegistry.registerMeter("drpc:num-failRequest-calls");
    private final static Meter meterFetchRequestCalls = StormMetricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
    private final static Meter meterShutdownCalls = StormMetricsRegistry.registerMeter("drpc:num-shutdown-calls");

    @ClojureClass(className="add drpc server into zk")
    public DrpcServer(Map conf) {
        this.conf = conf;
        this.authorizer = mkAuthorizationHandler((String) (this.conf.get(Config.DRPC_AUTHORIZER)));

        /** Added by JStorm developer*/
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
          this.acls = NimbusUtils.nimbusZKAcls();
        }
        try {
          this.stormClusterState = ClusterUtils.mkStormClusterState(conf, acls,
              new ClusterStateContext(DaemonType.UNKNOWN));
        } catch (Exception e) {
          LOG.error("Start drpc server failed! {}", CoreUtil.stringifyError(e));
          throw new RuntimeException(e);
        }

        initClearThread();
    }

    public void setHttpServlet(Servlet httpServlet) {
        this.httpServlet = httpServlet;
    }

    private ThriftServer initHandlerServer(final DrpcServer service) throws Exception {
        int port = (int) conf.get(Config.DRPC_PORT);
        if (port > 0) {
            handlerServer = new ThriftServer(conf, new DistributedRPC.Processor<DistributedRPC.Iface>(service), ThriftConnectionType.DRPC);
        }
        return handlerServer;
    }

    private ThriftServer initInvokeServer(final DrpcServer service) throws Exception {
        invokeServer = new ThriftServer(conf, new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(service),
                ThriftConnectionType.DRPC_INVOCATIONS);
        return invokeServer;
    }
    
    @ClojureClass(className = "mod by JStorm developer for http://git.oa.com/trc/jstorm/issues/7")
    private void initHttp(final DrpcServer service) throws Exception {
        LOG.info("Starting  RPC Http servers...");
        Integer drpcHttpPort = (Integer) conf.get(Config.DRPC_HTTP_PORT);
        if (drpcHttpPort != null && drpcHttpPort > 0) {
            String filterClass = (String) (conf.get(Config.DRPC_HTTP_FILTER));
            Map<String, String> filterParams = (Map<String, String>) (conf.get(Config.DRPC_HTTP_FILTER_PARAMS));
            FilterConfiguration filterConfiguration = new FilterConfiguration(filterParams, filterClass);
            final List<FilterConfiguration> filterConfigurations = Arrays.asList(filterConfiguration);
            final Integer httpsPort = Utils.getInt(conf.get(Config.DRPC_HTTPS_PORT), 0);
            final String httpsKsPath = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PATH));
            final String httpsKsPassword = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PASSWORD));
            final String httpsKsType = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_TYPE));
            final String httpsKeyPassword = (String) (conf.get(Config.DRPC_HTTPS_KEY_PASSWORD));
            final String httpsTsPath = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PATH));
            final String httpsTsPassword = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PASSWORD));
            final String httpsTsType = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_TYPE));
            final Boolean httpsWantClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_WANT_CLIENT_AUTH));
            final Boolean httpsNeedClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_NEED_CLIENT_AUTH));

            UIHelpers.stormRunJetty(drpcHttpPort, new IConfigurator() {
                @Override
                public void execute(Server s) {
                    UIHelpers.configSsl(s, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword, httpsTsPath, httpsTsPassword, httpsTsType,
                            httpsNeedClientAuth, httpsWantClientAuth);
                    UIHelpers.configFilter(s, httpServlet, filterConfigurations);
                    //add by JStorm developer for http://git.oa.com/trc/jstorm/issues/7
                    if (s.getHandlers().length > 0) {
                      ServletContextHandler context = (ServletContextHandler) s.getHandlers()[0];
                      context.setAttribute("handler", service);
                    }
                }
            });
        }

    }

    @ClojureClass(className="add drpc server info into zk")
    private void initThrift() throws Exception {
        handlerServer = initHandlerServer(this);
        invokeServer = initInvokeServer(this);
        httpCredsHandler = AuthUtils.GetDrpcHttpCredentialsPlugin(conf);

        // write drpc server id into zk
        DRPCServerInfo drpcServerInfo = DRPCServerInfo.fromConf(conf);
        LOG.info("add drpc server id : {} into zk.", drpcServerInfo);
        stormClusterState.addDrpcServer(drpcServerInfo);

        Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() {
            @Override
            public void run() {
                if (handlerServer != null)
                    handlerServer.stop();
                invokeServer.stop();
            }
        });
        LOG.info("Starting Distributed RPC servers...");
        new Thread(new Runnable() {

            @Override
            public void run() {
                invokeServer.serve();
            }
        }).start();

        StormMetricsRegistry.startMetricsReporters(conf);

        if (handlerServer != null)
            handlerServer.serve();
    }

    private void initClearThread() {
        clearThread = Utils.asyncLoop(new Callable() {

            @Override
            public Object call() throws Exception {
                for (Map.Entry<String, InternalRequest> e : outstandingRequests.entrySet()) {
                    InternalRequest internalRequest = e.getValue();
                    if (Time.deltaSecs(internalRequest.startTimeSecs) > Utils.getInt(conf.get(Config.DRPC_REQUEST_TIMEOUT_SECS))) {
                        String id = e.getKey();
                        Semaphore sem = internalRequest.sem;
                        if (sem != null) {
                            String func = internalRequest.function;
                            acquireQueue(func).remove(internalRequest.request);
                            LOG.warn("Timeout DRPC request id: {} start at {}", id, e.getValue());
                            sem.release();
                        }
                        cleanup(id);
                    }
                }
                return getTimeoutCheckSecs();
            }
        });
    }

    public Long getTimeoutCheckSecs() {
        return timeoutCheckSecs;
    }

    @ClojureClass(className = "modify by JStorm develper for: http://git.oa.com/trc/jstorm/issues/6")
    public void launchServer() throws Exception {
        LOG.info("Starting drpc server for storm version {}", VersionInfo.getVersion());
        try {
          initHttp(this);  
        } catch (Exception e) {
          LOG.error(CoreUtil.stringifyError(e));
        }
        try {
          initThrift();  
        } catch (Exception e) {
          LOG.error(CoreUtil.stringifyError(e));
          throw e;
        }
    }

    @Override
    public void close() {
        meterShutdownCalls.mark();
        clearThread.interrupt();
    }

    public void cleanup(String id) {
        outstandingRequests.remove(id);
    }

    @ClojureClass(className = "modify by jstorm develper, for http://git.code.oa.com/trc/jstorm/issues/62")
    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException, org.apache.thrift.TException {
        if (StringUtils.isEmpty(functionName)) {
          LOG.info("Received DRPC empty func_name request for {} ({}) at {} ", functionName, funcArgs, System.currentTimeMillis());
          return "can't execute by given empty func_name ";
        }
        meterExecuteCalls.mark();
        LOG.debug("Received DRPC request for {} ({}) at {} ", functionName, funcArgs, System.currentTimeMillis());
        Map<String, String> map = new HashMap<>();
        map.put(DRPCAuthorizerBase.FUNCTION_NAME, functionName);
        checkAuthorization(authorizer, map, "execute", functionName);

        int newid = 0;
        int orig = 0;
        do {
            orig = ctr.get();
            newid = (orig + 1) % 1000000000;
        } while (!ctr.compareAndSet(orig, newid));
        String strid = String.valueOf(newid);

        DRPCRequest req = new DRPCRequest(funcArgs, strid);
        InternalRequest internalRequest = new InternalRequest(functionName, req);
        this.outstandingRequests.put(strid, internalRequest);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        queue.add(req);
        LOG.debug("Waiting for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());
        try {
            internalRequest.sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.debug("Acquired for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());

        Object result = internalRequest.result;

        LOG.debug("Returning for DRPC request for " + functionName + " " + funcArgs + " at " + (System.currentTimeMillis()));

        this.cleanup(strid);

        if (result instanceof DRPCExecutionException) {
            throw (DRPCExecutionException) result;
        }
        if (result == null) {
            throw new DRPCExecutionException("Request timed out");
        }
        return (String) result;
    }

    @ClojureClass(className = "modify by jstorm develper, for http://git.code.oa.com/trc/jstorm/issues/62")
    @Override
    public void result(String id, String result) throws AuthorizationException, TException {
        meterResultCalls.mark();
        InternalRequest internalRequest = this.outstandingRequests.get(id);
        if (internalRequest != null) {
            Map<String, String> map = ImmutableMap.of(DRPCAuthorizerBase.FUNCTION_NAME, internalRequest.function);
            checkAuthorization(authorizer, map, "result", internalRequest.function);
            Semaphore sem = internalRequest.sem;
            LOG.debug("Received result {} for {} at {}", result, id, System.currentTimeMillis());
            if (sem != null) {
                internalRequest.result = result;
                sem.release();
            }
        }
    }

    @ClojureClass(className = "modify by jstorm develper, for http://git.code.oa.com/trc/jstorm/issues/62")
    @Override
    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException, TException {
        meterFetchRequestCalls.mark();
        Map<String, String> map = new HashMap<>();
        map.put(DRPCAuthorizerBase.FUNCTION_NAME, functionName);
        checkAuthorization(authorizer, map, "fetchRequest", functionName);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        DRPCRequest req = queue.poll();
        if (req != null) {
            LOG.debug("Fetched request for {} at {}", functionName, System.currentTimeMillis());
            return req;
        } else {
            return new DRPCRequest("", "");
        }
    }

    @ClojureClass(className = "modify by jstorm develper, for http://git.code.oa.com/trc/jstorm/issues/62")
    @Override
    public void failRequest(String id) throws AuthorizationException, TException {
        meterFailRequestCalls.mark();
        InternalRequest internalRequest = this.outstandingRequests.get(id);
        if (internalRequest != null) {
            Map<String, String> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, internalRequest.function);
            checkAuthorization(authorizer, map, "failRequest", internalRequest.function);
            Semaphore sem = internalRequest.sem;
            if (sem != null) {
                internalRequest.result = new DRPCExecutionException("Request failed");
                sem.release();
            }
        }
    }

    protected ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
        ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
        if (reqQueue == null) {
            reqQueue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<DRPCRequest> old = requestQueues.putIfAbsent(function, reqQueue);
            if (old != null) {
                reqQueue = old;
            }
        }
        return reqQueue;
    }

    @ClojureClass(className = "modify by jstorm develper, add function name for http://git.code.oa.com/trc/jstorm/issues/62")
    private void checkAuthorization(IAuthorizer aclHandler, Map mapping, String operation, ReqContext reqContext, String functionName) throws AuthorizationException {
        if (reqContext != null) {
            LOG.debug("Request ID: {} access from: {} principal: {} operation: {} function name: {}",
                 reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(), operation, functionName);
//            ThriftAccessLogger.logAccess(reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(), operation);
        }
        if (aclHandler != null) {
            if (reqContext == null)
                reqContext = ReqContext.context();
            if (!aclHandler.permit(reqContext, operation, mapping)) {
                Principal principal = reqContext.principal();
                String user = (principal != null) ? principal.getName() : "unknown";
                throw new AuthorizationException("DRPC request '" + operation + "' for '" + user + "' user is not authorized");
            }
        }
    }

    @ClojureClass(className = "modify by jstorm develper, add function name for http://git.code.oa.com/trc/jstorm/issues/62")
    private void checkAuthorization(IAuthorizer aclHandler, Map mapping, String operation, String functionName) throws AuthorizationException {
        checkAuthorization(aclHandler, mapping, operation, ReqContext.context(), functionName);
    }

    // TO be replaced by Common.mkAuthorizationHandler
    private IAuthorizer mkAuthorizationHandler(String klassname) {
        IAuthorizer authorizer = null;
        Class aznClass = null;
        if (StringUtils.isNotBlank(klassname)) {
            try {
                aznClass = Class.forName(klassname);
                authorizer = (IAuthorizer) aznClass.newInstance();
                authorizer.prepare(conf);
            } catch (Exception e) {
                LOG.error("mkAuthorizationHandler failed!", e);
            }
        }
        LOG.debug("authorization class name: {} class: {} handler: {}", klassname, aznClass, authorizer);
        return authorizer;
    }

    /**
     * add by JStorm developer for http://git.oa.com/trc/jstorm/issues/6
     * @param args
     * @throws Exception
     */
    @ClojureClass(className = "backtype.storm.daemon.drpc#main")
    public static void main(String[] args) throws Exception {
      Utils.setupDefaultUncaughtExceptionHandler();
      final DrpcServer server = new DrpcServer(ConfigUtils.readStormConfig());
      server.setHttpServlet(new DRPCFuncServlet());
      server.launchServer();
    }
}