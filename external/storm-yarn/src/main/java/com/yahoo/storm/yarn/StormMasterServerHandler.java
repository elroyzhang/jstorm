/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.yarn;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.utils.Utils;

import com.google.common.base.Joiner;
import com.tencent.jstorm.utils.ServerUtils;
import com.yahoo.storm.yarn.generated.StormMaster;

public class StormMasterServerHandler implements StormMaster.Iface {
  private static final Logger LOG = LoggerFactory
      .getLogger(StormMasterServerHandler.class);
  @SuppressWarnings("rawtypes")
  private Map _storm_conf;
  private StormAMRMClient _client;
  private MasterServer _masterServer;

  // process
  private StormProcess nimbusProcess;
  private StormProcess uiProcess;
  private StormProcess drpcProcess;

  public StormMasterServerHandler(@SuppressWarnings("rawtypes") Map storm_conf,
      StormAMRMClient client) {
    _storm_conf = storm_conf;
    setStormHostConf();
    Util.rmNulls(_storm_conf);
    _client = client;
  }

  void init(MasterServer masterServer) {
    _masterServer = masterServer;
  }

  void stop() {
    try {
      stopSupervisors();
      stopUI();
      stopDrpc();
      stopNimbus();
    } catch (TException e) {
      LOG.error("Stop storm master server failed.", e);
    }
  }

  synchronized void stopMaster() {
    try {
      stopUI();
      stopDrpc();
      stopNimbus();
    } catch (TException e) {
      LOG.error(ServerUtils.stringifyError(e));
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  private void setStormHostConf() {
    try {
      String host_addr = InetAddress.getLocalHost().getHostAddress();
      LOG.info("Storm master host:" + host_addr);
      _storm_conf.put(Config.NIMBUS_HOST, host_addr);
    } catch (UnknownHostException ex) {
      LOG.warn("Failed to get IP address of local host");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public String getStormConf() throws TException {
    LOG.info("getting configuration...");
    Map new_conf = new HashMap();
    new_conf.putAll(_storm_conf);
    int port = Utils.getInt(_storm_conf.get("nimbus.thrift.port"));
    if (port == -1) {
      new_conf.put("nimbus.thrift.port", _masterServer.getNimbusPort());
    }
    return JSONValue.toJSONString(new_conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setStormConf(String storm_conf) throws TException {
    LOG.info("setting configuration...");

    // stop processes
    stopSupervisors();
    // stopUI();
    stopNimbus();

    Object json = JSONValue.parse(storm_conf);
    Map<?, ?> new_conf = (Map<?, ?>) json;
    _storm_conf.putAll(new_conf);
    Util.rmNulls(_storm_conf);
    setStormHostConf();

    // start processes
    startNimbus();
    // startUI();
    startSupervisors();
  }

  @Override
  public synchronized void addSupervisors(int number) throws TException {
    LOG.info("adding " + number + " supervisors...");
    // _client.addSupervisors(number);
  }

  @Override
  public void startNimbus() {
    LOG.info("starting nimbus...");
    synchronized (this) {
      if (nimbusProcess != null && nimbusProcess.isAlive()) {
        LOG.info("Received a request to start nimbus, but it is running now");
        return;
      }
      nimbusProcess = new StormProcess("nimbus");
      nimbusProcess.start();
    }
  }

  @Override
  public void stopNimbus() {
    synchronized (this) {
      if (nimbusProcess == null)
        return;
      LOG.info("stopping nimbus...");
      if (!nimbusProcess.isAlive()) {
        LOG.info("Received a request to stop nimbus, but it is not running now");
        return;
      }
      nimbusProcess.stopStormProcess();
      nimbusProcess = null;
    }
  }

  @Override
  public void startUI() throws TException {
    LOG.info("starting UI...");
    synchronized (this) {
      if (uiProcess != null && uiProcess.isAlive()) {
        LOG.info("Received a request to start UI, but it is running now");
        return;
      }
      uiProcess = new StormProcess("ui");
      uiProcess.start();
    }
  }

  @Override
  public void stopUI() throws TException {
    synchronized (this) {
      if (uiProcess == null)
        return;
      LOG.info("stopping UI...");
      if (!uiProcess.isAlive()) {
        LOG.info("Received a request to stop UI, but it is not running now");
        return;
      }
      uiProcess.stopStormProcess();
      uiProcess = null;
    }
  }

  @Override
  public void startDrpc() throws TException {
    LOG.info("Starting drpc service...");
    synchronized (this) {
      if (drpcProcess != null && drpcProcess.isAlive()) {
        LOG.info("Received a request to start drpc service, but it is running now");
        return;
      }
      drpcProcess = new StormProcess("drpc");
      drpcProcess.start();
    }
  }

  @Override
  public void stopDrpc() throws TException {
    synchronized (this) {
      if (drpcProcess == null)
        return;
      LOG.info("Stopping drpc service...");
      if (!drpcProcess.isAlive()) {
        LOG.info("Received a request to stop drpc service, but it is not running now");
        return;
      }
      drpcProcess.stopStormProcess();
      drpcProcess = null;
    }
  }

  @Override
  public void startSupervisors() throws TException {
    LOG.info("starting supervisors...");
    _client.startAllSupervisors();
  }

  @Override
  public void stopSupervisors() throws TException {
    LOG.info("stopping supervisors...");
    _client.stopAllSupervisors();
  }

  @Override
  public void shutdown() throws TException {
    LOG.info("shutdown storm master...");
    _masterServer.stop();
  }

  @Override
  public void killTopology() throws TException {
    try {
      _masterServer.killTopology();
    } catch (NotAliveException e) {
      throw new TException("NotAliveException::" + e.get_msg());
    }
  }

  public StormProcess getNimbusProcess() {
    return nimbusProcess;
  }

  public void setNimbusProcess(StormProcess nimbusProcess) {
    this.nimbusProcess = nimbusProcess;
  }

  public StormProcess getUiProcess() {
    return uiProcess;
  }

  public void setUiProcess(StormProcess uiProcess) {
    this.uiProcess = uiProcess;
  }

  @SuppressWarnings("rawtypes")
  public Map get_storm_conf() {
    return _storm_conf;
  }

  @SuppressWarnings("rawtypes")
  public void set_storm_conf(Map _storm_conf) {
    this._storm_conf = _storm_conf;
  }

  protected class StormProcess extends Thread {
    Process _process;
    String _name;
    volatile String _pid;

    public StormProcess(String name) {
      _name = name;
    }

    public String getPid() {
      return this._pid;
    }

    public void run() {
      startStormProcess();
      try {
        // dirty hack to get pid
        Field f = _process.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        this._pid = f.get(_process).toString();
        LOG.info("Process " + _name + "pid:" + this._pid);
        int exitCode = _process.waitFor();
        if (exitCode != 0) {
          throw new RuntimeException("exitCode return by _process.waitFor(): "
              + exitCode);
        }
        LOG.info("Storm process " + _name + " stopped");
      } catch (Exception e) {
        e.printStackTrace();
        LOG.info("Interrupted => will stop the storm process too");
        _process.destroy();
      }
    }

    private void startStormProcess() {
      try {
        LOG.info("Running: " + Joiner.on(" ").join(buildCommands()));
        ProcessBuilder builder = new ProcessBuilder(buildCommands());
        // builder.redirectError(Redirect.INHERIT);
        // builder.redirectOutput(Redirect.INHERIT);

        _process = builder.start();
        Util.redirectStreamAsync(_process.getErrorStream(), System.out);
        Util.redirectStreamAsync(_process.getInputStream(), System.out);
      } catch (IOException e) {
        LOG.warn("Error starting nimbus process ", e);
      }
    }

    private List<String> buildCommands() throws IOException {
      if (_name == "nimbus") {
        return Util.buildNimbusCommands(_storm_conf);
      } else if (_name == "ui") {
        return Util.buildUICommands(_storm_conf);
      } else if ("drpc".equals("_name")) {
        return Util.buildDrpcCommands(_storm_conf);
      }

      throw new IllegalArgumentException("Cannot build command list for \""
          + _name + "\"");
    }

    public void stopStormProcess() {
      _process.destroy();
    }
  }
}
