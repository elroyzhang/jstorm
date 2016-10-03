package com.tencent.flink.config;

import java.util.Map;

import backtype.storm.Config;

public class FlinkStormConfig extends Config {

  public void setNimibusHost(Map conf, String host) {
    conf.put(Config.NIMBUS_HOST, host);
  }

  public void setNimibusHost(String host) {
    setNimibusHost(this, host);
  }

  public void setNimibusThriftPort(Map conf, int port) {
    conf.put(Config.NIMBUS_THRIFT_PORT, port);
  }

  public void setNimibusThriftPort(int port) {
    setNimibusThriftPort(this, port);
  }
}
