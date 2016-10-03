package com.tencent.jstorm.ql.parse;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;

public class Context {
  private Config conf;
  // alias --> [IRichSpout, parallelismHint]
  private Map<String, SpoutDesc> spoutDescs;
  // alias --> [IRichBolt, parallelismHint, groupingType, Fields]
  private Map<String, BoltDesc> boltDescs;

  public Context(Map<String, SpoutDesc> spoutDescs,
      Map<String, BoltDesc> boltDescs) {
    this.spoutDescs = spoutDescs;
    this.boltDescs = boltDescs;
  }

  public Context() {
    this.spoutDescs = new HashMap<String, SpoutDesc>();
    this.boltDescs = new HashMap<String, BoltDesc>();
  }

  public Map<String, SpoutDesc> getSpoutDescs() {
    return spoutDescs;
  }

  public void setSpoutDescs(Map<String, SpoutDesc> spoutDescs) {
    this.spoutDescs = spoutDescs;
  }

  public Map<String, BoltDesc> getBoltDescs() {
    return boltDescs;
  }

  public void setBoltDescs(Map<String, BoltDesc> boltDescs) {
    this.boltDescs = boltDescs;
  }

  public Config getConf() {
    return conf;
  }

  public void setConf(Config conf) {
    this.conf = conf;
  }

}
