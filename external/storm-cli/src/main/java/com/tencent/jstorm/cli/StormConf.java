package com.tencent.jstorm.cli;

import java.net.URL;
import java.util.Map;
import java.util.Properties;

public class StormConf {

  private static URL stormDefaultURL = null;
  private static URL stormSiteURL = null;
  private Map conf;

  static {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = StormConf.class.getClassLoader();
    }

    stormDefaultURL = classLoader.getResource("storm-default.yaml");
    stormSiteURL = classLoader.getResource("storm.yaml");
  }

  public StormConf(Class<?> cls) {
    initialize(cls);
  }

  private void initialize(Class<?> cls) {
    // TODO Auto-generated method stub

  }

  public Properties getAllProperties() {
    return getProperties(this);
  }

  private static Properties getProperties(StormConf conf) {
    return null;

  }

}
