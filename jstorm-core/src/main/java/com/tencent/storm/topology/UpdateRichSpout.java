package com.tencent.storm.topology;

import org.apache.storm.topology.base.BaseRichSpout;

public abstract class UpdateRichSpout extends BaseRichSpout {
  private static final long serialVersionUID = 1L;

  public void update(Object msgId) {
  }

}
