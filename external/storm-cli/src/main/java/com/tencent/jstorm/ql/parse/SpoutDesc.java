package com.tencent.jstorm.ql.parse;

import org.apache.storm.spout.ISpout;

public class SpoutDesc {
  private ISpout spout;
  private Integer parallelismHint;

  public SpoutDesc(ISpout spout, Integer parallelismHint) {
    this.spout = spout;
    this.parallelismHint = parallelismHint;
  }

  public Integer getParallelismHint() {
    return parallelismHint;
  }

  public void setParallelismHint(Integer parallelismHint) {
    this.parallelismHint = parallelismHint;
  }

  public ISpout getSpout() {
    return spout;
  }

  public void setSpout(ISpout spout) {
    this.spout = spout;
  }

}
