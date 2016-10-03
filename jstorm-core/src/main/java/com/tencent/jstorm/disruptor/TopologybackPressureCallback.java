package com.tencent.jstorm.disruptor;

import org.apache.storm.cluster.IStormClusterState;

import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:11:03 AM Mar 15, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#callback")
public class TopologybackPressureCallback extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  private String stormId;
  private WorkerData worker;
  private IStormClusterState stormClusterState;

  public TopologybackPressureCallback(String stormId, WorkerData worker,
      IStormClusterState stormClusterState) {
    this.stormId = stormId;
    this.worker = worker;
    this.stormClusterState = stormClusterState;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-worker#callback")
  public void run() {
    try {
      boolean throttleOn =
          stormClusterState.topologyBackpressure(stormId, null);
      worker.getThrottleOn().set(throttleOn);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
