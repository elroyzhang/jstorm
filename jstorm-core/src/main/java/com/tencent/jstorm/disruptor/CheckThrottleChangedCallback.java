package com.tencent.jstorm.disruptor;

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
 * @ModifiedTime 11:16:41 AM Mar 15, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#check-throttle-changed")
public class CheckThrottleChangedCallback extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  private String stormId;
  private WorkerData worker;

  public CheckThrottleChangedCallback(String stormId, WorkerData worker) {
    this.stormId = stormId;
    this.worker = worker;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#check-throttle-changed")
  public void run() {
    try {
      TopologybackPressureCallback callback = new TopologybackPressureCallback(
          stormId, worker, worker.getStormClusterState());
      boolean newThrottleOn =
          worker.getStormClusterState().topologyBackpressure(stormId, callback);
      worker.getThrottleOn().set(newThrottleOn);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
