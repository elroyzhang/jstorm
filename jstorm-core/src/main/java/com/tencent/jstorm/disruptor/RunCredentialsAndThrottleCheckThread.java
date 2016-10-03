package com.tencent.jstorm.disruptor;

import com.tencent.jstorm.daemon.worker.WorkerData;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 2:37:27 PM Feb 29, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#schedule-recurring#733")
public class RunCredentialsAndThrottleCheckThread implements Runnable {
  private WorkerData worker;
  private CheckCredentialsChangedCallback credentialsCallback;
  private CheckThrottleChangedCallback throttleCallback;

  public RunCredentialsAndThrottleCheckThread(WorkerData worker,
      CheckCredentialsChangedCallback credentialsCallback,
      CheckThrottleChangedCallback throttleCallback) {
    this.worker = worker;
    this.credentialsCallback = credentialsCallback;
    this.throttleCallback = throttleCallback;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#schedule-recurring#733")
  public void run() {
    if (worker.getWorkerActiveFlag().get()) {
      credentialsCallback.run();
      if (Utils.getBoolean(
          worker.getConf().get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false)) {
        throttleCallback.run();
      }
    }
  }

}
