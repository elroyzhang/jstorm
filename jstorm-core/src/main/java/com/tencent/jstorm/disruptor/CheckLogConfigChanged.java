package com.tencent.jstorm.disruptor;

import org.apache.storm.generated.LogConfig;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.daemon.worker.WorkerUtils;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 10:27:14 AM Jun 8, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-worker#check-log-config-changed")
public class CheckLogConfigChanged extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  private String stormId;
  private WorkerData worker;

  public CheckLogConfigChanged(String stormId, WorkerData worker) {
    this.stormId = stormId;
    this.worker = worker;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#check-log-config-changed")
  public void run() {
    LogConfig logConfig =
        worker.getStormClusterState().topologyLogConfig(stormId, null);
    WorkerUtils.processLogConfigChange(worker.getLatestLogConfig(),
        worker.getOriginalLogLevels(), logConfig);
    establishLogSettingCallback();
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#establish-log-setting-callback")
  public void establishLogSettingCallback() {
    worker.getStormClusterState().topologyLogConfig(stormId, this);
  }
}
