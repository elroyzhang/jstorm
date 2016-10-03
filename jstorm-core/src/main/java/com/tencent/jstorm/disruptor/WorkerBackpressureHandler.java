package com.tencent.jstorm.disruptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.utils.WorkerBackpressureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.worker.WorkerData;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:16:58 AM Mar 15, 2016
 */
// make a handler that checks and updates worker's backpressure flag
@ClojureClass(className = "backtype.storm.disruptor#worker-backpressure-handler")
public class WorkerBackpressureHandler implements WorkerBackpressureCallback {
  private static Logger LOG =
      LoggerFactory.getLogger(WorkerBackpressureHandler.class);

  private WorkerData worker;
  private String stormId;
  private String assignmentId;
  private IStormClusterState stormClusterState;
  private List<ExecutorShutdown> executors;

  public WorkerBackpressureHandler(WorkerData worker,
      List<ExecutorShutdown> executors) {
    this.worker = worker;
    this.stormId = worker.getTopologyId();
    this.assignmentId = worker.getAssignmentId();
    this.stormClusterState = worker.getStormClusterState();
    this.executors = executors;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-backpressure-handler")
  public void onEvent(Object obj) {
    AtomicBoolean prevBackpressureFlag = worker.getBackpressure();
    AtomicBoolean currBackpressureFlag = new AtomicBoolean(false);
    if (worker.getStormActiveAtom().get()) {
      if (executors != null && executors.size() > 0) {
        boolean isBackPressure = false;
        for (ExecutorShutdown executor : executors) {
          isBackPressure = executor.get_backpressure_flag().get();
          if (isBackPressure) {
            break;
          }
        }
        currBackpressureFlag
            .set(worker.getTransferQueue().getThrottleOn() || isBackPressure);
        // worker.getBackpressure()
        // .set(worker.getTransferBackpressure().get() || isBackPressure);
      } else {
        currBackpressureFlag.set(prevBackpressureFlag.get());
      }

      // update the worker's backpressure flag to zookeeper only when it has
      // changed
      LOG.debug("BP " + worker.getBackpressure().get() + " WAS "
          + prevBackpressureFlag.get());

      if (prevBackpressureFlag.get() != currBackpressureFlag.get()) {
        try {
          stormClusterState.workerBackpressure(stormId, assignmentId,
              Long.valueOf(worker.getPort()), currBackpressureFlag.get());
          // doing the local reset after the zk update succeeds is very
          // important to avoid a bad state upon zk exception
          worker.getBackpressure().set(currBackpressureFlag.get());
        } catch (Exception e) {
          LOG.error(
              "workerBackpressure update failed when connecting to ZK ... will retry");
        }
      }
    }
  }

}
