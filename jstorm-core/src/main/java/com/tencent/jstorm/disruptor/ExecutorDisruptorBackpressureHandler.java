package com.tencent.jstorm.disruptor;

import org.apache.storm.utils.DisruptorBackpressureCallback;
import org.apache.storm.utils.WorkerBackpressureThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorData;

/**
 * make a handler for the executor's receive disruptor queue to check
 * highWaterMark and lowWaterMark for backpressure
 *
 */
@ClojureClass(className = "backtype.storm.disruptor#disruptor-backpressure-handler")
public class ExecutorDisruptorBackpressureHandler
    implements DisruptorBackpressureCallback {
  private final static Logger LOG =
      LoggerFactory.getLogger(ExecutorDisruptorBackpressureHandler.class);

  private ExecutorData executorData;

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-disruptor-backpressure-handler")
  public ExecutorDisruptorBackpressureHandler(ExecutorData executorData) {
    this.executorData = executorData;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-disruptor-backpressure-handler#fn")
  public void highWaterMark() throws Exception {
    // When receive queue is above highWaterMark
    if (!executorData.getBackpressure().get()) {
      executorData.getBackpressure().set(true);
      LOG.info("executor " + executorData.getExecutorInfo()
          + " is congested, set backpressure flag true");
      WorkerBackpressureThread.notifyBackpressureChecker(
          executorData.getWorker().getBackpressureTrigger());
      executorData.getStormClusterState().updateComponentBackpressure(
          executorData.getStormId(), executorData.getComponentId());
    }

  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-disruptor-backpressure-handler#fn")
  public void lowWaterMark() throws Exception {
    // When receive queue is below lowWaterMark
    if (executorData.getBackpressure().get()) {
      executorData.getBackpressure().set(false);
      LOG.info("executor " + executorData.getExecutorInfo()
          + " is not-congested, set backpressure flag false");
      WorkerBackpressureThread.notifyBackpressureChecker(
          executorData.getWorker().getBackpressureTrigger());
    }

  }

}
