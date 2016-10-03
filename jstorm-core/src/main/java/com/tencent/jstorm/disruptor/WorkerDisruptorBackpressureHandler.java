package com.tencent.jstorm.disruptor;

import com.tencent.jstorm.daemon.worker.WorkerData;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.DisruptorBackpressureCallback;
import org.apache.storm.utils.WorkerBackpressureThread;

/**
 * make a handler for the worker's send disruptor queue to check highWaterMark
 * and lowWaterMark for backpressure
 *
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-disruptor-backpressure-handler")
public class WorkerDisruptorBackpressureHandler
    implements DisruptorBackpressureCallback {

  private WorkerData workerData;

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-disruptor-backpressure-handler")
  public WorkerDisruptorBackpressureHandler(WorkerData workerData) {
    this.workerData = workerData;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-disruptor-backpressure-handler#fn")
  public void highWaterMark() throws Exception {
    if (!workerData.getBackpressure().get()) {
      workerData.getBackpressure().set(true);
      WorkerBackpressureThread
          .notifyBackpressureChecker(workerData.getBackpressureTrigger());
    }

  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-disruptor-backpressure-handler#fn")
  public void lowWaterMark() throws Exception {
    if (workerData.getBackpressure().get()) {
      workerData.getBackpressure().set(false);
      WorkerBackpressureThread
          .notifyBackpressureChecker(workerData.getBackpressureTrigger());
    }
  }
}