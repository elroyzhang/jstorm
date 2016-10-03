package com.tencent.jstorm.daemon.executor;

import java.util.ArrayList;
import java.util.Arrays;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Constants;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;

@ClojureClass(className = "backtype.storm.daemon.executor#setup-metrics!#fn")
public class MetricsRunable implements Runnable {

  private DisruptorQueue receiveQueue;
  private WorkerTopologyContext context;
  private Integer tickTimeSecs;

  public MetricsRunable(ExecutorData executorData, Integer tickTimeSecs) {
    this.receiveQueue = executorData.getReceiveQueue();
    this.context = executorData.getWorkerContext();
    this.tickTimeSecs = tickTimeSecs;
  }

  @Override
  public void run() {
    TupleImpl tupleImpl =
        new TupleImpl(context, Arrays.asList((Object) tickTimeSecs),
            (int)Constants.SYSTEM_TASK_ID, Constants.METRICS_TICK_STREAM_ID);
    AddressedTuple tm =
        new AddressedTuple(AddressedTuple.BROADCAST_DEST, tupleImpl);
    ArrayList<AddressedTuple> pairs = new ArrayList<AddressedTuple>();
    pairs.add(tm);
    receiveQueue.publish(pairs);
  }

}
