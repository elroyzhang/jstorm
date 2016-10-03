package com.tencent.jstorm.daemon.executor;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-task-receiver")
public abstract class ExecutorEventHandler implements EventHandler<Object> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExecutorEventHandler.class);

  protected ExecutorData executorData;

  public ExecutorEventHandler(ExecutorData executorData) {
    this.executorData = executorData;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object tupleBatch, long sequenceId, boolean endOfBatch)
      throws Exception {
    if (tupleBatch == null) {
      return;
    }

    ArrayList<AddressedTuple> tuples = (ArrayList<AddressedTuple>) tupleBatch;
    for (AddressedTuple tuple : tuples) {
      Integer taskId = tuple.getDest();
      Object msg = tuple.getTuple();

      TupleImpl outTuple = null;
      if (msg instanceof Tuple) {
        outTuple = (TupleImpl) msg;
      } else {
        try {
          outTuple = (TupleImpl) executorData.getDeserializer()
              .deserialize((byte[]) msg);
        } catch (Throwable t) {
          LOG.error("Deserialize failed.{}", CoreUtil.stringifyError(t));
          return;
        }
      }

      if (executorData.getDebug()) {
        LOG.info("Processing received message FOR " + taskId + " TUPLE: "
            + outTuple);
      }

      if (taskId != AddressedTuple.BROADCAST_DEST) {
        tupleActionFn(taskId, outTuple);
      } else {
        // null task ids are broadcast tuples
        for (Integer t : executorData.getTaskIds()) {
          tupleActionFn(t, outTuple);
        }
      }
    }
  }

  protected abstract void tupleActionFn(Integer taskId, TupleImpl outTuple);
}
