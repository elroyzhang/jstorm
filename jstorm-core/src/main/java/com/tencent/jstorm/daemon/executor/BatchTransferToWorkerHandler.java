package com.tencent.jstorm.daemon.executor;

import java.util.ArrayList;

import com.lmax.disruptor.EventHandler;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.daemon.worker.transfer.TransferFn;

import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.MutableObject;

public class BatchTransferToWorkerHandler implements EventHandler<Object> {
  private TransferFn workerTransferFn;
  private KryoTupleSerializer serializer;
  private MutableObject cachedEmit =
      new MutableObject(new ArrayList<AddressedTuple>());

  public BatchTransferToWorkerHandler(WorkerData worker,
      ExecutorData executorData) {
    this.workerTransferFn = new TransferFn(worker);
    this.serializer = new KryoTupleSerializer(executorData.getStormConf(),
        executorData.getWorkerContext());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object o, long seqId, boolean isBatchEnd)
      throws Exception {
    if (o == null) {
      return;
    }
    ArrayList<AddressedTuple> aList =
        (ArrayList<AddressedTuple>) cachedEmit.getObject();
    aList.add((AddressedTuple) o);
    if (isBatchEnd) {
      workerTransferFn.transfer(serializer, aList);
      cachedEmit.setObject(new ArrayList<AddressedTuple>());
    }
  }
}
