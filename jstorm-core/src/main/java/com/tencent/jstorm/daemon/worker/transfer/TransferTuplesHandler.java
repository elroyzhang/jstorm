package com.tencent.jstorm.daemon.worker.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.utils.TransferDrainer;

import com.lmax.disruptor.EventHandler;
import com.tencent.jstorm.daemon.worker.WorkerData;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 1:51:45 PM Feb 26, 2016
 */
public class TransferTuplesHandler implements EventHandler<Object> {

  private HashMap<String, IConnection> nodeportToSocket;
  private ReentrantReadWriteLock.ReadLock endpointSocketReadLock;
  private TransferDrainer drainer;
  private HashMap<Integer, String> taskToNodeport;

  public TransferTuplesHandler(WorkerData workerData) {
    this.nodeportToSocket = new HashMap<String, IConnection>(
        workerData.getCachedNodeportToSocket());
    this.endpointSocketReadLock = workerData.getEndpointSocketLock().readLock();
    this.drainer = new TransferDrainer();
    this.taskToNodeport =
        new HashMap<Integer, String>(workerData.getCachedTaskToNodeport());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object packets, long sequence, boolean endOfBatch)
      throws Exception {
    if (packets == null) {
      return;
    }
    drainer.add((HashMap<Integer, ArrayList<TaskMessage>>) packets);
    if (endOfBatch) {
      endpointSocketReadLock.lock();
      try {
        drainer.send(taskToNodeport, nodeportToSocket);
      } finally {
        endpointSocketReadLock.unlock();
      }
      drainer.clear();
    }
  }
}
