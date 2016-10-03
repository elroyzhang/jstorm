package com.tencent.jstorm.daemon.worker.threads;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.ConnectionWithStatus.Status;
import org.apache.storm.messaging.IConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;

/**
 * we will wait all connections to be ready and then activate the spout/bolt
 * when the worker bootup
 *
 */

@ClojureClass(className = "backtype.storm.daemon.worker#activate-worker-when-all-connections-ready")
public class ActivateWorkerWhenAllConnectionsReady implements Runnable {
  private static Logger LOG =
      LoggerFactory.getLogger(ActivateWorkerWhenAllConnectionsReady.class);
  private WorkerData worker;

  public ActivateWorkerWhenAllConnectionsReady(WorkerData worker) {
    this.worker = worker;
  }

  @Override
  public void run() {
    if (allConnectionsReady(worker)) {
      LOG.debug(
          "All connections are ready for worker " + worker.getAssignmentId()
              + ":" + worker.getPort() + " with id " + worker.getWorkerId());
      worker.getWorkerActiveFlag().set(true);
    } else {
      worker.getWorkerActiveFlag().set(false);
    }
  }

  /**
   * all connections are ready
   * 
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#all-connections-ready")
  public boolean allConnectionsReady(WorkerData worker) {
    ConcurrentHashMap<String, IConnection> connections =
        worker.getCachedNodeportToSocket();
    for (IConnection connection : connections.values()) {
      if (!isConnectionReady(connection)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Check whether this messaging connection is ready to send data
   * 
   * @param connection
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#is-connection-ready")
  public boolean isConnectionReady(IConnection connection) {
    if (connection instanceof ConnectionWithStatus) {
      Status status = ((ConnectionWithStatus) connection).status();
      return status.equals(ConnectionWithStatus.Status.Ready);
    }
    return true;
  }
}
