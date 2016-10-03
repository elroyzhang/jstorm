package com.yahoo.storm.yarn;

import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class MasterHeartbeatThread extends RunnableCallback {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(MasterHeartbeatThread.class);
  private StormMasterServerHandler _handler;
  private StormAMRMClient client;

  public MasterHeartbeatThread(StormMasterServerHandler _handler,
      StormAMRMClient client, BlockingQueue<Container> launcherQueue) {
    this._handler = _handler;
    this.client = client;
    LOG.info("Starting HB thread");
  }

  @Override
  public void run() {
    try {
      if (client.getServiceState() == Service.STATE.STARTED
          && !Thread.currentThread().isInterrupted()) {

        client.getContainerCalculator().schedule();
      }
    } catch (Throwable t) {
      // Something happened we could not handle. Make sure the AM goes
      // down so that we are not surprised later on that our heart
      // stopped..
      LOG.error("Unhandled error in AM: ", t);
      _handler.stop();
      System.exit(1);
    }
  }
}
