package com.yahoo.storm.yarn;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class NMCheckerThread extends RunnableCallback {
  private static final Logger LOG = LoggerFactory
      .getLogger(NMCheckerThread.class);

  private static final long serialVersionUID = 1L;

  private StormMasterServerHandler _handler;

  public NMCheckerThread(StormMasterServerHandler _handler) {
    this._handler = _handler;
    LOG.info("Starting NodeManager Checker");
    int nodeManagerPid = Util.getProcPid("nodemanager");
    LOG.info("Util.getProcPid pid = " + nodeManagerPid);
  }

  @Override
  public void run() {
    final String NADEMANAGER_PROC_NAME = "nodemanager";
    try {

      int nodeManagerPid = Util.getProcPid(NADEMANAGER_PROC_NAME);
      int sleepInterval = 1000 * 60;

      if (!Thread.currentThread().isInterrupted()) {
        if (!Util.checkProcExist(NADEMANAGER_PROC_NAME, nodeManagerPid)) {
          // double check
          Thread.sleep(sleepInterval);
          if (!Util.checkProcExist(NADEMANAGER_PROC_NAME, nodeManagerPid)) {
            throw new IOException("NodeManager pid: " + nodeManagerPid
                + "Not Exist!");
          }

        }
      }
    } catch (Throwable t) {
      // Something happened we could not handle. Make sure the AM goes
      // down so that we are not surprised later on that our heart
      // stopped..
      LOG.error("NMChecker error in AM: ", t);
      _handler.stopMaster();
      System.exit(1);
    }
  }
}
