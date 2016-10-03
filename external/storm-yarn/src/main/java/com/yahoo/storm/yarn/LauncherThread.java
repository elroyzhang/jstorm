package com.yahoo.storm.yarn;

import java.io.IOException;
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
public class LauncherThread extends RunnableCallback {
  private static final Logger LOG = LoggerFactory
      .getLogger(LauncherThread.class);
  private static final long serialVersionUID = 1L;

  private StormAMRMClient client;
  private BlockingQueue<Container> launcherQueue;
  private Container container;;

  public LauncherThread(StormAMRMClient client,
      BlockingQueue<Container> launcherQueue) {
    this.client = client;
    this.launcherQueue = launcherQueue;
    this.container = null;
    LOG.info("Starting launcher");
  }

  @Override
  public void run() {
    if (client.getServiceState() == Service.STATE.STARTED
        && !Thread.currentThread().isInterrupted()) {
      try {
        while (launcherQueue.size() > 0) {
          container = launcherQueue.take();
          LOG.info("LAUNCHER: Taking container with id (" + container.getId()
              + ") from the queue.");
          if (client.supervisorsAreToRun()) {
            LOG.info("LAUNCHER: Supervisors are to run, so launching container id ("
                + container.getId() + ")");
            client.launchSupervisorOnContainer(container);
          } else {
            // Do nothing
            LOG.info("LAUNCHER: Supervisors are not to run, so not launching container id ("
                + container.getId() + ")");
          }
        }
      } catch (InterruptedException e) {
        if (client.getServiceState() == Service.STATE.STARTED) {
          LOG.error("Launcher thread interrupted : ", e);
          System.exit(1);
        }
        return;
      } catch (IOException e) {
        LOG.error("Launcher thread I/O exception : ", e);
        System.exit(1);
      }
    }

  }

}
