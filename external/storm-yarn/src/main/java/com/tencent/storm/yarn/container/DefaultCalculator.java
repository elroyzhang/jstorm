package com.tencent.storm.yarn.container;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.Utils;

import com.yahoo.storm.yarn.Config;
import com.yahoo.storm.yarn.StormAMRMClient;
import com.yahoo.storm.yarn.Util;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class DefaultCalculator implements IContainerCalculator {
  private static final Logger LOG = LoggerFactory
      .getLogger(DefaultCalculator.class);
  private StormAMRMClient client;
  private BlockingQueue<Container> launcherQueue;
  private ContainerRequest containerRequest;

  private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(StormAMRMClient client, Map conf) {
    this.client = client;
    this.launcherQueue = new LinkedBlockingQueue<Container>();
    Integer pri = Utils.getInt(conf.get(Config.MASTER_CONTAINER_PRIORITY), 0);
    this.DEFAULT_PRIORITY.setPriority(pri);

    String supervisorChildOpts = (String) conf.get("supervisor.childopts");
    int supervisorHeapSize = Util.getHeapMegabytes(supervisorChildOpts);
    LOG.debug("SupervisorHeapSize " + supervisorHeapSize);

    String workerChildOpts = (String) conf.get("worker.childopts");
    int workerHeapSize = Util.getHeapMegabytes(workerChildOpts);
    LOG.debug("WorkerHeapSize is " + supervisorHeapSize);

    int nWorkersPerSupervisor =
        Utils.getInt(conf.get(Config.SUPERVISOR_NUM_WORKERS), 2);
    LOG.debug("NumberWorkersPerSupervisor is " + nWorkersPerSupervisor);

    int totalHeapSize =
        supervisorHeapSize + workerHeapSize * nWorkersPerSupervisor;
    LOG.debug("TotalHeapSize is " + totalHeapSize);
    Integer nVcorePerWorker = Util.getNumVcorePerWorker(conf);
    int totalNumVcore = nWorkersPerSupervisor * nVcorePerWorker.intValue();

    int numWorkers = Utils.getInt(conf.get(Config.SUPERVISOR_NUM_WORKERS), 1);
    int topologyWorkers = Utils.getInt(conf.get("topology.workers"), 1);
    int numSupervisors = (topologyWorkers + numWorkers - 1) / numWorkers;
    if (numSupervisors < 1) {
      numSupervisors = 1;
    }
    Resource resource = Resource.newInstance(totalHeapSize, totalNumVcore);
    containerRequest =
        new ContainerRequest(resource, null, null, DEFAULT_PRIORITY);
    for (int i = 0; i < numSupervisors; i++) {
      client.addContainerRequest(containerRequest);
    }
    LOG.info("Request " + numSupervisors
        + " new containers each with [vcore: {},memory: {}]", totalNumVcore,
        totalHeapSize);
  }

  @Override
  public void schedule() throws Exception {

    // We always send 50% progress.
    AllocateResponse allocResponse = client.allocate(0.5f);
    AMCommand am_command = allocResponse.getAMCommand();
    if (am_command != null
        && (am_command == AMCommand.AM_SHUTDOWN || am_command == AMCommand.AM_RESYNC)) {
      LOG.info("Got AM_SHUTDOWN or AM_RESYNC from the RM");
      throw new Exception("Got AM_SHUTDOWN or AM_RESYNC from the RM");
    }

    List<Container> allocatedContainers =
        allocResponse.getAllocatedContainers();
    if (allocatedContainers.size() > 0) {
      LOG.info("Received allocated {} containers", allocatedContainers.size());
      if (client.supervisorsAreToRun()) {
        for (Container container : allocatedContainers) {
          client.addAllocatedContainers(container);
          client.removeContainerRequest(containerRequest);
          launcherQueue.add(container);
        }
      } else {
        client.allocate(0.9f);
        client.stopAllSupervisors();
      }
    }

    List<ContainerStatus> completedContainers =
        allocResponse.getCompletedContainersStatuses();
    if (completedContainers != null && completedContainers.size() > 0
        && client.supervisorsAreToRun()) {
      for (ContainerStatus cs : completedContainers) {
        client.removeCompletedContainers(cs.getContainerId());
        client.addContainerRequest(containerRequest);
      }
      LOG.info("Containers completed {} ", completedContainers.size());
    }

    launchContainers();
  }

  private Container container;;

  private void launchContainers() {
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
