package com.tencent.storm.yarn.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
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
public class CapabilityCalculator implements IContainerCalculator {
  private static final Logger LOG = LoggerFactory
      .getLogger(CapabilityCalculator.class);
  private StormAMRMClient client;
  private BlockingQueue<Container> launcherQueue;
  private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);
  private int nVcorePerWorker;
  private AtomicInteger runningWorkersNum;
  private int topologyWorkers;
  private int supervisorHeapSize;
  private int workerHeapSize;
  private int suggestWorkersPerContainer;
  private List<Integer> partitions;
  private AtomicInteger cnt;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(StormAMRMClient client, Map conf) {
    this.client = client;
    this.launcherQueue = new LinkedBlockingQueue<Container>();
    Integer pri = Utils.getInt(conf.get(Config.MASTER_CONTAINER_PRIORITY), 0);
    this.DEFAULT_PRIORITY.setPriority(pri);

    this.topologyWorkers =
        Utils.getInt(conf.get(backtype.storm.Config.TOPOLOGY_WORKERS), 1);
    runningWorkersNum = new AtomicInteger(0);

    String supervisorChildOpts =
        (String) conf.get(backtype.storm.Config.SUPERVISOR_CHILDOPTS);
    supervisorHeapSize = Util.getHeapMegabytes(supervisorChildOpts);
    LOG.debug("SupervisorHeapSize " + supervisorHeapSize);

    String workerChildOpts =
        (String) conf.get(backtype.storm.Config.WORKER_CHILDOPTS);
    int workerHeapSize = Util.getHeapMegabytes(workerChildOpts);
    LOG.debug("WorkerHeapSize is " + supervisorHeapSize);

    this.nVcorePerWorker = Util.getNumVcorePerWorker(conf);

    Resource res = client.getAvailableResources();
    int workers = (res.getMemory() - supervisorHeapSize) / workerHeapSize;
    int workers2 = res.getVirtualCores() / nVcorePerWorker;
    this.suggestWorkersPerContainer = workers < workers2 ? workers : workers2;
    int numContainers = topologyWorkers / suggestWorkersPerContainer + 1;
    this.partitions = partition(topologyWorkers, numContainers);

  }

  private ContainerRequest calcContainerRequest(int suggestWorkersPerContainer) {
    int totalHeapSize =
        supervisorHeapSize + workerHeapSize * suggestWorkersPerContainer;
    int totalNumVcore = suggestWorkersPerContainer * nVcorePerWorker;
    Resource resource = Resource.newInstance(totalHeapSize, totalNumVcore);
    return new ContainerRequest(resource, null, null, DEFAULT_PRIORITY);
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

    if (runningWorkersNum.get() < topologyWorkers) {

      for (Integer workers : partitions) {
        ContainerRequest containerRequest = calcContainerRequest(workers);
        client.addContainerRequest(containerRequest);
      }

      if (cnt.incrementAndGet() > 3 & partitions.size() > 0) {
        List<Integer> newPartitions = new ArrayList<Integer>();
        for (Integer workers : partitions) {
          newPartitions.addAll(partition(workers, 2));
        }
        partitions = newPartitions;
      } else {
        cnt.getAndSet(0);
      }
    }

    List<Container> allocatedContainers =
        allocResponse.getAllocatedContainers();
    if (allocatedContainers.size() > 0) {
      LOG.info("Received allocated {} containers", allocatedContainers.size());
      if (client.supervisorsAreToRun()) {
        for (Container container : allocatedContainers) {
          int allocatedWorkers =
              container.getResource().getVirtualCores() / nVcorePerWorker;
          partitions.remove(allocatedWorkers);
          runningWorkersNum.addAndGet(allocatedWorkers);
          client.addAllocatedContainers(container);
          ContainerRequest containerRequest =
              calcContainerRequest(allocatedWorkers);
          client.removeContainerRequest(containerRequest);
          launcherQueue.add(container);
        }
      } else {
        client.stopAllSupervisors();
      }
    } else {

    }

  }

  private static List<Integer> partition(int totalWorkers, int numContainers) {
    List<Integer> result = new ArrayList<Integer>(numContainers);
    int baseSize = totalWorkers / numContainers;
    int remainder = totalWorkers % numContainers;
    for (int i = 0; i < numContainers; i++) {
      int count = baseSize;
      if (remainder > 0) {
        count += 1;
        remainder -= 1;
      }
      result.add(count);
    }
    return result;
  }
}
