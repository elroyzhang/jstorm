package com.tencent.jstorm.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerUtils;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class AbsoluteScheduler implements IScheduler {

  private final static Logger LOG =
      LoggerFactory.getLogger(AbsoluteScheduler.class);

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf) {
  }

  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    List<TopologyDetails> topologyDetails =
        new ArrayList<TopologyDetails>(topologies.getTopologies());
    Collections.sort(topologyDetails, new Comparator<TopologyDetails>() {

      @Override
      public int compare(TopologyDetails o1, TopologyDetails o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    for (TopologyDetails topology : topologyDetails) {
      SchedulerContext context = new SchedulerContext(topology, cluster);
      if (context.isNeedesReassigned()) {
        if (!context.isResourced()) {
          Object[] msg = { topology.getId(), context.getUsedWorkers(),
              context.getWorkerNums() };
          LOG.error(
              "Assign topology:{}.Resource isn't enough, used worker number is {} and needed worker number is {}",
              msg);
        } else {
          Map<ExecutorDetails, WorkerSlot> assignment =
              assignTask(context, cluster);

          // node+port->executors
          Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors =
              Utils.reverseMap(assignment);

          for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors
              .entrySet()) {
            WorkerSlot nodePort = entry.getKey();
            List<ExecutorDetails> executors = entry.getValue();
            cluster.assign(nodePort, topology.getId(), executors);
          }
        }
      }
    }
  }

  private Map<ExecutorDetails, WorkerSlot> assignTask(SchedulerContext context,
      Cluster cluster) {
    List<WorkerSlot> assignedSlots =
        sortSlotsByUtilizationrate(context.getAvailableSlot(), cluster);
    Map<ExecutorDetails, WorkerSlot> assignment =
        new HashMap<ExecutorDetails, WorkerSlot>();
    LOG.info("{} allSlots:{}",
        new Object[] { context.getTopologyId(), assignedSlots.toString() });
    int workerSize = assignedSlots.size();
    int neededWorkers = context.getWorkerNums() - context.getUsedWorkers();
    Set<ExecutorDetails> neededAssignedExecutors =
        context.getNeededAssignedExecutors();

    if (workerSize >= neededWorkers) {
      LOG.info("{} assignedSlots:{}", new Object[] { context.getTopologyId(),
          assignedSlots.subList(0, neededWorkers).toString() });
      assignment = SchedulerUtils.assignExecutor2Worker(neededAssignedExecutors,
          assignedSlots.subList(0, neededWorkers));
    } else {
      // worker number isn't enough
      int missingWorkers = neededWorkers - workerSize;
      int executorNum = 0;
      Map<Integer, Integer> distribution = context.getExecutorDistribution();
      TreeMap<Integer, Integer> sortedDis =
          new TreeMap<Integer, Integer>(new Comparator<Integer>() {

            @Override
            public int compare(Integer o1, Integer o2) {
              return o2 - o1;
            }
          });
      sortedDis.putAll(distribution);
      for (Map.Entry<Integer, Integer> entry : sortedDis.entrySet()) {
        if (workerSize == 0) {
          break;
        }
        int tmpWorkers = entry.getValue();
        if (workerSize > tmpWorkers) {
          executorNum += entry.getKey() * tmpWorkers;
          workerSize = workerSize - tmpWorkers;
        } else {
          executorNum += workerSize * entry.getKey();
          workerSize = 0;
        }
      }

      List<ExecutorDetails> sortedAssDetails =
          new ArrayList<ExecutorDetails>(neededAssignedExecutors);
      Collections.sort(sortedAssDetails, new Comparator<ExecutorDetails>() {

        @Override
        public int compare(ExecutorDetails o1, ExecutorDetails o2) {
          return o1.getStartTask() - o2.getStartTask();
        }
      });
      Set<ExecutorDetails> reassigned = new HashSet<ExecutorDetails>();
      Set<ExecutorDetails> reminded = new HashSet<ExecutorDetails>();
      int i = 0;
      for (ExecutorDetails e : sortedAssDetails) {
        if (i++ < executorNum) {
          reassigned.add(e);
        } else {
          reminded.add(e);
        }
      }
      assignment =
          SchedulerUtils.assignExecutor2Worker(reassigned, assignedSlots);
      Object[] msg = { context.getTopologyId(), missingWorkers, reminded };
      LOG.error(
          "Assign topology:{}.The worker number of missing is {} and not assigned tasks:{}",
          msg);
    }
    return assignment;

  }

  public List<WorkerSlot> sortSlotsByUtilizationrate(
      List<WorkerSlot> availableSlots, Cluster cluster) {
    if (null != availableSlots && availableSlots.size() > 0) {
      // group by node id
      Map<String, List<WorkerSlot>> slotGroups =
          new TreeMap<String, List<WorkerSlot>>();
      for (WorkerSlot slot : availableSlots) {
        String sHost = cluster.getHost(slot.getNodeId());
        List<WorkerSlot> slotList = slotGroups.get(sHost);
        if (null == slotList) {
          slotList = new ArrayList<WorkerSlot>();
          slotGroups.put(sHost, slotList);
        }
        slotList.add(slot);
      }
      // sort list by port
      for (List<WorkerSlot> slotList : slotGroups.values()) {
        Collections.sort(slotList, new Comparator<WorkerSlot>() {
          @Override
          public int compare(WorkerSlot o1, WorkerSlot o2) {
            return o1.getPort() - o2.getPort();
          }
        });
      }
      // sort by count
      List<List<WorkerSlot>> tmpList =
          new ArrayList<List<WorkerSlot>>(slotGroups.values());
      Collections.sort(tmpList, new Comparator<List<WorkerSlot>>() {
        @Override
        public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
          double allSlot1 =
              Double
                  .valueOf(cluster
                      .getAssignablePorts(
                          cluster.getSupervisorById(o1.get(0).getNodeId()))
                      .size());
          double allSlot2 =
              Double
                  .valueOf(cluster
                      .getAssignablePorts(
                          cluster.getSupervisorById(o2.get(0).getNodeId()))
                      .size());
          double rate1 = Double.valueOf(o1.size()) / allSlot1;
          double rate2 = Double.valueOf(o2.size()) / allSlot2;
          if (rate2 > rate1) {
            return 1;
          } else if (rate2 < rate1) {
            return -1;
          } else {
            return o2.size() - o1.size();
          }
        }
      });

      return CoreUtil.interleaveAll(tmpList);
    }
    return null;
  }

  class SchedulerContext {
    private String topologyId;
    private int workerNums = 0;
    private int usedWorkers = 0;
    private Set<ExecutorDetails> neededAssignedExecutors;
    private List<WorkerSlot> availableSlot;
    private Map<Integer, Integer> executorDistribution;

    public SchedulerContext(TopologyDetails topology, Cluster cluster) {
      initContext(topology, cluster);
    }

    public void initContext(TopologyDetails topology, Cluster cluster) {
      topologyId = topology.getId();
      workerNums = topology.getNumWorkers();
      availableSlot = cluster.getAvailableSlots();
      Set<ExecutorDetails> topologyExecutors =
          (Set<ExecutorDetails>) topology.getExecutors();
      executorDistribution =
          Utils.integerDivided(topologyExecutors.size(), workerNums);

      SchedulerAssignment existingAssignment =
          cluster.getAssignmentById(topologyId);
      Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
      if (null != existingAssignment) {
        aliveExecutors.addAll(existingAssignment.getExecutors());
        usedWorkers = existingAssignment.getSlots().size();

        Map<ExecutorDetails, WorkerSlot> existingAssigned =
            existingAssignment.getExecutorToSlot();
        Map<WorkerSlot, List<ExecutorDetails>> workerExecutors =
            Utils.reverseMap(existingAssigned);

        for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : workerExecutors
            .entrySet()) {
          int executorNum = entry.getValue().size();
          if (executorDistribution.containsKey(executorNum)) {
            int num = executorDistribution.get(executorNum) - 1;
            if (num > 0) {
              executorDistribution.put(executorNum, num);
            } else {
              executorDistribution.remove(executorNum);
            }
          }
        }
      }
      neededAssignedExecutors =
          CoreUtil.set_difference(topologyExecutors, aliveExecutors);

    }

    public String getTopologyId() {
      return topologyId;
    }

    public void setTopologyId(String topologyId) {
      this.topologyId = topologyId;
    }

    public int getWorkerNums() {
      return workerNums;
    }

    public void setWorkerNums(int workerNums) {
      this.workerNums = workerNums;
    }

    public int getUsedWorkers() {
      return usedWorkers;
    }

    public void setUsedWorkers(int usedWorkers) {
      this.usedWorkers = usedWorkers;
    }

    public Set<ExecutorDetails> getNeededAssignedExecutors() {
      return neededAssignedExecutors;
    }

    public void setNeededAssignedExecutors(
        Set<ExecutorDetails> neededAssignedExecutors) {
      this.neededAssignedExecutors = neededAssignedExecutors;
    }

    public Map<Integer, Integer> getExecutorDistribution() {
      return executorDistribution;
    }

    public void setExecutorDistribution(
        Map<Integer, Integer> executorDistribution) {
      this.executorDistribution = executorDistribution;
    }

    public List<WorkerSlot> getAvailableSlot() {
      return availableSlot;
    }

    public void setAvailableSlot(List<WorkerSlot> availableSlot) {
      this.availableSlot = availableSlot;
    }

    public boolean isResourced() {
      return null != availableSlot && availableSlot.size() > 0;
    }

    public boolean isNeedesReassigned() {
      return null != neededAssignedExecutors
          && neededAssignedExecutors.size() > 0;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this,
          ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
