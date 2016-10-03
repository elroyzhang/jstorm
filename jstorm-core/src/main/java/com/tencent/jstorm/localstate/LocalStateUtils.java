package com.tencent.jstorm.localstate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LSApprovedWorkers;
import org.apache.storm.generated.LSSupervisorAssignments;
import org.apache.storm.generated.LSSupervisorId;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.LSTopoHistoryList;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.utils.LocalState;

@ClojureClass(className = "backtype.storm.local-state")
public class LocalStateUtils {

  public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
  public static final String LS_ID = "supervisor-id";
  public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
  public static final String LS_APPROVED_WORKERS = "approved-workers";
  public static final String LS_TOPO_HISTORY = "topo-hist";

  @ClojureClass(className = "backtype.storm.local-state#ls-topo-hist!")
  public static void putLSTopoList(LocalState localState,
      List<LSTopoHistory> topo_history) {
    localState.put(LS_TOPO_HISTORY, new LSTopoHistoryList(topo_history));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-topo-hist")
  public static List<LSTopoHistory> getLSTopoList(LocalState localState) {
    LSTopoHistoryList lsTopoHistoryList =
        (LSTopoHistoryList) localState.get(LS_TOPO_HISTORY);
    if (lsTopoHistoryList != null) {
      return lsTopoHistoryList.get_topo_history();
    }
    return new ArrayList<LSTopoHistory>();
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-supervisor-id!")
  public static void putLSSupervisorid(LocalState localState, String id) {
    localState.put(LS_ID, new LSSupervisorId(id));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-supervisor-id")
  public static LSSupervisorId getLSSupervisorid(LocalState localState) {
    return (LSSupervisorId) localState.get(LocalStateUtils.LS_ID);
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-approved-workers!")
  public static void setLSApprovedWorkers(LocalState localState,
      Map<String, Integer> workers) {
    localState.put(LocalStateUtils.LS_APPROVED_WORKERS,
        new LSApprovedWorkers(workers));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-approved-workers")
  public static Map<String, Integer> getLSApprovedWorkers(
      LocalState localState) {
    Map<String, Integer> tmp = new HashMap<String, Integer>();
    LSApprovedWorkers tmpApprovedWorkers =
        (LSApprovedWorkers) localState.get(LocalStateUtils.LS_APPROVED_WORKERS);
    if (tmpApprovedWorkers != null) {
      tmp = tmpApprovedWorkers.get_approved_workers();
    }
    return tmp;
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-local-assignments!")
  public static void putLSLocalAssignments(LocalState localState,
      Map<Integer, LocalAssignment> localAssignmentMap) {
    localState.put(LS_LOCAL_ASSIGNMENTS,
        new LSSupervisorAssignments(localAssignmentMap));
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-local-assignments")
  public static Map<Integer, LocalAssignment> getLSLocalAssignments(
      LocalState localState) {
    LSSupervisorAssignments lsSupervisorAssignments =
        (LSSupervisorAssignments) localState
            .get(LocalStateUtils.LS_LOCAL_ASSIGNMENTS);
    if (lsSupervisorAssignments != null) {
      return lsSupervisorAssignments.get_assignments();
    }
    return new HashMap<Integer, LocalAssignment>();
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-worker-heartbeat!")
  public static void putLSWorkerHeartbeat(LocalState localState, int time_secs,
      String topology_id, List<ExecutorInfo> executors, int port,
      int process_id) {
    localState.put(LS_WORKER_HEARTBEAT, new LSWorkerHeartbeat(time_secs,
        topology_id, executors, port, process_id), false);
  }

  @ClojureClass(className = "backtype.storm.local-state#ls-worker-heartbeat")
  public static LSWorkerHeartbeat getLSWorkerHeartbeat(LocalState localState) {
    LSWorkerHeartbeat ret =
        (LSWorkerHeartbeat) localState.get(LocalStateUtils.LS_WORKER_HEARTBEAT);
    if (ret == null) {
      return new LSWorkerHeartbeat();
    }
    return ret;
  }
}
