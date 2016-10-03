struct WorkerSummary {
  1: required i32 port;
  2: required string topology;
  3: required list<ExecutorSummary> tasks;
}

struct SupervisorWorkers {
  1: required SupervisorSummary supervisor;
  2: required list<WorkerSummary> workers;
}