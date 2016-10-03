package com.tencent.jstorm.daemon.executor.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorEventHandler;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#event-handler")
public class BoltEventHandler extends ExecutorEventHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(BoltEventHandler.class);

  private Callable<Boolean> executeSampler;
  private Map<String, Callable<Boolean>> compToExcuteAckSamplers =
      new HashMap<String, Callable<Boolean>>();

  public BoltEventHandler(ExecutorData executorData) {
    super(executorData);
    this.executeSampler =
        ConfigUtils.mkStatsSampler(executorData.getStormConf());
    Map<Integer, String> taskToComp = executorData.getTaskToComponent();
    Set<String> compIds = new HashSet<String>(taskToComp.values());
    for (String compId : compIds) {
      compToExcuteAckSamplers.put(compId,
          ConfigUtils.mkStatsSampler(executorData.getStormConf()));
    }
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#tuple-action-fn")
  public void tupleActionFn(Integer taskId, TupleImpl tuple) {
    String streamId = tuple.getSourceStreamId();
    if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
      TaskData taskData = executorData.getTaskDatas().get(taskId);
      IBolt boltObj = (IBolt) taskData.getObject();
      if (boltObj instanceof ICredentialsListener) {
        ((ICredentialsListener) boltObj)
            .setCredentials((Map<String, String>) tuple.getValue(0));
      }
    } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
      ExecutorUtils.metricsTick(executorData,
          executorData.getTaskDatas().get(taskId), tuple);
    } else {
      TaskData taskData = executorData.getTaskDatas().get(taskId);
      IBolt boltObj = (IBolt) taskData.getObject();

      Long now = null;
      boolean isLatenciesSampler = false;
      try {
        isLatenciesSampler = executorData.getLatenciesSampler().call();
      } catch (Exception e) {
        LOG.error("LatenciesSampler call error {}", CoreUtil.stringifyError(e));
      }
      boolean isExecuteSampler = false;
      try {
        // 由于AckerBolt的输入来源ack_ack/ack_init/ack_fail于不同的component，如果ack_ack/ack_init/ack_fail
        // 公用取样器会导致各自数据不准确，数据多了之后，虽然三者相加的数据是一致的，但各自的误差较大，容易造成混淆
        // 比如20取样1次，在前十次中有ack_ack/ack_init，第11次的取样成功而streamId刚好为ack_fail，那么就会统计到ack_fail上去，三者之间会互相干扰。
        if (Utils.isSystemId(streamId) && compToExcuteAckSamplers
            .get(tuple.getSourceComponent()) != null) {
          isExecuteSampler =
              compToExcuteAckSamplers.get(tuple.getSourceComponent()).call();
        } else {
          isExecuteSampler = executeSampler.call();
        }
      } catch (Exception e) {
        LOG.error("ExecuteSampler call error {}", CoreUtil.stringifyError(e));
      }
      if (isLatenciesSampler || isExecuteSampler) {
        now = System.currentTimeMillis();
      }

      if (isLatenciesSampler) {
        tuple.setProcessSampleStartTime(now);
      }

      if (isExecuteSampler) {
        tuple.setExecuteSampleStartTime(now);
      }

      boltObj.execute(tuple);

      Long delta = BoltUtils.tupleExecuteTimeDelta(tuple);
      if (executorData.getDebug()) {
        LOG.info("Execute done TUPLE " + tuple.getValues().toString()
            + " TASK: " + taskId + " DELTA: " + delta);
      }

      TaskUtils.applyHooks(taskData.getUserContext(),
          new BoltExecuteInfo(tuple, taskId, delta));

      if (delta != null) {
        ((BoltExecutorStats) executorData.getStats()).boltExecuteTuple(
            tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
      }
    }
  }

}
