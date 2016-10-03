package com.tencent.jstorm.daemon.executor.spout;

import java.util.Map;

import org.apache.storm.daemon.Acker;
import org.apache.storm.stats.SpoutExecutorStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorEventHandler;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.storm.topology.UpdateRichSpout;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.spout.ISpout;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.RotatingMap;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout#event-handler")
public class SpoutEventHandler extends ExecutorEventHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpoutEventHandler.class);
  private RotatingMap<Long, TupleInfo> pending;

  public SpoutEventHandler(ExecutorData executorData,
      RotatingMap<Long, TupleInfo> pending) {
    super(executorData);
    this.pending = pending;
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout#tuple-action-fn")
  public void tupleActionFn(Integer taskId, TupleImpl tuple) {
    String streamId = tuple.getSourceStreamId();
    if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      pending.rotate();
    } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
      ExecutorUtils.metricsTick(executorData,
          executorData.getTaskDatas().get(taskId), tuple);
    } else if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
      TaskData taskData = executorData.getTaskDatas().get(taskId);
      ISpout spoutObject = (ISpout) taskData.getObject();
      if (spoutObject instanceof ICredentialsListener) {
        ((ICredentialsListener) spoutObject)
            .setCredentials((Map<String, String>) tuple.getValue(0));
      }
    } else if (streamId.equals(Acker.ACKER_RESET_TIMEOUT_STREAM_ID)) {
      Long id = tuple.getLong(0);
      TupleInfo pendingForId = pending.get(id);
      if (pendingForId != null) {
        pending.put(id, pendingForId);
      }
    } else if (streamId.equals(Acker.ACKER_UPDATE_STREAM_ID)) {
      UpdateRichSpout spout =
          (UpdateRichSpout) executorData.getTaskDatas().get(taskId).getObject();
      spout.update(tuple.getValue(0));
    } else {
      Long id = tuple.getLong(0);
      Object obj = pending.remove(id);
      if (obj == null) {
        return;
      }
      TupleInfo tupleInfo = (TupleInfo) obj;
      Object spoutId = tupleInfo.getMessageId();
      Integer storedTaskId = tupleInfo.getTaskId();

      if (spoutId != null) {
        if (storedTaskId != taskId) {
          throw new RuntimeException("Fatal error, mismatched task ids: "
              + taskId + " " + storedTaskId);
        }

        Long startTimeMs = tupleInfo.getTimestamp();
        Long timeDelta = null;
        if (startTimeMs != null) {
          timeDelta = CoreUtil.time_delta_ms(startTimeMs);
          ((SpoutExecutorStats) executorData.getStats())
              .spoutRecordCompleteLatencies(tupleInfo.getStream(), timeDelta);
        }
        if (streamId.equals(Acker.ACKER_ACK_STREAM_ID)) {
          AckSpoutMsg asm = new AckSpoutMsg(executorData,
              executorData.getTaskDatas().get(taskId), spoutId, tupleInfo,
              timeDelta, id);
          asm.run();
        } else if (streamId.equals(Acker.ACKER_FAIL_STREAM_ID)) {
          FailSpoutMsg fsm = new FailSpoutMsg(executorData,
              executorData.getTaskDatas().get(taskId), spoutId, tupleInfo,
              timeDelta, "FAIL-STREAM", id);
          fsm.run();
        } else {
          // TODO: on failure, emit tuple to failure stream
          LOG.warn("streamId : {} , not legal", streamId);
        }
      } else {
        LOG.warn("spoutId is null");
      }
    }
  }
}
