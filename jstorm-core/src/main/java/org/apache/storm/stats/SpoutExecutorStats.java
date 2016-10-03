/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.stats;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

@SuppressWarnings("unchecked")
public class SpoutExecutorStats extends CommonStats {

    public static final String ACKED = "acked";
    public static final String FAILED = "failed";
    public static final String COMPLETE_LATENCIES = "complete-latencies";

    public SpoutExecutorStats(int rate) {
        super(rate);
        this.put(ACKED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(FAILED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(COMPLETE_LATENCIES, new MultiLatencyStatAndMetric(NUM_STAT_BUCKETS));
    }

    public MultiCountStatAndMetric getAcked() {
        return (MultiCountStatAndMetric) this.get(ACKED);
    }

    public MultiCountStatAndMetric getFailed() {
        return (MultiCountStatAndMetric) this.get(FAILED);
    }

    public MultiLatencyStatAndMetric getCompleteLatencies() {
        return (MultiLatencyStatAndMetric) this.get(COMPLETE_LATENCIES);
    }

    public void spoutAckedTuple(String stream, Long latencyMs) {
      this.getAcked().incBy(stream, this.rate);
      // ack与complete-latencies拆分计算,此处只计算ack数,屏蔽complete-latencies累计内容
      // this.getCompleteLatencies().record(stream, latencyMs);
    }

    /**
     * Added by JStorm developer, Spout整体流程完成时间独立设置
     * 
     * @param stats
     * @param stream
     * @param latencyMs
     */
    public void spoutRecordCompleteLatencies(String stream, Long latencyMs) {
      this.getCompleteLatencies().record(stream, latencyMs);
    }

    /** added by JStorm developer : long -> Long */
    public void spoutFailedTuple(String stream, Long latencyMs) {
        this.getFailed().incBy(stream, this.rate);
    }

    public ExecutorStats renderStats() {
        cleanupStats();

        ExecutorStats ret = new ExecutorStats();
        // common fields
        ret.set_emitted(valueStat(EMITTED));
        ret.set_transferred(valueStat(TRANSFERRED));
        ret.set_rate(this.rate);

        // spout stats
        SpoutStats spoutStats = new SpoutStats(
                valueStat(ACKED), valueStat(FAILED), valueStat(COMPLETE_LATENCIES));
        ret.set_specific(ExecutorSpecificStats.spout(spoutStats));

        return ret;
    }
}
