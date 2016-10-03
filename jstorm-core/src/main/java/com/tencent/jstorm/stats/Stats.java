package com.tencent.jstorm.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.storm.cluster.ExecutorBeat;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;
import org.codehaus.jackson.JsonGenerator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorCache;
import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.CoreUtil;

@SuppressWarnings({ "unchecked" })
public class Stats {

  @ClojureClass(className = "org.apache.storm.stats##TEN-MIN-IN-SECONDS")
  public static final Integer TEN_MIN_IN_SECONDS = 600;

  /**
   * Returns true if x is a number that is not NaN or Infinity, false otherwise.
   *
   * @param x
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#valid-number")
  public static boolean validNumber(Object x) {
    if (x != null) {
      String xStr = String.valueOf(x);
      return NumberUtils.isNumber(xStr) && !Double.isNaN(Double.valueOf(xStr))
          && !Double.isInfinite(Double.valueOf(xStr));
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.stats#apply-or-0")
  public static double applyOr0(Object x) {
    double val = 0;
    if (validNumber(x)) {
      val = Double.valueOf(String.valueOf(x));
    }
    return val;
  }

  @ClojureClass(className = "org.apache.storm.stats#sum-or-0")
  public static double sumOr0(Object... args) {
    double sum = 0;
    for (Object x : args) {
      sum += applyOr0(x);
    }
    return sum;
  }

  @ClojureClass(className = "org.apache.storm.stats#sum-or-0")
  public static double sumOr0(Set<Object> args) {
    double sum = 0;
    for (Object x : args) {
      sum += applyOr0(x);
    }
    return sum;
  }

  /**
   * multiple args.
   *
   * @param args
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#product-or-0")
  public static double productOr0(Set<Object> args) {
    double produce = 1;
    for (Object x : args) {
      produce *= applyOr0(x);
    }
    return produce;
  }

  @ClojureClass(className = "org.apache.storm.stats#max-or-0")
  public static double maxOr0(Set<Object> args) {
    double max = 0;
    for (Object x : args) {
      max = Math.max(max, applyOr0(x));
    }
    return max;
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-bolt-lat-and-count#weight-avg")
  private static double weightAvg(Map<?, ?> numMap, Object id, Object avg) {
    Set<Object> set = new HashSet<Object>();
    set.add(numMap.get(id));
    set.add(avg);
    return productOr0(set);
  }

  /**
   * Aggregates number executed, process latency, and execute latency across all
   * streams.
   *
   * @param execAvg
   * @param procAvg
   * @param numExecuted
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-bolt-lat-and-count")
  private static Map<StatsFields, Double> aggBoltLatAndCount(
      Map<GlobalStreamId, Double> execAvg, Map<GlobalStreamId, Double> procAvg,
      Map<GlobalStreamId, Long> numExecuted) {
    Map<StatsFields, Double> result = new HashMap<StatsFields, Double>();
    Double esum = new Double(0);
    for (Entry<GlobalStreamId, Double> e : execAvg.entrySet()) {
      esum += weightAvg(numExecuted, e.getKey(), e.getValue());
    }
    result.put(StatsFields.execute_latencies_total, esum);
    Double psum = new Double(0);
    for (Entry<GlobalStreamId, Double> e : procAvg.entrySet()) {
      psum += weightAvg(numExecuted, e.getKey(), e.getValue());
    }
    result.put(StatsFields.process_latencies_total, psum);
    Double vsum = new Double(0);
    for (Entry<GlobalStreamId, Long> e : numExecuted.entrySet()) {
      vsum += e.getValue();
    }
    result.put(StatsFields.executed, vsum);
    return result;
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-bolt-streams-lat-and-count")
  private static Map<Object, Map<StatsFields, Double>> aggBoltStreamsLatAndCount(
      Map<Object, Object> execAvg, Map<Object, Object> procAvg,
      Map<Object, Long> executed) {
    Map<Object, Map<StatsFields, Double>> result =
        new HashMap<Object, Map<StatsFields, Double>>();
    for (Entry<Object, Object> e : execAvg.entrySet()) {
      Map<StatsFields, Double> map = new HashMap<StatsFields, Double>();
      map.put(StatsFields.execute_latencies_total,
          weightAvg(executed, e.getKey(), e.getValue()));
      map.put(StatsFields.process_latencies_total,
          weightAvg(executed, e.getKey(), procAvg.get(e.getKey())));
      map.put(StatsFields.executed, Double.valueOf(executed.get(e.getKey())));

      result.put(e.getKey(), map);
    }
    return result;
  }

  /**
   * Aggregates number acked and complete latencies.
   *
   * @param compAvg
   * @param acked
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-spout-streams-lat-and-count")
  private static Map<Object, Map<StatsFields, Double>> aggSpoutStreamsLatAndCount(
      Map<Object, Object> compAvg, Map<Object, Long> acked) {
    Map<Object, Map<StatsFields, Double>> result =
        new HashMap<Object, Map<StatsFields, Double>>();
    for (Entry<Object, Object> e : compAvg.entrySet()) {
      Map<StatsFields, Double> map = new HashMap<StatsFields, Double>();
      map.put(StatsFields.complete_latencies_total,
          weightAvg(acked, e.getKey(), e.getValue()));
      map.put(StatsFields.acked, Double.valueOf(acked.get(e.getKey())));

      result.put(e.getKey(), map);
    }
    return result;
  }

  /**
   * Aggregates number acked and complete latencies across all streams.
   *
   * @param execAvg
   * @param procAvg
   * @param numExecuted
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-spout-lat-and-count")
  private static Map<StatsFields, Double> aggSpoutLatAndCount(
      Map<String, Double> compAvg, Map<String, Long> numAcked) {
    Map<StatsFields, Double> result = new HashMap<StatsFields, Double>();
    Double csum = new Double(0);
    for (Entry<String, Double> e : compAvg.entrySet()) {
      csum += weightAvg(numAcked, e.getKey(), e.getValue());
    }
    result.put(StatsFields.complete_latencies_total, csum);
    Double asum = new Double(0);
    for (Entry<String, Long> e : numAcked.entrySet()) {
      asum += e.getValue();
    }
    result.put(StatsFields.acked, asum);
    return result;
  }

  /**
   * expandAverages
   * 
   * @param avg
   * @param counts
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#expand-averages")
  public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAverages(
      Map<K1, Map<K2, M1>> avg, Map<K1, Map<K2, M2>> counts) {
    Map<K1, Map<K2, Pair<Object, Object>>> ret =
        new HashMap<K1, Map<K2, Pair<Object, Object>>>();
    for (Map.Entry<K1, Map<K2, M2>> entry : counts.entrySet()) {
      K1 slice = entry.getKey();
      if (avg.get(slice) == null) {
        continue;
      }
      Map<K2, M2> streams = entry.getValue();
      Map<K2, Pair<Object, Object>> tmpValue =
          new HashMap<K2, Pair<Object, Object>>();
      for (Map.Entry<K2, M2> se : streams.entrySet()) {
        M2 count = se.getValue();
        K2 stream = se.getKey();
        tmpValue.put(se.getKey(), new Pair<Object, Object>(
            CoreUtil.multiply(count, avg.get(slice).get(stream)), count));
      }
      ret.put(slice, tmpValue);
    }

    return ret;
  }

  /**
   * addPairs
   * 
   * @param pairs1
   * @param pairs2
   * @return Pair
   */
  @ClojureClass(className = "org.apache.storm.stats#add-pairs")
  public static Pair<Object, Object> addPairs(Pair<Object, Object> pairs1,
      Pair<Object, Object> pairs2) {

    if (null == pairs1 && null == pairs2) {
      return new Pair<Object, Object>(0, 0);
    } else if (null == pairs1) {
      return pairs2;
    } else if (null == pairs2) {
      return pairs1;
    } else {
      return new Pair<Object, Object>(
          CoreUtil.add(pairs1.getFirst(), pairs2.getFirst()),
          CoreUtil.add(pairs1.getSecond(), pairs2.getSecond()));
    }
  }

  @ClojureClass(className = "org.apache.storm.stats#mk-include-sys-fn")
  public static boolean mkIncludeSysFn(boolean includeSys, Object stream) {
    return includeSys
        || (stream instanceof String && !Utils.isSystemId((String) stream));
  }

  @ClojureClass(className = "org.apache.storm.stats#mk-include-sys-filter")
  public static void mkIncludeSysFilter(Map<?, ?> map, boolean includeSys,
      Object stream) {
    for (Entry<?, ?> e : map.entrySet()) {
      if (!mkIncludeSysFn(false, e.getKey())) {
        map.remove(e);
      }
    }
  }

  /**
   * expandAveragesSeq
   * 
   * @param averageSeq
   * @param countsSeq
   * @return Map
   */
  @ClojureClass(className = "org.apache.storm.stats#expand-averages-seq")
  public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAveragesSeq(
      List<Map<K1, Map<K2, M1>>> averageSeq,
      List<Map<K1, Map<K2, M2>>> countsSeq) {

    Iterator<Map<K1, Map<K2, M1>>> avgItr = averageSeq.iterator();
    Iterator<Map<K1, Map<K2, M2>>> couItr = countsSeq.iterator();

    Map<K1, Map<K2, Pair<Object, Object>>> ret =
        new HashMap<K1, Map<K2, Pair<Object, Object>>>();

    while (avgItr.hasNext() && couItr.hasNext()) {
      Map<K1, Map<K2, Pair<Object, Object>>> tmp =
          expandAverages(avgItr.next(), couItr.next());

      for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : tmp
          .entrySet()) {
        K1 key = entry.getKey();
        Map<K2, Pair<Object, Object>> value = entry.getValue();
        if (ret.containsKey(key)) {
          Map<K2, Pair<Object, Object>> original = ret.get(key);
          for (Map.Entry<K2, Pair<Object, Object>> v : value.entrySet()) {
            K2 vk = v.getKey();
            Pair<Object, Object> vv = v.getValue();
            Pair<Object, Object> tmpPair =
                original.containsKey(vk) ? addPairs(original.get(vk), vv) : vv;
            original.put(vk, tmpPair);
          }
        } else {
          ret.put(key, value);
        }
      }
    }
    return ret;
  }

  /**
   * valAvg
   * 
   * @param pair
   * @return valavg
   */
  @ClojureClass(className = "org.apache.storm.stats#val-avg")
  public static Object valAvg(Pair<Object, Object> pair) {
    if (pair == null) {
      return 0;
    }
    if (pair.getSecond() == null
        || "0".equals(String.valueOf(pair.getSecond()))) {
      return pair.getFirst();
    }
    return CoreUtil.divide(pair.getFirst(), pair.getSecond());
  }

  /**
   * aggregateAverages
   * 
   * @param averageSeq
   * @param countsSeq
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-averages")
  public static <K1, K2, M1, M2> Map<Object, Map<Object, Object>> aggregateAverages(
      List<Map<K1, Map<K2, M1>>> averageSeq,
      List<Map<K1, Map<K2, M2>>> countsSeq) {
    Map<Object, Map<Object, Object>> ret =
        new HashMap<Object, Map<Object, Object>>();
    Map<K1, Map<K2, Pair<Object, Object>>> expands =
        expandAveragesSeq(averageSeq, countsSeq);
    for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : expands
        .entrySet()) {
      Map<K2, Pair<Object, Object>> value = entry.getValue();
      Map<Object, Object> tmpAvgs = new HashMap<Object, Object>();
      for (Map.Entry<K2, Pair<Object, Object>> tmp : value.entrySet()) {
        tmpAvgs.put(tmp.getKey(), valAvg(tmp.getValue()));
      }
      ret.put(entry.getKey(), tmpAvgs);
    }
    return ret;
  }

  /**
   * mkIncludeSysFn
   * 
   * @param includeSys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#mk-include-sys-fn")
  public static IncludeSysFn mkIncludeSysFn(boolean includeSys) {
    return new IncludeSysFn(includeSys);
  }

  /**
   * preProcess
   * 
   * @param streamSummary
   * @param includeSys
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#pre-process")
  public static void preProcess(
      Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary,
      boolean includeSys) {
    IncludeSysFn filterFn = mkIncludeSysFn(includeSys);
    Map<Object, Map<Object, Object>> emitted =
        streamSummary.get(StatsFields.emitted);
    for (Map.Entry<Object, Map<Object, Object>> entry : emitted.entrySet()) {
      Object window = entry.getKey();
      Map<Object, Object> stat = entry.getValue();
      emitted.put(window, CoreUtil.filterKey(filterFn, stat));
    }
    Map<Object, Map<Object, Object>> transferred =
        streamSummary.get(StatsFields.transferred);
    for (Map.Entry<Object, Map<Object, Object>> entry : transferred
        .entrySet()) {
      Object window = entry.getKey();
      Map<Object, Object> stat = entry.getValue();
      transferred.put(window, CoreUtil.filterKey(filterFn, stat));
    }
  }

  /**
   * aggregateCounts
   * 
   * @param countsSeq
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-counts")
  public static <K, V> Map<Object, Map<Object, Object>> aggregateCounts(
      List<Map<String, Map<K, V>>> countsSeq) {
    Map<Object, Map<Object, Object>> result =
        new HashMap<Object, Map<Object, Object>>();
    for (Map<String, Map<K, V>> listEntry : countsSeq) {
      if (listEntry == null) {
        continue;
      }
      for (Map.Entry<String, Map<K, V>> me : listEntry.entrySet()) {
        String key = me.getKey();
        Map<K, V> value = me.getValue();
        Map<Object, Object> valueObj = new HashMap<Object, Object>();
        for (Map.Entry<K, V> entry : value.entrySet()) {
          valueObj.put(entry.getKey(), entry.getValue());
        }
        if (result.containsKey(key)) {
          Map<Object, Object> tmp = result.get(key);
          result.put(key, CoreUtil.mergeWith(tmp, valueObj));
        } else {
          result.put(key, valueObj);
        }
      }
    }
    return result;

  }

  /**
   * aggregateCommonStats
   * 
   * @param statsSeq
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-common-stats")
  public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateCommonStats(
      List<ExecutorStats> statsSeq) {

    List<Map<String, Map<String, Long>>> emittedList =
        new ArrayList<Map<String, Map<String, Long>>>();
    List<Map<String, Map<String, Long>>> transferrdList =
        new ArrayList<Map<String, Map<String, Long>>>();
    for (ExecutorStats e : statsSeq) {
      emittedList.add(e.get_emitted());
      transferrdList.add(e.get_transferred());
    }
    Map<StatsFields, Map<Object, Map<Object, Object>>> ret =
        new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();

    ret.put(StatsFields.emitted, aggregateCounts(emittedList));
    ret.put(StatsFields.transferred, aggregateCounts(transferrdList));
    return ret;

  }

  /**
   * aggregateSpoutStats
   * 
   * @param statsSeq
   * @param includeSys
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#aggregate-spout-stats")
  public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateSpoutStats(
      List<ExecutorStats> statsSeq, boolean includeSys) {
    Map<StatsFields, Map<Object, Map<Object, Object>>> ret =
        new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();
    Map<StatsFields, Map<Object, Map<Object, Object>>> commons =
        aggregateCommonStats(statsSeq);
    preProcess(commons, includeSys);
    ret.putAll(commons);
    // acked
    List<Map<String, Map<String, Long>>> acked =
        new ArrayList<Map<String, Map<String, Long>>>();
    List<Map<String, Map<String, Long>>> failed =
        new ArrayList<Map<String, Map<String, Long>>>();
    List<Map<String, Map<String, Double>>> completeLatencies =
        new ArrayList<Map<String, Map<String, Double>>>();
    for (ExecutorStats es : statsSeq) {
      ExecutorSpecificStats specific = es.get_specific();
      if (specific.is_set_spout()) {
        SpoutStats spoutStats = specific.get_spout();
        acked.add(spoutStats.get_acked());
        failed.add(spoutStats.get_failed());
        completeLatencies.add(spoutStats.get_complete_ms_avg());
      }
    }

    ret.put(StatsFields.acked, aggregateCounts(acked));
    ret.put(StatsFields.failed, aggregateCounts(failed));
    ret.put(StatsFields.complete_latencies,
        aggregateAverages(completeLatencies, acked));
    return ret;
  }

  /**
   * getFilledStats
   * 
   * @param summs
   * @return List of not null ExecutorStats
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#get-filled-stats")
  public static List<ExecutorStats> getFilledStats(
      List<ExecutorSummary> summs) {
    List<ExecutorStats> res = new ArrayList<ExecutorStats>();
    for (ExecutorSummary es : summs) {
      ExecutorStats executorStats = es.get_stats();
      if (executorStats != null) {
        res.add(executorStats);
      }
    }
    return res;
  }

  /**
   * aggregateSpoutStreams
   * 
   * @param stats
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#aggregate-spout-streams")
  public static Map<StatsFields, Map<Object, Object>> aggregateSpoutStreams(
      Map<StatsFields, Map<Object, Map<Object, Object>>> stats) {
    Map<StatsFields, Map<Object, Object>> ret =
        new HashMap<StatsFields, Map<Object, Object>>();

    ret.put(StatsFields.acked,
        aggregateCountStreams(stats.get(StatsFields.acked)));
    ret.put(StatsFields.failed,
        aggregateCountStreams(stats.get(StatsFields.failed)));
    ret.put(StatsFields.emitted,
        aggregateCountStreams(stats.get(StatsFields.emitted)));
    ret.put(StatsFields.transferred,
        aggregateCountStreams(stats.get(StatsFields.transferred)));
    ret.put(StatsFields.complete_latencies,
        aggregateAvgStreams(stats.get(StatsFields.complete_latencies),
            stats.get(StatsFields.acked)));

    return ret;
  }

  /**
   * spoutStreamsStats
   * 
   * @param summs
   * @param includeSys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#spout-streams-stats")
  public static Map<StatsFields, Map<Object, Object>> spoutStreamsStats(
      List<ExecutorSummary> summs, boolean includeSys) {
    List<ExecutorStats> statsSeq = getFilledStats(summs);
    return aggregateSpoutStreams(aggregateSpoutStats(statsSeq, includeSys));

  }

  /**
   * aggregateBoltStats
   * 
   * @param statsSeq
   * @param includeSys
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-bolt-stats")
  public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateBoltStats(
      List<ExecutorStats> statsSeq, boolean includeSys) {
    Map<StatsFields, Map<Object, Map<Object, Object>>> result =
        new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();
    // common
    Map<StatsFields, Map<Object, Map<Object, Object>>> common =
        aggregateCommonStats(statsSeq);
    preProcess(common, includeSys);

    List<Map<String, Map<GlobalStreamId, Long>>> ackedList =
        new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
    List<Map<String, Map<GlobalStreamId, Long>>> failedList =
        new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
    List<Map<String, Map<GlobalStreamId, Long>>> executedList =
        new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
    List<Map<String, Map<GlobalStreamId, Double>>> processLatenciesList =
        new ArrayList<Map<String, Map<GlobalStreamId, Double>>>();
    List<Map<String, Map<GlobalStreamId, Double>>> executeLatencies =
        new ArrayList<Map<String, Map<GlobalStreamId, Double>>>();

    for (ExecutorStats stats : statsSeq) {
      ExecutorSpecificStats specific = stats.get_specific();
      if (specific.is_set_bolt()) {
        BoltStats boltStats = specific.get_bolt();

        ackedList.add(boltStats.get_acked());
        failedList.add(boltStats.get_failed());
        executedList.add(boltStats.get_executed());
        processLatenciesList.add(boltStats.get_process_ms_avg());
        executeLatencies.add(boltStats.get_execute_ms_avg());
      }
    }

    result.putAll(common);
    result.put(StatsFields.acked, aggregateCounts(ackedList));
    result.put(StatsFields.failed, aggregateCounts(failedList));
    result.put(StatsFields.executed, aggregateCounts(executedList));
    result.put(StatsFields.process_latencies,
        aggregateAverages(processLatenciesList, ackedList));
    result.put(StatsFields.execute_latencies,
        aggregateAverages(executeLatencies, executedList));
    return result;

  }

  /**
   * aggregateAvgStreams
   * 
   * @param avg
   * @param counts
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-avg-streams")
  public static <K1, K2, M1, M2> Map<K1, Object> aggregateAvgStreams(
      Map<K1, Map<K2, M1>> avg, Map<K1, Map<K2, M2>> counts) {

    Map<K1, Object> ret = new HashMap<K1, Object>();

    Map<K1, Map<K2, Pair<Object, Object>>> expanded =
        expandAverages(avg, counts);
    for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : expanded
        .entrySet()) {
      Map<K2, Pair<Object, Object>> streams = entry.getValue();
      Pair<Object, Object> tmpPair = null;
      for (Pair<Object, Object> v : streams.values()) {
        tmpPair = addPairs(tmpPair, v);
      }
      ret.put(entry.getKey(), valAvg(tmpPair));
    }
    return ret;
  }

  /**
   * aggregateCountStreams
   * 
   * @param stats
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-count-streams")
  public static <K, K2, V> Map<K, Object> aggregateCountStreams(
      Map<K, Map<K2, V>> stats) {
    Map<K, Object> ret = new HashMap<K, Object>();
    if (null != stats) {
      for (Map.Entry<K, Map<K2, V>> entry : stats.entrySet()) {
        Map<K2, V> values = entry.getValue();
        Object tmpCounts = 0L;
        for (V value : values.values()) {
          tmpCounts = CoreUtil.add(tmpCounts, value);
        }
        ret.put(entry.getKey(), tmpCounts);
      }
    }

    return ret;
  }

  /**
   * aggregateBoltStreams
   * 
   * @param stats
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.stats#aggregate-bolt-streams")
  public static Map<StatsFields, Map<Object, Object>> aggregateBoltStreams(
      Map<StatsFields, Map<Object, Map<Object, Object>>> stats) {
    Map<StatsFields, Map<Object, Object>> ret =
        new HashMap<StatsFields, Map<Object, Object>>();
    ret.put(StatsFields.acked,
        aggregateCountStreams(stats.get(StatsFields.acked)));
    ret.put(StatsFields.failed,
        aggregateCountStreams(stats.get(StatsFields.failed)));
    ret.put(StatsFields.emitted,
        aggregateCountStreams(stats.get(StatsFields.emitted)));
    ret.put(StatsFields.transferred,
        aggregateCountStreams(stats.get(StatsFields.transferred)));
    ret.put(StatsFields.process_latencies,
        aggregateAvgStreams(stats.get(StatsFields.process_latencies),
            stats.get(StatsFields.acked)));
    ret.put(StatsFields.executed,
        aggregateCountStreams(stats.get(StatsFields.executed)));
    ret.put(StatsFields.execute_latencies,
        aggregateAvgStreams(stats.get(StatsFields.execute_latencies),
            stats.get(StatsFields.executed)));
    return ret;
  }

  /**
   * swapMapOrder
   * 
   * @param m
   * @return map
   */
  @ClojureClass(className = "org.apache.storm.ui.helpers#swap-map-order")
  public static <K2, V2, K> Map<K2, Map<K, V2>> swapMapOrder(
      Map<K, Map<K2, V2>> m) {
    Map<K2, Map<K, V2>> res = new HashMap<K2, Map<K, V2>>();
    for (Map.Entry<K, Map<K2, V2>> entry : m.entrySet()) {
      K k = entry.getKey();
      Map<K2, V2> value = entry.getValue();
      for (Map.Entry<K2, V2> e : value.entrySet()) {
        K2 k2 = e.getKey();
        V2 v2 = e.getValue();
        if (res.containsKey(k2)) {
          res.get(k2).put(k, v2);
        } else {
          Map<K, V2> tmp = new HashMap<K, V2>();
          tmp.put(k, v2);
          res.put(k2, tmp);
        }
      }
    }
    return res;
  }

  /**
   * computeExecutorCapacity
   * 
   * @param e
   * @return executor capacity
   */
  @ClojureClass(className = "org.apache.storm.stats#compute-executor-capacity")
  public static Double computeExecutorCapacity(ExecutorSummary e) {
    Map<StatsFields, Object> stats = new HashMap<StatsFields, Object>();
    ExecutorStats executorStats = e.get_stats();
    if (executorStats != null) {
      Map<StatsFields, Map<Object, Object>> boltStreams = aggregateBoltStreams(
          aggregateBoltStats(Lists.newArrayList(executorStats), true));
      Map<Object, Map<StatsFields, Object>> swapMapOrder =
          swapMapOrder(boltStreams);
      if (swapMapOrder.containsKey("600")) {
        stats = swapMapOrder.get("600");
      }
    }
    int uptime = e.get_uptime_secs();
    int window = 0;
    if (uptime < 600) {
      window = uptime;
    } else {
      window = 600;
    }

    long executed = CoreUtil.getLong(stats.get(StatsFields.executed), 0L);
    Double latency =
        Utils.getDouble(stats.get(StatsFields.execute_latencies), 0.0);

    Double result = 0.0;
    if (window > 0) {
      result = Double.valueOf((executed * latency) / (1000 * window));
    }
    return result;

  }

  /**
   * boltStreamsStats
   * 
   * @param summs
   * @param includeSys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#bolt-streams-stats")
  public static Map<StatsFields, Map<Object, Object>> boltStreamsStats(
      List<ExecutorSummary> summs, boolean includeSys) {
    List<ExecutorStats> statsSeq = getFilledStats(summs);
    return aggregateBoltStreams(aggregateBoltStats(statsSeq, includeSys));

  }

  /**
   * totalAggregateStats
   * 
   * @param spoutSumms
   * @param boltSumms
   * @param includeSys
   * @param dumpGenerator
   * @return
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#total-aggregate-stats")
  public static Map<StatsFields, Map<Object, Object>> totalAggregateStats(
      List<ExecutorSummary> spoutSumms, List<ExecutorSummary> boltSumms,
      boolean includeSys, JsonGenerator dumpGenerator) {
    Map<StatsFields, Map<Object, Object>> aggSpoutStats =
        Stats.spoutStreamsStats(spoutSumms, includeSys);
    Map<StatsFields, Map<Object, Object>> aggBoltStats =
        Stats.boltStreamsStats(boltSumms, includeSys);
    Map<StatsFields, Map<Object, Object>> result = aggSpoutStats;
    // Include only keys that will be used. We want to count acked and
    // failed only for the "tuple trees," so we do not include those keys
    // from the bolt executors.
    result.put(StatsFields.emitted,
        CoreUtil.mergeWith(aggBoltStats.get(StatsFields.emitted),
            aggSpoutStats.get(StatsFields.emitted)));
    result.put(StatsFields.transferred,
        CoreUtil.mergeWith(aggBoltStats.get(StatsFields.transferred),
            aggSpoutStats.get(StatsFields.transferred)));

    return result;

  }

  /**
   * Computes the capacity metric for one executor given its heartbeat data and
   * uptime.
   *
   * @param boltStats
   * @param upTime
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#compute-agg-capacity")
  public static Double computeAggCapacity(BoltStats boltStats, int upTime) {
    if (upTime > 0) {
      // For each stream, create weighted averages and counts.
      Map<GlobalStreamId, Pair<Object, Object>> map =
          new HashMap<GlobalStreamId, Pair<Object, Object>>();

      Map<GlobalStreamId, Double> executeMsAvgInTenMin = boltStats
          .get_execute_ms_avg().get(String.valueOf(TEN_MIN_IN_SECONDS)) == null
              ? new HashMap<GlobalStreamId, Double>()
              : boltStats.get_execute_ms_avg()
                  .get(String.valueOf(TEN_MIN_IN_SECONDS));
      Map<GlobalStreamId, Long> executedInTenMin = boltStats.get_executed()
          .get(String.valueOf(TEN_MIN_IN_SECONDS)) == null
              ? new HashMap<GlobalStreamId, Long>()
              : boltStats.get_executed()
                  .get(String.valueOf(TEN_MIN_IN_SECONDS));
      for (Entry<GlobalStreamId, Double> e : executeMsAvgInTenMin.entrySet()) {
        Double avg = e.getValue();
        Long cnt = executedInTenMin.get(e.getKey());
        if (cnt != null) {
          map.put(e.getKey(), new Pair<Object, Object>(avg * cnt, cnt));
        }
      }

      Pair<Object, Object> pair = new Pair<Object, Object>(0d, 0l);
      for (Pair<Object, Object> entry : map.values()) {
        pair = addPairs(pair, entry);
      }

      return (Double) CoreUtil.divide(pair.getFirst(),
          Math.min(TEN_MIN_IN_SECONDS, upTime) * 1000);
    }
    return null;
  }

  /**
   * Before merge executor stats into component stats, we need to generate a
   * component stats data structure for each executor.
   *
   * @param executorAggrStats
   * @param window
   * @param isIncludeSys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-pre-merge-topo-page-spout")
  public static Pair<String, ComponentAggregateStats> aggPreMergeTopoPageSpout(
      ExecutorAggregateStats executorAggrStats, String window,
      boolean isIncludeSys) {
    String componentId =
        executorAggrStats.get_exec_summary().get_component_id();
    ComponentAggregateStats compAggStats = new ComponentAggregateStats();
    Pair<String, ComponentAggregateStats> ret =
        new Pair<String, ComponentAggregateStats>(componentId, compAggStats);

    SpecificAggregateStats specAggStats = new SpecificAggregateStats();
    SpoutAggregateStats spoutAggStats = new SpoutAggregateStats();
    specAggStats.set_spout(spoutAggStats);
    CommonAggregateStats commonAggStats = new CommonAggregateStats();
    compAggStats.set_common_stats(commonAggStats);
    compAggStats.set_specific_stats(specAggStats);

    ExecutorStats statkToWToSidToNum =
        executorAggrStats.get_exec_summary().get_stats();
    SpoutStats spoutStats = statkToWToSidToNum.get_specific().get_spout();
    Map<String, Long> emitted =
        statkToWToSidToNum.get_emitted().get(window) == null
            ? new HashMap<String, Long>()
            : statkToWToSidToNum.get_emitted().get(window);
    Map<String, Long> transferred =
        statkToWToSidToNum.get_transferred().get(window) == null
            ? new HashMap<String, Long>()
            : statkToWToSidToNum.get_transferred().get(window);
    Map<String, Long> acked = spoutStats.get_acked().get(window) == null
        ? new HashMap<String, Long>() : spoutStats.get_acked().get(window);
    Map<String, Long> failed = spoutStats.get_failed().get(window) == null
        ? new HashMap<String, Long>() : spoutStats.get_failed().get(window);
    Map<String, Double> completeLatencies =
        spoutStats.get_complete_ms_avg().get(window) == null
            ? new HashMap<String, Double>()
            : spoutStats.get_complete_ms_avg().get(window);

    Map<StatsFields, Double> map =
        aggSpoutLatAndCount(completeLatencies, acked);
    spoutAggStats
        .set_complete_latency_ms(map.get(StatsFields.complete_latencies_total));

    commonAggStats.set_acked(map.get(StatsFields.acked).longValue());
    commonAggStats.set_num_executors(1);
    commonAggStats.set_num_tasks(
        executorAggrStats.get_stats().get_common_stats().get_num_tasks());
    mkIncludeSysFilter(emitted, isIncludeSys, null);
    mkIncludeSysFilter(transferred, isIncludeSys, null);
    long emittedVal = 0l;
    long transferredVal = 0l;
    long failedVal = 0l;
    for (Long eval : emitted.values()) {
      emittedVal += eval.longValue();
    }
    for (Long tval : transferred.values()) {
      transferredVal += tval.longValue();
    }
    for (Long fval : failed.values()) {
      failedVal += fval.longValue();
    }
    commonAggStats.set_emitted(emittedVal);
    commonAggStats.set_transferred(transferredVal);
    commonAggStats.set_failed(failedVal);

    return ret;
  }

  /**
   * Before merge executor stats into component stats, we need to generate a
   * component stats data structure for each executor.
   *
   * @param executorAggrStats
   * @param window
   * @param isIncludeSys
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-pre-merge-topo-page-bolt")
  public static Pair<String, ComponentAggregateStats> aggPreMergeTopoPageBolt(
      ExecutorAggregateStats executorAggrStats, String window,
      boolean isIncludeSys) {
    String componentId =
        executorAggrStats.get_exec_summary().get_component_id();
    ComponentAggregateStats compAggStats = new ComponentAggregateStats();
    Pair<String, ComponentAggregateStats> ret =
        new Pair<String, ComponentAggregateStats>(componentId, compAggStats);

    SpecificAggregateStats specAggStats = new SpecificAggregateStats();
    BoltAggregateStats boltAggStats = new BoltAggregateStats();
    specAggStats.set_bolt(boltAggStats);
    CommonAggregateStats commonAggStats = new CommonAggregateStats();
    compAggStats.set_common_stats(commonAggStats);
    compAggStats.set_specific_stats(specAggStats);

    ExecutorStats statkToWToSidToNum =
        executorAggrStats.get_exec_summary().get_stats();
    BoltStats boltStats = statkToWToSidToNum.get_specific().get_bolt();
    Map<String, Long> emitted =
        statkToWToSidToNum.get_emitted().get(window) == null
            ? new HashMap<String, Long>()
            : statkToWToSidToNum.get_emitted().get(window);
    Map<String, Long> transferred =
        statkToWToSidToNum.get_transferred().get(window) == null
            ? new HashMap<String, Long>()
            : statkToWToSidToNum.get_transferred().get(window);
    Map<GlobalStreamId, Long> acked = boltStats.get_acked().get(window) == null
        ? new HashMap<GlobalStreamId, Long>()
        : boltStats.get_acked().get(window);
    Map<GlobalStreamId, Long> failed =
        boltStats.get_failed().get(window) == null
            ? new HashMap<GlobalStreamId, Long>()
            : boltStats.get_failed().get(window);
    Map<GlobalStreamId, Double> exeLatencies =
        boltStats.get_execute_ms_avg().get(window) == null
            ? new HashMap<GlobalStreamId, Double>()
            : boltStats.get_execute_ms_avg().get(window);
    Map<GlobalStreamId, Double> proLatencies =
        boltStats.get_process_ms_avg().get(window) == null
            ? new HashMap<GlobalStreamId, Double>()
            : boltStats.get_process_ms_avg().get(window);
    Map<GlobalStreamId, Long> executed =
        boltStats.get_executed().get(window) == null
            ? new HashMap<GlobalStreamId, Long>()
            : boltStats.get_executed().get(window);
    int upTime = executorAggrStats.get_exec_summary().get_uptime_secs();

    Map<StatsFields, Double> map =
        aggBoltLatAndCount(exeLatencies, proLatencies, executed);
    boltAggStats
        .set_execute_latency_ms(map.get(StatsFields.execute_latencies_total));
    boltAggStats
        .set_process_latency_ms(map.get(StatsFields.process_latencies_total));
    boltAggStats.set_executed(map.get(StatsFields.executed).longValue());
    boltAggStats.set_capacity(computeAggCapacity(boltStats, upTime));

    commonAggStats.set_num_executors(1);
    commonAggStats.set_num_tasks(
        executorAggrStats.get_stats().get_common_stats().get_num_tasks());
    mkIncludeSysFilter(emitted, isIncludeSys, null);
    mkIncludeSysFilter(transferred, isIncludeSys, null);
    long emittedVal = 0l;
    long transferredVal = 0l;
    long ackedVal = 0l;
    long failedVal = 0l;
    for (Long eval : emitted.values()) {
      emittedVal += eval.longValue();
    }
    for (Long tval : transferred.values()) {
      transferredVal += tval.longValue();
    }
    for (Long aval : acked.values()) {
      ackedVal += aval.longValue();
    }
    for (Long fval : failed.values()) {
      failedVal += fval.longValue();
    }
    commonAggStats.set_emitted(emittedVal);
    commonAggStats.set_transferred(transferredVal);
    commonAggStats.set_acked(ackedVal);
    commonAggStats.set_failed(failedVal);

    return ret;
  }

  /**
   * merge executor stats into component stats.
   *
   * @param topoPageInfo
   * @param compIdToBoltStats
   */
  @ClojureClass(className = "org.apache.storm.stats#merge-agg-comp-stats-topo-page-bolt")
  public static void mergeAggCompStatsTopoPageBolt(
      TopologyPageInfo topoPageInfo,
      Pair<String, ComponentAggregateStats> compIdToBoltStats) {
    String compID = compIdToBoltStats.getFirst();
    ComponentAggregateStats boltStats = compIdToBoltStats.getSecond();
    ComponentAggregateStats accBoltStats =
        topoPageInfo.get_id_to_bolt_agg_stats().get(compID);
    // merge the bolt executor stats to acc's bolt component stats
    if (accBoltStats == null) {
      topoPageInfo.get_id_to_bolt_agg_stats().put(compID, boltStats);
    } else {
      CommonAggregateStats boltStatsCommon = boltStats.get_common_stats();
      CommonAggregateStats accBoltStatsCommon = accBoltStats.get_common_stats();
      BoltAggregateStats boltStatsSpec =
          boltStats.get_specific_stats().get_bolt();
      BoltAggregateStats accBoltStatsSpec =
          accBoltStats.get_specific_stats().get_bolt();

      accBoltStatsCommon.set_num_executors(
          (int) sumOr0(accBoltStatsCommon.get_num_executors(), 1));
      accBoltStatsCommon
          .set_num_tasks((int) sumOr0(boltStatsCommon.get_num_tasks(),
              accBoltStatsCommon.get_num_tasks()));
      accBoltStatsCommon
          .set_emitted((long) sumOr0(boltStatsCommon.get_emitted(),
              accBoltStatsCommon.get_emitted()));
      accBoltStatsCommon
          .set_transferred((long) sumOr0(boltStatsCommon.get_transferred(),
              accBoltStatsCommon.get_transferred()));
      accBoltStatsCommon.set_acked((long) sumOr0(boltStatsCommon.get_acked(),
          accBoltStatsCommon.get_acked()));
      accBoltStatsCommon.set_failed((long) sumOr0(boltStatsCommon.get_failed(),
          accBoltStatsCommon.get_failed()));

      accBoltStatsSpec.set_capacity(sumOr0(boltStatsSpec.get_capacity(),
          accBoltStatsSpec.get_capacity()));
      accBoltStatsSpec.set_executed((long) sumOr0(boltStatsSpec.get_executed(),
          accBoltStatsSpec.get_executed()));
      // We sum average latency totals here to avoid dividing at each step.
      // Compute the average latencies by dividing the total by the count.
      accBoltStatsSpec
          .set_execute_latency_ms(sumOr0(boltStatsSpec.get_execute_latency_ms(),
              accBoltStatsSpec.get_execute_latency_ms()));
      accBoltStatsSpec
          .set_process_latency_ms(sumOr0(boltStatsSpec.get_process_latency_ms(),
              accBoltStatsSpec.get_process_latency_ms()));
    }
  }

  /**
   * merge executor stats into component stats.
   *
   * @param topoPageInfo
   * @param compIdToSpoutStats
   */
  @ClojureClass(className = "org.apache.storm.stats#merge-agg-comp-stats-topo-page-spout")
  public static void mergeAggCompStatsTopoPageSpout(
      TopologyPageInfo topoPageInfo,
      Pair<String, ComponentAggregateStats> compIdToSpoutStats) {
    String compID = compIdToSpoutStats.getFirst();
    ComponentAggregateStats spoutStats = compIdToSpoutStats.getSecond();
    ComponentAggregateStats accSpoutStats =
        topoPageInfo.get_id_to_spout_agg_stats().get(compID);
    // merge the spout executor stats to acc's spout component stats
    if (accSpoutStats == null) {
      topoPageInfo.get_id_to_spout_agg_stats().put(compID, spoutStats);
    } else {
      CommonAggregateStats spoutStatsCommon = spoutStats.get_common_stats();
      CommonAggregateStats accSpoutStatsCommon =
          accSpoutStats.get_common_stats();
      SpoutAggregateStats spoutStatsSpec =
          spoutStats.get_specific_stats().get_spout();
      SpoutAggregateStats accSpoutStatsSpec =
          accSpoutStats.get_specific_stats().get_spout();

      accSpoutStatsCommon.set_num_executors(
          (int) sumOr0(accSpoutStatsCommon.get_num_executors(), 1));
      accSpoutStatsCommon
          .set_num_tasks((int) sumOr0(spoutStatsCommon.get_num_tasks(),
              accSpoutStatsCommon.get_num_tasks()));
      accSpoutStatsCommon
          .set_emitted((long) sumOr0(spoutStatsCommon.get_emitted(),
              accSpoutStatsCommon.get_emitted()));
      accSpoutStatsCommon
          .set_transferred((long) sumOr0(spoutStatsCommon.get_transferred(),
              accSpoutStatsCommon.get_transferred()));
      accSpoutStatsCommon.set_acked((long) sumOr0(spoutStatsCommon.get_acked(),
          accSpoutStatsCommon.get_acked()));
      accSpoutStatsCommon
          .set_failed((long) sumOr0(spoutStatsCommon.get_failed(),
              accSpoutStatsCommon.get_failed()));

      // We sum average latency totals here to avoid dividing at each step.
      // Compute the average latencies by dividing the total by the count.
      accSpoutStatsSpec.set_complete_latency_ms(
          sumOr0(spoutStatsSpec.get_complete_latency_ms(),
              accSpoutStatsSpec.get_complete_latency_ms()));
    }
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-topo-exec-stats*")
  public static NodeInfo aggTopoExecStatsStar(String window,
      boolean isIncludeSys, TopologyPageInfo topologyPageInfo,
      ExecutorAggregateStats executorAggrStats,
      Map<String, ComponentAggregateStats> compKey, ComponentType compType) {
    boolean isSpout = ComponentType.SPOUT.equals(compType);
    // 1 TopologyStats
    TopologyStats topologyStats = topologyPageInfo.get_topology_stats();
    if (topologyStats == null) {
      topologyStats = new TopologyStats();
      topologyPageInfo.set_topology_stats(topologyStats);
    }

    // 2 window->complete_latencies_total & window->acked for executor
    Map<String, Double> wToCompLatWgtAvg = new HashMap<String, Double>();
    Map<String, Object> wToAcked = new HashMap<String, Object>();

    ExecutorStats statkToWToSidToNum =
        executorAggrStats.get_exec_summary().get_stats();

    if (isSpout) {
      // spout executor
      SpoutStats spoutStats = statkToWToSidToNum.get_specific().get_spout();
      Set<String> windows = spoutStats.get_acked().keySet();
      Map<StatsFields, Double> latAndCount;
      for (String w : windows) {
        Map<String, Long> acked = spoutStats.get_acked().get(w) == null
            ? new HashMap<String, Long>() : spoutStats.get_acked().get(w);
        Map<String, Double> completeLatencies =
            spoutStats.get_complete_ms_avg().get(w) == null
                ? new HashMap<String, Double>()
                : spoutStats.get_complete_ms_avg().get(w);

        latAndCount = aggSpoutLatAndCount(completeLatencies, acked);
        wToCompLatWgtAvg.put(w,
            latAndCount.get(StatsFields.complete_latencies_total));
        wToAcked.put(w, latAndCount.get(StatsFields.acked).longValue());
      }
    } else {
      BoltStats boltStats = statkToWToSidToNum.get_specific().get_bolt();
      // bolt executor, unused
      wToAcked = aggregateCountStreams(boltStats.get_acked());
    }

    // 3 merge executor into topology page info.
    Map<String, Long> windowToEmited = topologyStats.get_window_to_emitted();
    Map<String, Long> windowToTransferred =
        topologyStats.get_window_to_transferred();
    Map<String, Double> windowToCompLatWgtAvg =
        topologyStats.get_window_to_complete_latencies_ms();
    Map<String, Long> windowToAcked = topologyStats.get_window_to_acked();
    Map<String, Long> windowToFailed = topologyStats.get_window_to_failed();

    // 3.1 Emitted
    Map<String, Map<String, Long>> statkToWToSidToNumEmitted =
        statkToWToSidToNum.get_emitted();
    for (Entry<String, Map<String, Long>> eEntry : statkToWToSidToNumEmitted
        .entrySet()) {
      Map<String, Long> emitted = eEntry.getValue();
      mkIncludeSysFilter(emitted, isIncludeSys, null);
    }
    Map<String, Object> wToEmited =
        aggregateCountStreams(statkToWToSidToNumEmitted);
    for (Entry<String, Object> e : wToEmited.entrySet()) {
      windowToEmited.put(e.getKey(),
          (long) sumOr0(e.getValue(), windowToEmited.get(e.getKey())));
    }
    // 3.2 Transferred
    Map<String, Map<String, Long>> statkToWToSidToNumTransferred =
        statkToWToSidToNum.get_transferred();
    for (Entry<String, Map<String, Long>> tEntry : statkToWToSidToNumTransferred
        .entrySet()) {
      Map<String, Long> transferred = tEntry.getValue();
      mkIncludeSysFilter(transferred, isIncludeSys, null);
    }
    Map<String, Object> wToTransferred =
        aggregateCountStreams(statkToWToSidToNumTransferred);
    for (Entry<String, Object> e : wToTransferred.entrySet()) {
      windowToTransferred.put(e.getKey(),
          (long) sumOr0(e.getValue(), windowToTransferred.get(e.getKey())));
    }
    // 3.3 comp-lat-wgt-avg, a total value, see postAggregateTopoStats
    for (Entry<String, Double> e : wToCompLatWgtAvg.entrySet()) {
      windowToCompLatWgtAvg.put(e.getKey(),
          sumOr0(e.getValue(), windowToCompLatWgtAvg.get(e.getKey())));
    }
    // 3.4 acked
    if (isSpout) {
      for (Entry<String, Object> e : wToAcked.entrySet()) {
        windowToAcked.put(e.getKey(),
            (long) sumOr0(e.getValue(), windowToAcked.get(e.getKey())));
      }
    }
    // 3.5 failed
    if (isSpout) {
      SpoutStats spoutStats = statkToWToSidToNum.get_specific().get_spout();
      Map<String, Object> wToFailed =
          aggregateCountStreams(spoutStats.get_failed());
      for (Entry<String, Object> e : wToFailed.entrySet()) {
        windowToFailed.put(e.getKey(),
            (long) sumOr0(e.getValue(), windowToFailed.get(e.getKey())));
      }
    }

    // 3.6 comp-key
    if (isSpout) {
      Pair<String, ComponentAggregateStats> compIdToStatsPair =
          aggPreMergeTopoPageSpout(executorAggrStats, window, isIncludeSys);
      mergeAggCompStatsTopoPageSpout(topologyPageInfo, compIdToStatsPair);
    } else {
      Pair<String, ComponentAggregateStats> compIdToStatsPair =
          aggPreMergeTopoPageBolt(executorAggrStats, window, isIncludeSys);
      mergeAggCompStatsTopoPageBolt(topologyPageInfo, compIdToStatsPair);
    }

    // 3.7 workers-set
    NodeInfo workerNodeInfo = new NodeInfo();
    workerNodeInfo.set_node(executorAggrStats.get_exec_summary().get_host());
    workerNodeInfo.set_port(Sets
        .newHashSet((long) executorAggrStats.get_exec_summary().get_port()));

    return workerNodeInfo;
  }

  /**
   * Combines the aggregate stats of one executor with the given map, selecting
   * the appropriate window and including system components as specified.
   *
   * @param window
   * @param isIncludeSys
   * @param accStats
   * @param executorAggrStats
   * @param type
   * @return
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-topo-exec-stats")
  public static NodeInfo aggTopoExecStats(String window, boolean isIncludeSys,
      TopologyPageInfo accStats, ExecutorAggregateStats executorAggrStats,
      ComponentType type) {
    if (ComponentType.BOLT.equals(type)) {
      return aggTopoExecStatsBolt(window, isIncludeSys, accStats,
          executorAggrStats);
    } else if (ComponentType.SPOUT.equals(type)) {
      return aggTopoExecStatsSpout(window, isIncludeSys, accStats,
          executorAggrStats);
    }
    return null;
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-topo-exec-stats:spout")
  public static NodeInfo aggTopoExecStatsSpout(String window,
      boolean isIncludeSys, TopologyPageInfo accStats,
      ExecutorAggregateStats executorAggrStats) {
    return aggTopoExecStatsStar(window, isIncludeSys, accStats,
        executorAggrStats, accStats.get_id_to_spout_agg_stats(),
        ComponentType.SPOUT);
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-topo-exec-stats:bolt")
  public static NodeInfo aggTopoExecStatsBolt(String window,
      boolean isIncludeSys, TopologyPageInfo accStats,
      ExecutorAggregateStats executorAggrStats) {
    return aggTopoExecStatsStar(window, isIncludeSys, accStats,
        executorAggrStats, accStats.get_id_to_bolt_agg_stats(),
        ComponentType.BOLT);
  }

  /**
   * Get the component type
   *
   * @param topology topology
   * @param id component id
   * @return Returns the component type (either :bolt or :spout) for a given
   *         topology and component id. Returns nil if not found.
   */
  @ClojureClass(className = "org.apache.storm.stats#component-type")
  public static ComponentType componentType(StormTopology topology, String id) {
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    ComponentType type = null;
    if (Utils.isSystemId(id)) {
      type = ComponentType.BOLT;
    } else if (bolts.containsKey(id)) {
      type = ComponentType.BOLT;
    } else if (spouts.containsKey(id)) {
      type = ComponentType.SPOUT;
    } else {
      type = ComponentType.BOLT;
    }
    return type;
  }

  @ClojureClass(className = "org.apache.storm.stats#extract-data-from-hb")
  public static Set<ExecutorAggregateStats> extractDataFromHb(
      Map<List<Long>, NodeInfo> executorToNodeport,
      Map<Integer, String> taskToComponent,
      Map<ExecutorInfo, ExecutorCache> beats, boolean isIncludeSys,
      StormTopology topology) {
    return extractDataFromHb(executorToNodeport, taskToComponent, beats,
        isIncludeSys, topology, null);
  }

  @ClojureClass(className = "org.apache.storm.stats#extract-data-from-hb")
  public static Set<ExecutorAggregateStats> extractDataFromHb(
      Map<List<Long>, NodeInfo> executorToNodeport,
      Map<Integer, String> taskToComponent,
      Map<ExecutorInfo, ExecutorCache> beats, boolean isIncludeSys,
      StormTopology topology, String componentId) {
    Set<ExecutorAggregateStats> exeAggStatsSet =
        new HashSet<ExecutorAggregateStats>();

    for (Entry<List<Long>, NodeInfo> e : executorToNodeport.entrySet()) {
      List<Long> exeInfo = e.getKey();
      NodeInfo nodeInfo = e.getValue();
      ExecutorInfo executor = new ExecutorInfo(exeInfo.get(0).intValue(),
          exeInfo.get(exeInfo.size() - 1).intValue());
      if (beats.get(executor) == null) {
        continue;
      }
      ExecutorBeat beat = beats.get(executor).getHeartbeat();
      String id = taskToComponent.get(exeInfo.get(0).intValue());
      if ((componentId == null || componentId.equals(id))
          && (isIncludeSys || !Utils.isSystemId(id)) && beat != null) {
        ExecutorAggregateStats exeAggStats = new ExecutorAggregateStats();

        int taskNum = executor.get_task_end() - executor.get_task_start() + 1;
        ExecutorSummary execSummary = new ExecutorSummary(executor, id,
            nodeInfo.get_node(), nodeInfo.get_port_iterator().next().intValue(),
            beat.getUptime());
        execSummary.set_stats(beat.getStats());

        ComponentAggregateStats stats = new ComponentAggregateStats();
        CommonAggregateStats commonStats = new CommonAggregateStats();
        stats.set_type(componentType(topology, id));
        stats.set_common_stats(commonStats);
        commonStats.set_num_tasks(taskNum);

        exeAggStats.set_exec_summary(execSummary);
        exeAggStats.set_stats(stats);

        exeAggStatsSet.add(exeAggStats);
      }
    }
    return exeAggStatsSet;
  }

  @ClojureClass(className = "org.apache.storm.stats#aggregate-topo-stats")
  public static TopologyPageInfo aggregateTopoStats(
      TopologyPageInfo topoPageInfo, String window, boolean isIncludeSys,
      Set<ExecutorAggregateStats> data) {
    TopologyStats topoStats = new TopologyStats();
    topoPageInfo.set_topology_stats(topoStats);
    // workers-set
    Set<NodeInfo> workersSet = new HashSet<NodeInfo>();
    for (ExecutorAggregateStats e : data) {
      NodeInfo workerNodeInfo = aggTopoExecStats(window, isIncludeSys,
          topoPageInfo, e, e.get_stats().get_type());
      workersSet.add(workerNodeInfo);
    }
    topoPageInfo.set_num_workers(workersSet.size());
    return topoPageInfo;
  }

  @ClojureClass(className = "org.apache.storm.stats#compute-weighted-averages-per-window")
  public static Map<String, Double> computeWeightedAveragesPerWindow(
      TopologyPageInfo topoPageInfo, Map<String, Double> wgtAvgKey,
      Map<String, Long> divisorKey) {
    Map<String, Double> ret = new HashMap<String, Double>();
    for (Entry<String, Double> e : wgtAvgKey.entrySet()) {
      double val = 0d;
      String window = e.getKey();
      Double wgtAvg = e.getValue();
      Long divisor = divisorKey.get(window);
      if (divisor != null && divisor.longValue() > 0) {
        val = (double) CoreUtil.divide(wgtAvg, divisor);
      }
      ret.put(window, val);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.stats#post-aggregate-topo-stats")
  public static void postAggregateTopoStats(String stormId, NimbusData nimbus,
      Map<Integer, String> taskToComponent,
      Map<List<Long>, NodeInfo> executorToNodeport,
      TopologyPageInfo topologyPageInfo) throws TException {
    // 1 task executor worker numbers, num-workers is set in aggregateTopoStats
    topologyPageInfo.set_num_tasks(taskToComponent.size());
    topologyPageInfo.set_num_executors(executorToNodeport.size());
    // 2 bolt-id->stats
    Map<String, ComponentAggregateStats> idToBoltStats =
        topologyPageInfo.get_id_to_bolt_agg_stats();
    Map<String, List<ErrorInfo>> componentToErrors =
        nimbus.getStormClusterState().errorsByStormId(stormId);
    Map<String, ErrorInfo> componentToLastError =
        nimbus.getStormClusterState().lastErrorByStormId(stormId);
    for (Entry<String, ComponentAggregateStats> idToBoltStatsEntry : idToBoltStats
        .entrySet()) {
      double executedLatency = 0l;
      double processLatency = 0l;
      BoltAggregateStats boltAggStats =
          idToBoltStatsEntry.getValue().get_specific_stats().get_bolt();
      long executed = boltAggStats.get_executed();
      // executedLatencyTotal and processLatencyTotal is a total value , see
      // mergeAggCompStatsTopoPageBolt
      double executedLatencyTotal = boltAggStats.get_execute_latency_ms();
      double processLatencyTotal = boltAggStats.get_process_latency_ms();
      if (executed > 0) {
        executedLatency =
            (double) CoreUtil.divide(executedLatencyTotal, executed);
        processLatency =
            (double) CoreUtil.divide(processLatencyTotal, executed);
      }
      boltAggStats.set_process_latency_ms(processLatency);
      boltAggStats.set_execute_latency_ms(executedLatency);
      try {
        List<ErrorInfo> errList = NimbusUtils.errorsFnByStormId(
            NumErrorsChoice.ONE, idToBoltStatsEntry.getKey(), componentToErrors,
            componentToLastError);
        if (errList.size() > 0) {
          idToBoltStatsEntry.getValue().set_last_error(errList.get(0));
        }
      } catch (Exception e) {
        throw new TException(e);
      }
    }

    // 3 spout-id->stats
    Map<String, ComponentAggregateStats> idToSpoutStats =
        topologyPageInfo.get_id_to_spout_agg_stats();
    for (Entry<String, ComponentAggregateStats> idToSpoutStatsEntry : idToSpoutStats
        .entrySet()) {
      double completeLatency = 0l;
      CommonAggregateStats commonAggStats =
          idToSpoutStatsEntry.getValue().get_common_stats();
      SpoutAggregateStats spoutAggStats =
          idToSpoutStatsEntry.getValue().get_specific_stats().get_spout();
      long acked = commonAggStats.get_acked();
      // completeLatencyTotal is a total value , see
      // mergeAggCompStatsTopoPageSpout
      double completeLatencyTotal = spoutAggStats.get_complete_latency_ms();
      if (acked > 0) {
        completeLatency = (double) CoreUtil.divide(completeLatencyTotal, acked);
      }
      spoutAggStats.set_complete_latency_ms(completeLatency);
      try {
        List<ErrorInfo> errList = NimbusUtils.errorsFnByStormId(
            NumErrorsChoice.ONE, idToSpoutStatsEntry.getKey(),
            componentToErrors, componentToLastError);
        if (errList.size() > 0) {
          idToSpoutStatsEntry.getValue().set_last_error(errList.get(0));
        }
      } catch (Exception e) {
        throw new TException(e);
      }
    }
    // 4 TopologyStats
    TopologyStats topoStats = topologyPageInfo.get_topology_stats();
    // window->complete-latency, from total value to average, see
    // mergeAggCompStatsTopoPageSpout
    Map<String, Double> windowToCompleteLatency =
        computeWeightedAveragesPerWindow(topologyPageInfo,
            topoStats.get_window_to_complete_latencies_ms(),
            topoStats.get_window_to_acked());
    topoStats.set_window_to_complete_latencies_ms(windowToCompleteLatency);
  }

  /**
   * Aggregate various executor statistics for a topology from the given
   * heartbeats.
   * 
   * @param topologyPageInfo
   * 
   * @param isIncludeSys
   * @param window
   * @param topology
   * @param beats
   * @param taskToComponent
   * @param executorToNodeport
   * @param topologyId
   * @throws TException
   */
  @ClojureClass(className = "org.apache.storm.stats#agg-topo-execs-stats")
  public static void aggTopoExecsStats(TopologyPageInfo topologyPageInfo,
      String topologyId, NimbusData nimbus,
      Map<List<Long>, NodeInfo> executorToNodeport,
      Map<Integer, String> taskToComponent,
      Map<ExecutorInfo, ExecutorCache> beats, StormTopology topology,
      String window, boolean isIncludeSys) throws TException {
    Set<ExecutorAggregateStats> exeAggStatsSet = extractDataFromHb(
        executorToNodeport, taskToComponent, beats, isIncludeSys, topology);
    aggregateTopoStats(topologyPageInfo, window, isIncludeSys, exeAggStatsSet);
    postAggregateTopoStats(topologyId, nimbus, taskToComponent,
        executorToNodeport, topologyPageInfo);
  }

  /**
   * Aggregate various executor statistics for a topology from the given
   * heartbeats.
   * 
   * @param topologyPageInfo
   * 
   * @param isIncludeSys
   * @param window
   * @param topology
   * @param beats
   * @param taskToComponent
   * @param executorToNodeport
   * @param stormId
   */

  @ClojureClass(className = "org.apache.storm.stats#agg-comp-execs-stats")
  public static void aggCompExecsStats(ComponentPageInfo componentPageInfo,
      String stormId, Map<List<Long>, NodeInfo> executorToNodeport,
      Map<Integer, String> taskToComponent,
      Map<ExecutorInfo, ExecutorCache> beats, StormTopology topology,
      String window, boolean isIncludeSys, String componentId) {
    componentPageInfo.set_component_id(componentId);
    componentPageInfo.set_component_type(componentType(topology, componentId));
    for (Map.Entry<List<Long>, NodeInfo> executorNodePort : executorToNodeport
        .entrySet()) {
      List<Long> list = executorNodePort.getKey();
      ExecutorInfo executor = new ExecutorInfo(list.get(0).intValue(),
          list.get(list.size() - 1).intValue());
      NodeInfo workerSlot = executorNodePort.getValue();
      ExecutorCache beat = beats.get(executor);
      String compId = taskToComponent.get(list.get(0).intValue());
      int num_tasks = StormCommon.executorIdToTasks(list).size();
      String host = workerSlot.get_node();
      int port = workerSlot.get_port_iterator().next().intValue();
      int uptime = beat.getNimbusTime();
    }
    int num_tasks = 0;
    componentPageInfo.set_num_tasks(num_tasks);
    int num_executors = 0;
    componentPageInfo.set_num_executors(num_executors);
    Map<String, ComponentAggregateStats> windowToStats =
        new HashMap<String, ComponentAggregateStats>();
    componentPageInfo.set_window_to_stats(windowToStats);
    Map<String, ComponentAggregateStats> sidToOutputStats =
        new HashMap<String, ComponentAggregateStats>();
    componentPageInfo.set_sid_to_output_stats(sidToOutputStats);
    Map<GlobalStreamId, ComponentAggregateStats> gsidToInputStats =
        new HashMap<GlobalStreamId, ComponentAggregateStats>();
    componentPageInfo.set_gsid_to_input_stats(gsidToInputStats);
    List<ExecutorAggregateStats> exec_stats =
        new ArrayList<ExecutorAggregateStats>();
    ExecutorAggregateStats boltAggreateStats = new ExecutorAggregateStats();
    boltAggreateStats.set_exec_summary(null);
    boltAggreateStats.set_stats(null);
    ExecutorAggregateStats spoultAggreateStats = new ExecutorAggregateStats();
    spoultAggreateStats.set_exec_summary(null);
    spoultAggreateStats.set_stats(null);
    exec_stats.add(boltAggreateStats);
    exec_stats.add(spoultAggreateStats);
    componentPageInfo.set_exec_stats(exec_stats);
  }

  @ClojureClass(className = "org.apache.storm.stats#agg-comp-execs-stats")
  public static void aggreagateCompStats(String window, String isIncludeSys) {

  }

  @ClojureClass(className = "org.apache.storm.stats#thriftify-comp-page-data")
  public static void thriftityCompPageData(String topo_id,
      StormTopology topology, String comp_id) {
  }

  @ClojureClass(className = "org.apache.storm.stats#extract-nodeinfos-from-hb-for-comp")
  public static List<NodeInfo> extractNodeinfosFromHbForComp(
      Map<ExecutorInfo, NodeInfo> executorHostPort,
      Map<Integer, String> taskToComponent, boolean includeSys, String compId) {
    List<NodeInfo> retList = new ArrayList<NodeInfo>();
    for (Map.Entry<ExecutorInfo, NodeInfo> tmpEntry : executorHostPort
        .entrySet()) {
      ExecutorInfo executor = tmpEntry.getKey();
      NodeInfo nodeInfo = tmpEntry.getValue();
      int start = executor.get_task_start();
      String id = taskToComponent.get(start);
      if ((compId == null || compId.equals(id))
          && (includeSys || !Utils.isSystemId(id))) {
        retList.add(nodeInfo);
      }
    }
    return CoreUtil.distinctList(retList);
  }

  /**
   * computeBoltCapacity
   * 
   * @param executors
   * @return bolt capacity
   */
  @ClojureClass(className = "org.apache.storm.ui.stats#compute-bolt-capacity")
  public static Double computeBoltCapacity(List<ExecutorSummary> executors) {
    Double max = 0.0;
    for (ExecutorSummary e : executors) {
      Double executorCapacity = computeExecutorCapacity(e);
      if (executorCapacity != null && executorCapacity > max) {
        max = executorCapacity;
      }
    }
    return max;

  }
}
