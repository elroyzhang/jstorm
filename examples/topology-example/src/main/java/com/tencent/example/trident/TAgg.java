package com.tencent.example.trident;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TAgg implements Aggregator<Object> {
  private static final long serialVersionUID = 1L;
  protected short tableID = 10;
  protected boolean debug = false;
  protected int AGG_COUNTS_PRINT = 100;
  protected AtomicLong tupleCounts = new AtomicLong(0L);
  protected AtomicLong completeCounts = new AtomicLong(0L);
  protected int COMPLETE_COUNTS_PRINT = 100;
  private static final Logger logger = LoggerFactory.getLogger(TAgg.class);

  public void prepare(Map conf, TridentOperationContext context) {
    this.debug = Utils.getBoolean(conf.get("djc_debug"), false);
  }

  public void cleanup() {
  }

  public Object init(Object batchId, TridentCollector collector) {
    logger.info("aggregate,init,batchId={}", batchId);
    return null;
  }

  public void aggregate(Object val, TridentTuple tuple,
      TridentCollector collector) {
    logger.info("aggregate,tuple={}", tuple);
  }

  public void complete(Object val, TridentCollector collector) {
    logger.info("aggregate,complete");
  }
}
