package backtype.storm.metric;

import java.util.Map;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

@ClojureClass(className = "backtype.storm.metrics-test#count-acks")
public class CountAcksBolt implements IRichBolt {

  private static final long serialVersionUID = 1L;
  private CountMetric mycustommetric;
  private OutputCollector collector;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    mycustommetric = new CountMetric();
    this.collector = collector;
    context.registerMetric("my-custom-metric", mycustommetric, 5);
  }

  @Override
  public void execute(Tuple tuple) {
    mycustommetric.incr();
    collector.ack(tuple);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
