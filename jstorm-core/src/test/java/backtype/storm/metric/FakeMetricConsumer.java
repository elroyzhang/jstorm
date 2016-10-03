package backtype.storm.metric;

import java.util.Collection;
import java.util.Map;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

@ClojureClass(className = "backtype.storm.metric.testing#gen-class#FakeMetricConsumer")
public class FakeMetricConsumer implements IMetricsConsumer {

  public FakeMetricConsumer() {

  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, Object registrationArgument,
      TopologyContext context, IErrorReporter errorReporter) {
  }

  @Override
  public void handleDataPoints(TaskInfo taskInfo,
      Collection<DataPoint> dataPoints) {

  }

  @Override
  public void cleanup() {
  }

}
