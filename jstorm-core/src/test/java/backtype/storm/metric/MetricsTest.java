package backtype.storm.metric;

import org.junit.Test;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.metrics-test")
public class MetricsTest {
  
  @Test
  @ClojureClass(className = "backtype.storm.metrics-test#test-custom-metric")
  public void testCustomMetric() {

  }
}
