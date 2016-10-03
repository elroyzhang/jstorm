package backtype.storm.testing;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.testing#CompletableSpout")
public interface CompletableSpout {

  /**
   * Whether all the tuples for this spout have been completed.
   * 
   * @return
   */
  public boolean isExhausted();

  /**
   * Cleanup any global state kept
   */
  public void cleanup();

  /**
   * Prepare the spout (globally) before starting the topology
   */
  public void startup();
}
