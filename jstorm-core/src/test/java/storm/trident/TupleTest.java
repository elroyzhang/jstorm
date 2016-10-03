package storm.trident;

import org.junit.Test;

import com.tencent.jstorm.ClojureClass;
import junit.framework.TestCase;

@ClojureClass(className = "storm.trident.tuple-test")
public class TupleTest extends TestCase {

  @Test
  @ClojureClass(className = "storm.trident.tuple-test#test-fresh")
  public void testFresh() throws Exception {

  }
  
  @Test
  @ClojureClass(className = "storm.trident.tuple-test#test-projection")
  public void testProjection() throws Exception {

  }
  
  @Test
  @ClojureClass(className = "storm.trident.tuple-test#test-appends")
  public void testAppends() throws Exception {

  }
}
