package backtype.storm.testing;

import java.util.List;

import org.apache.storm.testing.FixedTupleSpout;

public class FixedTupleSpoutExt extends FixedTupleSpout implements
    CompletableSpout {

  public FixedTupleSpoutExt(List tuples) {
    super(tuples);
  }

  public FixedTupleSpoutExt(List tuples, String fieldName) {
    super(tuples, fieldName);
  }

  @Override
  public boolean isExhausted() {
    if (this.getSourceTuples().size() == this.getCompleted()) {
      return true;
    }
    return false;
  }

  @Override
  public void cleanup() {
    super.cleanup();
  }

  @Override
  public void startup() {

  }

}