package com.tencent.jstorm.mock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;

public class MockTupleHelpers {

  private MockTupleHelpers() {
  }

  private static final String ANY_NON_SYSTEM_COMPONENT_ID =
      "irrelevant_component_id";
  private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

  public static Tuple mockNormalTuple(Object obj) {
    Tuple tuple =
        MockTupleHelpers.mockTuple(ANY_NON_SYSTEM_COMPONENT_ID,
            ANY_NON_SYSTEM_STREAM_ID);
    when(tuple.getValue(0)).thenReturn(obj);
    return tuple;
  }

  public static Tuple mockTickTuple() {
    return mockTuple(Constants.SYSTEM_COMPONENT_ID,
        Constants.SYSTEM_TICK_STREAM_ID);
  }

  public static Tuple mockTuple(String componentId, String streamId) {
    Tuple tuple = mock(Tuple.class);
    when(tuple.getSourceComponent()).thenReturn(componentId);
    when(tuple.getSourceStreamId()).thenReturn(streamId);
    return tuple;
  }
}
