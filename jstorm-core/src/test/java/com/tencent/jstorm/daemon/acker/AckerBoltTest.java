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
package com.tencent.jstorm.daemon.acker;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.Map;

import org.apache.storm.daemon.Acker;
import org.junit.Test;

import com.tencent.jstorm.mock.MockTupleHelpers;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import junit.framework.Assert;

public class AckerBoltTest {

  @Test
  public void testCommon() {
    Assert.assertEquals("__acker", Acker.ACKER_COMPONENT_ID);
    Assert.assertEquals("__ack_init", Acker.ACKER_INIT_STREAM_ID);
    Assert.assertEquals("__ack_ack", Acker.ACKER_ACK_STREAM_ID);
    Assert.assertEquals("__ack_fail", Acker.ACKER_FAIL_STREAM_ID);
    Assert.assertEquals(3, Acker.TIMEOUT_BUCKET_NUM);
  }

  @Test
  public void testSystemTickStream() {
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();
    Acker ackerBolt = new Acker();
    Map conf = mock(Map.class);
    TopologyContext context = mock(TopologyContext.class);
    OutputCollector collector = mock(OutputCollector.class);
    ackerBolt.prepare(conf, context, collector);

    // when
    ackerBolt.execute(tickTuple);

    // then
    verifyZeroInteractions(collector);
  }

  @Test
  public void testAckTuple() {
    // given
    Tuple normalTuple = MockTupleHelpers.mockNormalTuple(new Object());
    Tuple tickTuple = MockTupleHelpers.mockTickTuple();

    Acker bolt = new Acker();
    Map conf = mock(Map.class);
    TopologyContext context = mock(TopologyContext.class);
    OutputCollector collector = mock(OutputCollector.class);
    bolt.prepare(conf, context, collector);

    // when
    bolt.execute(normalTuple);
    bolt.execute(tickTuple);

    // then
    verify(collector).ack(any(Tuple.class));
  }
}
