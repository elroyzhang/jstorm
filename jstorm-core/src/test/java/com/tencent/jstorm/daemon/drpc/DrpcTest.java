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
package com.tencent.jstorm.daemon.drpc;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.backtype.storm.drpc-test")
public class DrpcTest {

  /*
   * private exclamationBolt() {
   * 
   * }
   * 
   * 
   * @Test
   * 
   * @ClojureClass(className = "backtype.storm.drpc-test#test-drpc-flow") public
   * void testDrpcFlow() { LocalDRPC drpc = new LocalDRPC(); DRPCSpout spout =
   * new DRPCSpout("test", drpc); LocalCluster cluster = new LocalCluster();
   * 
   * ReturnResults rr = new ReturnResults(); Map sf = new HashMap();
   * sf.put(Grouping._Fields.SHUFFLE, "2"); Bolt bs = Thrift.mkBoltSpec(sf, rr,
   * null, null, sf);
   * 
   * Map bs1 = new HashMap(); bs1.put(Grouping._Fields.SHUFFLE, "1"); Bolt bs2 =
   * Thrift.mkBoltSpec(bs1, exclamation_bolt);
   * 
   * SpoutSpec tms = Thrift.mkSpoutSpec(spout, null, null, null); Map bs21 = new
   * HashMap(); bs21.put("1", tms);
   * 
   * Map bs22 = new HashMap(); bs22.put("2", bs1); bs22.put("3", bs);
   * StormTopology topology = Thrift.mkTopology(bs21, bs22);
   * 
   * Map tmp = new HashMap(); cluster.submitTopology("test", tmp, topology);
   * Assert.assertEquals("aaa!!!", drpc.execute("test", "aaa"));
   * Assert.assertEquals("b!!!", drpc.execute("test", "b"));
   * Assert.assertEquals("c!!!", drpc.execute("test", "c"));
   * 
   * cluster.shutdown(); drpc.shutdown(); }
   */
}
