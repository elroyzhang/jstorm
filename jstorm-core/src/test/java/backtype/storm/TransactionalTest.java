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
package backtype.storm;

import org.junit.Test;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.backtype.storm.transactional-test")
public class TransactionalTest {

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-coordinator")
  public void testCoordinator() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-batch-bolt")
  public void testBatchBolt() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-rotating-transactional-state")
  public void testRotatingTransactionalState() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-transactional-topology")
  public void testTransactionalTopology() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-transactional-topology-restart")
  public void testTransactionalTopologyRestart() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.backtype.storm.transactional-test#test-opaque-transactional-topology")
  public void testOpaqueTransactionalTopology() {

  }

}
