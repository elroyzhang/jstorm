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
package com.tencent.jstorm.daemon.supervisor;

import org.junit.Test;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.supervisor-test")
public class SupervisorTest {

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#launches-assignment")
  public void launchesAssignment() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#test-multiple-active-storms-multiple-supervisors")
  public void testMultipleActiveStormsMultipleSupervisors() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#heartbeats-to-nimbus")
  public void heartbeatsToNimbus() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#test-worker-launch-command")
  public void testWorkerLaunchCommand() {

  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#test-workers-go-bananas")
  public void testWorkersGoBananas() {
    // test that multiple workers are started for a port, and test that
    // supervisor shuts down propertly (doesn't shutdown the most
    // recently launched one, checks heartbeats correctly, etc.
  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#downloads-code")
  public void downloadsCode() {
  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#test-stateless")
  public void testStateless() {
  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#cleans-up-on-unassign")
  public void cleansUpOnUnassign() {
    // TODO just do reassign, and check that cleans up worker states after
    // killing but doesn't get rid of downloaded code
  }

  @Test
  @ClojureClass(className = "backtype.storm.supervisor-test#test-retry-read-assignments")
  public void testRetryReadAssignments() {

  }
}
