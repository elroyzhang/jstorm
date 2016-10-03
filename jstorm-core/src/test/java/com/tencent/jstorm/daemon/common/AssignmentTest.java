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
package com.tencent.jstorm.daemon.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileRequest;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import junit.framework.Assert;

public class AssignmentTest {
  private Assignment assignment;
  private String masterCodeDir;
  private Map<List<Long>, NodeInfo> executorToNodeport =
      new HashMap<List<Long>, NodeInfo>();
  private Map<String, String> nodeHost = new HashMap<String, String>();
  private Map<List<Long>, Long> taskStartTimeSecs =
      new HashMap<List<Long>, Long>();

  @Test
  public void testGetTaskToPortbyNode() {
    masterCodeDir = "test/codedir";
    List<Long> ei1 = Lists.newArrayList(1l, 1l);
    List<Long> ei2 = Lists.newArrayList(2l, 2l);
    NodeInfo ws1 = new NodeInfo("sv1", Sets.newHashSet(123l));
    NodeInfo ws2 = new NodeInfo("sv2", Sets.newHashSet(456l));
    executorToNodeport.put(ei1, ws1);
    executorToNodeport.put(ei2, ws2);
    nodeHost.put("sv1", "host1");
    nodeHost.put("sv2", "host2");
    taskStartTimeSecs.put(ei1, 1l);
    taskStartTimeSecs.put(ei1, 2l);
    assignment = new Assignment(masterCodeDir, new ArrayList<ProfileRequest>());
    assignment.set_executor_node_port(executorToNodeport);
    assignment.set_node_host(nodeHost);
    assignment.set_executor_start_time_secs(taskStartTimeSecs);
    // , nodeHost,
    Map<List<Long>, NodeInfo> executorToNodePort =
        assignment.get_executor_node_port();
    Assert.assertEquals(1, executorToNodePort.size());
    Assert.assertEquals(ws1, executorToNodePort.get(ei1));

    Map<String, String> nodeHosts = assignment.get_node_host();
    Assert.assertEquals(2, nodeHosts.size());
    Assert.assertEquals("host1", nodeHosts.get("sv1"));
    Assert.assertEquals("host2", nodeHosts.get("sv2"));

    Assert.assertEquals(masterCodeDir, assignment.get_master_code_dir());
  }
}
