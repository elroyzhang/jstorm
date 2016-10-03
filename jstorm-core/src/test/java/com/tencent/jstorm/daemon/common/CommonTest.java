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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.daemon.StormCommon;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.tencent.jstorm.SetupTestEnv;

import org.apache.storm.Config;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.utils.Utils;
import junit.framework.Assert;

public class CommonTest extends SetupTestEnv {

  @Test
  public void testIsSystemId() {
    Assert.assertEquals("__system", StormCommon.SYSTEM_STREAM_ID);
    String id = "__system:1";
    String id2 = "spout:1";
    Assert.assertTrue(Utils.isSystemId(id));
    Assert.assertFalse(Utils.isSystemId(id2));
  }

  @Test
  public void testExecutorIdToTasks() {
    List<Long> executorInfo = Lists.newArrayList(1l, 1l);
    List<Integer> taskIds = StormCommon.executorIdToTasks(executorInfo);
    Assert.assertEquals(1, taskIds.size());
    Assert.assertEquals(1, (int) taskIds.get(0));
    //
    //
    List<Long> executorInfo1 = Lists.newArrayList(1l, 3l);
    List<Integer> taskIds1 = StormCommon.executorIdToTasks(executorInfo1);
    Assert.assertEquals(3, taskIds1.size());
    Assert.assertTrue(taskIds1.contains(1));
    Assert.assertTrue(taskIds1.contains(2));
    Assert.assertTrue(taskIds1.contains(3));
  }

  @Test
  public void testHasAcker() {
    Map stormConf = new HashMap();
    Integer tae = null;
    Assert.assertTrue(StormCommon.hasAckers(stormConf));
    stormConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
    Assert.assertFalse(StormCommon.hasAckers(stormConf));
    stormConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
    Assert.assertTrue(StormCommon.hasAckers(stormConf));
  }

}
