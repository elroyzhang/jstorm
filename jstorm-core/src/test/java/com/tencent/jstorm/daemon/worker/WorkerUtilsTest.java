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
package com.tencent.jstorm.daemon.worker;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import junit.framework.Assert;

public class WorkerUtilsTest {

  private final static Logger LOG =
      LoggerFactory.getLogger(WorkerUtilsTest.class);

  private final static String STORM_ID = "test-storm-id";

  @SuppressWarnings("rawtypes")
  private Map conf;
  private LocalState localstate;
  private TopologyStatus TopologyStatus;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    conf = Utils.readStormConfig();
    TopologyStatus = TopologyStatus.ACTIVE;
    try {
      localstate = ConfigUtils.supervisorState(conf);
    } catch (IOException e) {
      LOG.error("init local Status path.", e);
    }
  }

  @After
  public void clean() {
    try {
      String localPath = String.valueOf(conf.get(Config.STORM_LOCAL_DIR));
      if (Utils.checkFileExists(localPath)) {
        FileUtils.deleteDirectory(new File(localPath));
      }
    } catch (IOException e) {
      LOG.error("clean local file failed", e);
    }
  }

  // @Test
  // public void testSupervisorNormal() {
  // synchronizeLocalState(STORM_ID, TopologyStatus);
  // // timeout 300s
  // try {
  // Thread.sleep((timeout - 1) * 1000L);
  // Assert.assertTrue(WorkerUtils.checkSupervisorStatus(
  // localstate.getLastVersion(), conf));
  // } catch (InterruptedException e) {
  // LOG.error("test sleep failed.", e);
  // }
  // }
  //
  // @Test
  // public void testSupervisorDead() {
  // synchronizeLocalState(STORM_ID, TopologyStatus);
  // try {
  // Thread.sleep((timeout + 1) * 1000L);
  // Assert.assertFalse(WorkerUtils.checkSupervisorStatus(
  // localstate.getLastVersion(), conf));
  // } catch (InterruptedException e) {
  // LOG.error("test sleep failed.", e);
  // }
  // }

  @Test
  @ClojureClass(className = "backtype.storm.worker-test#test-worker-is-connection-ready")
  public void testWorkerIsConnectionReady() {
    ConnectionWithStatus connection = Mockito.mock(ConnectionWithStatus.class);
    Mockito.when(connection.status())
        .thenReturn(ConnectionWithStatus.Status.Ready);
    Assert.assertTrue(WorkerUtils.isConnectionReady(connection));

    Mockito.when(connection.status())
        .thenReturn(ConnectionWithStatus.Status.Connecting);
    Assert.assertFalse(WorkerUtils.isConnectionReady(connection));

    Mockito.when(connection.status())
        .thenReturn(ConnectionWithStatus.Status.Closed);
    Assert.assertFalse(WorkerUtils.isConnectionReady(connection));
  }
}
