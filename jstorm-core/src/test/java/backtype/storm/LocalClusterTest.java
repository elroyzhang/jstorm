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

import java.util.Map;

import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.storm.topology.TopologyBuilder;

public class LocalClusterTest extends TestCase {
  private LocalCluster localCluster;

  @BeforeClass
  protected void setUp() {

  }

  @Test
  public void testLaunchLocalCluster() throws Exception {
    localCluster = new LocalCluster();
    Map cluster_map = localCluster.getState();
    
    localCluster.shutdown();
  }
  
  @Test
  public void testSubmitTopology(){
    localCluster = new LocalCluster();
    TopologyBuilder builder = new TopologyBuilder();
    localCluster.shutdown();
  }

  @AfterClass
  protected void cleanUp() {

  }
}
