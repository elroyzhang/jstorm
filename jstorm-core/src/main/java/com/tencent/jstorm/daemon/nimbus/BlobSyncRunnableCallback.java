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
package com.tencent.jstorm.daemon.nimbus;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.nimbus.NimbusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.blobstore.BlobSynchronizer;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 3:07:34 PM Mar 15, 2016
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
@ClojureClass(className = "org.apache.storm.daemon.nimbus#blob-sync")
public class BlobSyncRunnableCallback extends RunnableCallback {

  private static final long serialVersionUID = 1L;
  private final static Logger LOG =
      LoggerFactory.getLogger(BlobSyncRunnableCallback.class);

  private Map conf;
  private NimbusData nimbus;

  public BlobSyncRunnableCallback(Map conf, NimbusData nimbus) {
    this.conf = conf;
    this.nimbus = nimbus;
  }

  @Override
  public void run() {
    String clusterMode = ConfigUtils.clusterMode(conf);
    boolean isDistributeMode = clusterMode.endsWith("distributed");
    if (isDistributeMode) {
      if (!NimbusUtils.isLeader(nimbus, false)) {
        IStormClusterState stormClusterState = nimbus.getStormClusterState();
        NimbusInfo nimbusHostPortInfo = nimbus.getNimbusHostPortInfo();
        try {
          Set<String> blobStoreKeySet =
              NimbusUtils.getKeySeqFromBlobStore(nimbus.getBlobStore());
          Set<String> zkKeySet = new HashSet(stormClusterState
              .blobstore(new BlobSyncRunnableCallback(conf, nimbus)));
          LOG.debug("blob-sync blob-store-keys "
              + CoreUtil.setToString(blobStoreKeySet, "/") + "zookeeper-keys "
              + CoreUtil.setToString(zkKeySet, "/"));
          BlobSynchronizer syncBlobs =
              new BlobSynchronizer(nimbus.getBlobStore(), conf);
          syncBlobs.setNimbusInfo(nimbusHostPortInfo);
          syncBlobs.setBlobStoreKeySet(blobStoreKeySet);
          syncBlobs.setZookeeperKeySet(zkKeySet);
          syncBlobs.syncBlobs();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
