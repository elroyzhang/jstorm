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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;

/**
 * Constructs a TimeCacheMap instance with a blob store timeout whose expiration
 * callback invokes cancel on the value held by an expired entry when that value
 * is an AtomicOutputStream and calls close otherwise.
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 3:08:14 PM Jan 7, 2016
 * @param <K>
 * @param <V>
 */
@SuppressWarnings({ "rawtypes", "deprecation" })
@ClojureClass(className = "backtype.storm.daemon.nimbus#mk-blob-cache-map")
public class MkBlobCacheMap<K, V> extends TimeCacheMap
    implements TimeCacheMap.ExpiredCallback<K, V> {
  private final static Logger LOG =
      LoggerFactory.getLogger(MkBlobCacheMap.class);

  public MkBlobCacheMap(Map conf) {
    super(Utils.getInt(conf.get(Config.NIMBUS_BLOBSTORE_EXPIRATION_SECS), 600));
  }

  @Override
  public void expire(K id, V stream) {
    try {
      if (stream != null) {
        if (stream instanceof AtomicOutputStream) {
          AtomicOutputStream channel = (AtomicOutputStream) stream;
          channel.cancel();
          channel.close();
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
