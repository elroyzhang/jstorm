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
package com.tencent.jstorm.daemon.nimbus.transitions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.threads.DelayEventRunnable;
import com.tencent.jstorm.utils.thread.BaseCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.TopologyStatus;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 11:40:26 AM Mar 1, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.nimbus#delay-event")
public class DelayEvent extends BaseCallback {
  private static Logger LOG = LoggerFactory.getLogger(DelayEvent.class);
  private NimbusData data;
  private String stormId;
  private int delaySecs;
  private TopologyStatus status;

  public DelayEvent(NimbusData data, String stormId, int delaySecs,
      TopologyStatus status) {
    this.data = data;
    this.stormId = stormId;
    this.delaySecs = delaySecs;
    this.status = status;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    LOG.info("Delaying event " + status.toString() + " for "
        + String.valueOf(delaySecs) + " secs for " + stormId);
    data.getTimer().schedule(delaySecs,
        new DelayEventRunnable(data, stormId, status));
    return null;
  }

}
