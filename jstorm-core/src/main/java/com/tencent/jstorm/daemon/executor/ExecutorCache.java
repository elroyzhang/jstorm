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
package com.tencent.jstorm.daemon.executor;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.cluster.ExecutorBeat;

import com.tencent.jstorm.ClojureClass;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 9:45:37 AM Mar 15, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.nimbus#update-executor-cache#{}")
public class ExecutorCache {
  private boolean isTimedOut;
  private int nimbusTime;
  private int executorReportedTime;
  private ExecutorBeat heartbeat;

  /**
   * 
   * @param isTimeOut whether nimbus task is time out
   * @param nimbusTime
   * @param executorReportedTime
   * @param heartbeat
   */
  public ExecutorCache(boolean isTimeOut, int nimbusTime,
      int executorReportedTime, ExecutorBeat heartbeat) {
    this.isTimedOut = isTimeOut;
    this.nimbusTime = nimbusTime;
    this.executorReportedTime = executorReportedTime;
    this.heartbeat = heartbeat;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this,
        ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public int getNimbusTime() {
    return nimbusTime;
  }

  public void setNimbusTime(int nimbusTime) {
    this.nimbusTime = nimbusTime;
  }

  public ExecutorBeat getHeartbeat() {
    return heartbeat;
  }

  public void setHeartbeat(ExecutorBeat heartbeat) {
    this.heartbeat = heartbeat;
  }

  public int getExecutorReportedTime() {
    return executorReportedTime;
  }

  public void setExecutorReportedTime(int taskReportedTime) {
    this.executorReportedTime = taskReportedTime;
  }

  public boolean isTimedOut() {
    return isTimedOut;
  }

  public void setTimedOut(boolean isTimeOut) {
    this.isTimedOut = isTimeOut;
  }
}