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
package com.tencent.jstorm.daemon.executor.error;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 10:41:32 AM Feb 25, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-executor-data#:report-error-and-die")
public class ReportErrorAndDie extends RunnableCallback {
  private final static Logger LOG =
      LoggerFactory.getLogger(ReportErrorAndDie.class);

  private static final long serialVersionUID = 1L;

  private ITaskReportErr reporterror;
  private RunnableCallback suicideFn;

  public ReportErrorAndDie(ITaskReportErr _reporterror,
      RunnableCallback _haltfn) {
    this.reporterror = _reporterror;
    this.suicideFn = _haltfn;
  }

  @Override
  public void uncaughtException(Thread t, Throwable error) {
    if (error != null) {
      try {
        reporterror.report(error);
      } catch (Exception e) {
        LOG.error(
            "Error while reporting error to cluster, proceeding with shutdown {}",
            CoreUtil.stringifyError(e));
      }
    }
    if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, error)
        || Utils.exceptionCauseIsInstanceOf(
            java.io.InterruptedIOException.class, error)) {
      LOG.info("Got interrupted excpetion shutting thread down...");
    } else {
      this.suicideFn.run();
    }
  }
}
