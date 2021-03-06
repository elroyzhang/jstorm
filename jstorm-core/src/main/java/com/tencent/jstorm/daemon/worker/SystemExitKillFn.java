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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

public class SystemExitKillFn extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(SystemExitKillFn.class);
  private String msg = "System Exit 1.";

  public SystemExitKillFn() {

  }

  public SystemExitKillFn(String msg) {
    this.msg = msg;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    Exception e = (Exception) args[0];
    LOG.error(msg + CoreUtil.stringifyError(e));
    System.exit(1);
    return e;
  }

  @Override
  public void run() {
    System.exit(1);
  }
}
