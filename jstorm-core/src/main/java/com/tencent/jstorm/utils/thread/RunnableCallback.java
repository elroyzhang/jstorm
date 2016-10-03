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
package com.tencent.jstorm.utils.thread;

import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Callable;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 3:23:53 PM Feb 25, 2016
 */
public class RunnableCallback implements Runnable, Callback, Serializable,
    UncaughtExceptionHandler, Callable<Object> {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    return null;
  }

  @Override
  public void run() {

  }

  public Exception error() {
    return null;
  }

  public Object getResult() {
    return null;
  }

  public void shutdown() {
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
  }

  @Override
  public Object call() throws Exception {
    return null;
  }

}
