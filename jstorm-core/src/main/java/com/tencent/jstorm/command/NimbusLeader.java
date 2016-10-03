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
package com.tencent.jstorm.command;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.utils.NimbusClient;

import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 6:50:08 PM Apr 5, 2016
 */
public class NimbusLeader {
  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm leader");
  }

  public static void main(String[] args) {

    try {
      NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
        @Override
        public void run(Nimbus.Client nimbus) throws Exception {
          NimbusSummary leader = nimbus.getNimbusLeader();
          System.out.println(
              "-------------------------------------------------------------------");
          System.out.println(leader.get_host() + " : " + leader.get_port());
        }
      });
    } catch (Exception e1) {
      System.out.println(CoreUtil.stringifyError(e1));
      printUsage();
    }
  }
}
