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
package backtype.storm.command;

import java.util.Map;

import org.apache.storm.zookeeper.Zookeeper;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 7:50:53 PM Mar 16, 2016
 */
@ClojureClass(className = "backtype.storm.command.dev-zookeeper")
public class DevZookeeper {

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) throws Exception {

    Map conf = Utils.readStormConfig();
    Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
    String localpath =
        Utils.getString(conf.get(Config.DEV_ZOOKEEPER_PATH),
            "storm-local");

    Utils.forceDelete(localpath);
    Zookeeper.mkInprocessZookeeper(localpath, Utils.getInt(port));
  }
}
