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
package com.tencent.jstorm.daemon.executor.grouping;

import java.util.List;
import java.util.Map;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.grouping.LoadAwareShuffleGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 4:14:27 PM Feb 22, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-shuffle-grouper")
public class ShuffleGrouper implements IGrouper {
  private CustomGrouper customGrouper;

  @SuppressWarnings("rawtypes")
  public ShuffleGrouper(List<Integer> targetTasks, Map topoConf,
      WorkerTopologyContext context, String componentId, String streamId) {
    if (Utils.getBoolean(
        topoConf.get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING), false)) {
      customGrouper = new CustomGrouper(new ShuffleGrouping(), context,
          componentId, streamId, targetTasks);
    } else {
      customGrouper = new CustomGrouper(new LoadAwareShuffleGrouping(), context,
          componentId, streamId, targetTasks);
    }

  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.executor#mk-shuffle-grouper#fn")
  public List<Integer> fn(Integer taskId, List<Object> values,LoadMapping load) {
    return customGrouper.fn(taskId, values,load);
  }
}
