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

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.task.WorkerTopologyContext;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-custom-grouper")
public class CustomGrouper implements IGrouper {
  private CustomStreamGrouping grouping;
  private boolean isLoadAwareCustomStreamGrouping = false;

  public CustomGrouper(CustomStreamGrouping grouping,
      WorkerTopologyContext context, String componentId, String streamId,
      List<Integer> targetTasks) {
    this.grouping = grouping;
    this.grouping.prepare(context, new GlobalStreamId(componentId, streamId),
        targetTasks);
    this.isLoadAwareCustomStreamGrouping = grouping instanceof LoadAwareCustomStreamGrouping;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-custom-grouper#fn")
  public List<Integer> fn(Integer taskId, List<Object> values,
      LoadMapping load) {
    if (this.isLoadAwareCustomStreamGrouping) {
      return ((LoadAwareCustomStreamGrouping) grouping).chooseTasks(taskId,
          values, load);
    } else {
      return grouping.chooseTasks(taskId, values);
    }
  }
}
