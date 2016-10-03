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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.storm.Thrift;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 4:14:47 PM Feb 22, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-grouper")
public class MkGrouper {
  private Grouping._Fields grouptype;
  private IGrouper grouper;

  @SuppressWarnings("rawtypes")
  public MkGrouper(WorkerTopologyContext context, String componentId,
      String streamId, Fields outFields, Grouping thriftGrouping,
      List<Integer> targetTasks, Map topoConf) {
    this.grouptype = Thrift.groupingType(thriftGrouping);

    init(thriftGrouping, outFields, context, componentId, streamId, targetTasks,
        topoConf);
  }

  @SuppressWarnings("rawtypes")
  private void init(Grouping thriftGrouping, Fields outFields,
      WorkerTopologyContext context, String componentId, String streamId,
      List<Integer> targetTasks, Map topoConf) {
    Collections.sort(targetTasks);
    switch (grouptype) {
    case FIELDS:
      grouper = new FieldsGrouper(thriftGrouping, outFields, targetTasks);
      break;
    case ALL:
      grouper = new AllGrouper(targetTasks);
      break;
    case SHUFFLE:
      grouper = new ShuffleGrouper(targetTasks, topoConf, context, componentId,
          streamId);
      break;
    case LOCAL_OR_SHUFFLE:
      grouper = new LocalOrShuffleGrouper(context, targetTasks, topoConf,
          componentId, streamId);
      break;
    case NONE:
      grouper = new NoneGrouper(targetTasks);
    case CUSTOM_OBJECT:
      CustomStreamGrouping grouping = (CustomStreamGrouping) Thrift
          .instantiateJavaObject(thriftGrouping.get_custom_object());
      grouper = new CustomGrouper(grouping, context, componentId, streamId,
          targetTasks);
      break;
    case CUSTOM_SERIALIZED:
      grouper = new CustomGrouper(
          (CustomStreamGrouping) Utils.javaDeserialize(
              thriftGrouping.get_custom_serialized(), Serializable.class),
          context, componentId, streamId, targetTasks);
      break;
    default:
      grouper = new DefaultGrouper();
      break;
    }

  }

  /*
   * Returns a function that returns a vector of which task indices to send
   * tuple to, or just a single task index.
   */
  public List<Integer> grouper(Integer taskId, List<Object> values,
      LoadMapping load) {
    return grouper.fn(taskId, values, load);
  }

  public Grouping._Fields getGrouptype() {
    return grouptype;
  }

  public void setGrouptype(Grouping._Fields grouptype) {
    this.grouptype = grouptype;
  }

}
