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

import org.apache.storm.Thrift;

import com.google.common.collect.Lists;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.TupleUtils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 4:03:28 PM Feb 26, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-fields-grouper")
public class FieldsGrouper implements IGrouper {

  private Fields outFields;
  private List<Integer> targetTasks;
  private int numTasks;
  private Fields groupFields;
  private boolean isGlobalGrouping;

  public FieldsGrouper(Grouping thriftGrouping, Fields outFields,
      List<Integer> targetTasks) {
    this.outFields = outFields;
    this.targetTasks = targetTasks;
    this.numTasks = targetTasks.size();
    this.isGlobalGrouping = Thrift.isGlobalGrouping(thriftGrouping);
    this.groupFields = new Fields(Thrift.fieldGrouping(thriftGrouping));
  }

  @Override
  @ClojureClass(className = "org.apache.storm.daemon.executor#mk-fields-grouper#fn")
  public List<Integer> fn(Integer taskId, List<Object> values,
      LoadMapping load) {
    if (isGlobalGrouping) {
      // It's possible for target to have multiple tasks if it reads
      // multiple sources
      return Lists.newArrayList(targetTasks.get(0));
    } else {
      Integer ret = targetTasks.get(Math
          .abs(TupleUtils.listHashCode(outFields.select(groupFields, values))
              % numTasks));
      return Lists.newArrayList(ret);
    }
  }
}
