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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 10:06:17 AM Feb 26, 2016
 */
public class GroupingUtils {
  /**
   * 
   * @param workerContext
   * @param componentId
   * @param topoConf
   * @return Returns map of stream id to component id to grouper
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "org.apache.storm.daemon.executor#outbound-components")
  public static Map<String, Map<String, MkGrouper>> outboundComponents(
      WorkerTopologyContext workerContext, String componentId, Map topoConf) {
    Map<String, Map<String, MkGrouper>> stormIdToComponentIdToGrouper =
        new HashMap<String, Map<String, MkGrouper>>();

    Map<String, Map<String, Grouping>> outputGroupings =
        workerContext.getTargets(componentId);

    for (Entry<String, Map<String, Grouping>> entry : outputGroupings
        .entrySet()) {
      String streamId = entry.getKey();
      Map<String, Grouping> componentToGrouping = entry.getValue();
      Map<String, MkGrouper> componentGrouper =
          outboundGroupings(workerContext, componentId, streamId,
              workerContext.getComponentOutputFields(componentId, streamId),
              componentToGrouping, topoConf);
      stormIdToComponentIdToGrouper.put(streamId, componentGrouper);
    }
    for (String streamId : workerContext.getComponentCommon(componentId)
        .get_streams().keySet()) {
      if (!stormIdToComponentIdToGrouper.containsKey(streamId)) {
        stormIdToComponentIdToGrouper.put(streamId, null);
      }
    }
    return stormIdToComponentIdToGrouper;
  }

  /**
   * 
   * @param workerContext
   * @param thisComponentId
   * @param streamId
   * @param outFields
   * @param componentToGrouping
   * @param topoConf
   * @return
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "org.apache.storm.daemon.executor#outbound-groupings")
  private static Map<String, MkGrouper> outboundGroupings(
      WorkerTopologyContext workerContext, String thisComponentId,
      String streamId, Fields outFields,
      Map<String, Grouping> componentToGrouping, Map topoConf) {
    Map<String, MkGrouper> componentGrouper = new HashMap<String, MkGrouper>();
    for (Entry<String, Grouping> cg : componentToGrouping.entrySet()) {
      String component = cg.getKey();
      List<Integer> componentTasks = workerContext.getComponentTasks(component);
      if (componentTasks != null && componentTasks.size() > 0) {
        Grouping tgrouping = cg.getValue();
        MkGrouper grouper = new MkGrouper(workerContext, thisComponentId,
            streamId, outFields, tgrouping,
            workerContext.getComponentTasks(component), topoConf);
        componentGrouper.put(component, grouper);
      }
    }
    return componentGrouper;
  }
}
