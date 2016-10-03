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
package com.tencent.jstorm.daemon.nimbus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.tencent.jstorm.localstate.LocalStateUtils;

import org.apache.storm.Config;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

public class CleanTopologyHistoryRunnableCallback implements Runnable {
  @SuppressWarnings("rawtypes")
  private Map conf;
  private NimbusData nimbus;

  @SuppressWarnings("rawtypes")
  public CleanTopologyHistoryRunnableCallback(Map conf, NimbusData nimbus) {
    this.conf = conf;
    this.nimbus = nimbus;
  }

  @Override
  public void run() {
    synchronized (nimbus.getTopologyHistoryLock()) {
      int mins =
          Utils.getInt(conf.get(Config.LOGVIEWER_CLEANUP_AGE_MINS), 10800);
      int cutOffAge = Time.currentTimeSecs() - (60 * mins);
      LocalState topoHistoryState = nimbus.getTopoHistorystate();
      List<LSTopoHistory> currHistory =
          LocalStateUtils.getLSTopoList(topoHistoryState);
      List<LSTopoHistory> newHistory = new ArrayList<LSTopoHistory>();
      for (LSTopoHistory line : currHistory) {
        if (line.get_time_stamp() > cutOffAge) {
          newHistory.add(line);
        }
      }
      LocalStateUtils.putLSTopoList(topoHistoryState, newHistory);
    }
  }
}
