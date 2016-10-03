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
package org.apache.storm.daemon.supervisor;

import org.apache.storm.Config;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;

public class StandaloneSupervisor implements ISupervisor {
  private static final Logger LOG =
      LoggerFactory.getLogger(StandaloneSupervisor.class);
    private String supervisorId;
    private Map conf;
    private List<Integer> portList;

    @ClojureClass(className="for docker")
    private Map<Integer, Integer> virtualPortToPort;

    @SuppressWarnings("unchecked")
    @Override
    @ClojureClass(className="for docker")
    public void prepare(Map stormConf, String schedulerLocalDir) {
        try {
            LocalState localState = new LocalState(schedulerLocalDir);
            String supervisorId = localState.getSupervisorId();
            if (supervisorId == null) {
                supervisorId = generateSupervisorId();
                localState.setSupervisorId(supervisorId);
            }
            this.conf = stormConf;
            this.supervisorId = supervisorId;
            this.virtualPortToPort = (Map<Integer, Integer>) conf
                .get("storm.virtual.real.ports");
            List<Integer> ports =
                (List<Integer>) conf.get(Config.SUPERVISOR_SLOTS_PORTS);
            this.portList = new ArrayList<Integer>();
            for (Integer svPort : ports) {
              if (virtualPortToPort != null && virtualPortToPort.containsKey(svPort)) {
                portList.add(virtualPortToPort.get(svPort));
                LOG.info("Use {} ---> {}", svPort, virtualPortToPort.get(svPort));
              } else {
                portList.add(svPort);
                LOG.info("Keep {}", svPort);
              }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSupervisorId() {
        return supervisorId;
    }

    @Override
    public String getAssignmentId() {
        return supervisorId;
    }

    @Override
    @ClojureClass(className="for docker")
    public Object getMetadata() {
        return this.portList;
    }

    @Override
    public boolean confirmAssigned(int port) {
        return true;
    }

    @Override
    public void killedWorker(int port) {

    }

    @Override
    public void assigned(Collection<Integer> ports) {

    }

    public String generateSupervisorId(){
        return Utils.uuid();
    }
}