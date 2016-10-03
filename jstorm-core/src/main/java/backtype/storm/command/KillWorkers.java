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

import java.util.Collection;
import java.util.Map;

import org.apache.storm.daemon.supervisor.StandaloneSupervisor;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.tencent.jstorm.ClojureClass;

@ClojureClass(className = "backtype.storm.command.kill_workers")
public class KillWorkers {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm killworkers");
  }

  @SuppressWarnings({ "rawtypes" })
  public void realMain(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      _help = true;
    }
    if (_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    Map conf = Utils.readStormConfig();
    ISupervisor isupervisor = new StandaloneSupervisor();
    SupervisorData supervisorData = new SupervisorData(conf, null, isupervisor);
    Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);
    if (workerIds == null || workerIds.isEmpty()) {
      System.out.println("No wokers need to kill!");
      return;
    }
    SupervisorUtils.shutdownAllWorkers(conf, supervisorData.getSupervisorId(),
        supervisorData.getWorkerThreadPids(), supervisorData.getDeadWorkers(),
        supervisorData.getWorkerManager());
  }

  public static void main(String[] args) throws Exception {
    new KillWorkers().realMain(args);
  }

}
