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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.NimbusClient;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.command.activate")
public class activate {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "";

  private static void printUsage() {
    System.out.println("Usage:");
    System.out
        .println("    $STORM_HOME/bin/storm activate --name topoloygname");
  }

  public static void main(String[] args) throws Exception {
    new activate().realMain(args);
  }

  @SuppressWarnings("rawtypes")
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
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException(
          "Should privide TOPOLOGY_NAME at least!");
    }
    try {
      CoreUtil.activateTopology(_name);
      System.out
          .println("Successfully submit command activate " + _name);
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    } 
  }
}
