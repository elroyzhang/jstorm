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
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class shellSubmission {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--resoucedir", aliases = {
      "-rd" }, metaVar = "key", usage = "resource dir")
  private String _resoucedir = "";

  public static void main(String[] args) throws Exception {
    new shellSubmission().realMain(args);
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
    if (_resoucedir == null || _resoucedir.isEmpty()) {
      throw new IllegalArgumentException(
          "Should privide resource dir at least!");
    }
    try {
      Map conf = ConfigUtils.readStormConfig();
      Random random = new Random();
      String tmpjarpath = "stormshell" + random.nextInt(10000000) + ".jar";
      String resourcesdir = _resoucedir;
      Utils.execCommand("jar", "cf", tmpjarpath, resourcesdir);

      String host = (String) conf.get(Config.NIMBUS_HOST);
      String port = (String) conf.get(Config.NIMBUS_THRIFT_PORT);
      StormSubmitter.submitJar(conf, tmpjarpath);
      String[] newArgs = { host, port, tmpjarpath };
      String[] cmds = (String[]) ArrayUtils.addAll(args, newArgs);
      // TODO different from shell_submission.clj
      Utils.execCommand(cmds);
      Utils.execCommand("rm", tmpjarpath);
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      throw new RuntimeException(e);
    }
  }
}
