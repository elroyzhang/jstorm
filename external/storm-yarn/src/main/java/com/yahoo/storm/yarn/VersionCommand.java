/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.yarn;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.yahoo.storm.yarn.Client.ClientCommand;

class VersionCommand implements ClientCommand {

  VersionCommand() {
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    return opts;
  }

  @Override
  public String getHeaderDescription() {
    return "storm-yarn version";
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void process(CommandLine cl, Map stormConf) throws Exception {
    System.out.println(Util.getStormVersion());
  }
}
