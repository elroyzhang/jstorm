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
package com.tencent.jstorm.log;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.tencent.jstorm.http.HttpServer;

public class TestLogLevel {
  static final PrintStream out = System.out;

  @Test
  public void testDynamicLogLevel() throws Exception {
    String logName = TestLogLevel.class.getName();
    Log testlog = LogFactory.getLog(logName);

    // only test Log4JLogger
    if (testlog instanceof Log4JLogger) {
      Logger log = ((Log4JLogger) testlog).getLogger();
      log.debug("log.debug1");
      log.info("log.info1");
      log.error("log.error1");
      Assert.assertTrue(!Level.ERROR.equals(log.getEffectiveLevel()));

      HttpServer server = new HttpServer("..", "localhost", 22222, true);
      server.start();
      int port = server.getPort();

      // servlet
      URL url =
          new URL("http://localhost:" + port + "/logLevel?log=" + logName
              + "&level=" + Level.ERROR);
      out.println("*** Connecting to " + url);
      URLConnection connection = url.openConnection();
      connection.connect();

      BufferedReader in =
          new BufferedReader(new InputStreamReader(connection.getInputStream()));
      for (String line; (line = in.readLine()) != null; out.println(line))
        ;
      in.close();

      log.debug("log.debug2");
      log.info("log.info2");
      log.error("log.error2");
      Assert.assertTrue(Level.ERROR.equals(log.getEffectiveLevel()));

      // command line
      String[] args =
          { "-setlevel", "localhost:" + port, logName, "" + Level.DEBUG };
      LogLevel.main(args);
      log.debug("log.debug3");
      log.info("log.info3");
      log.error("log.error3");
      Assert.assertTrue(Level.DEBUG.equals(log.getEffectiveLevel()));
    } else {
      out.println(testlog.getClass() + " not tested.");
    }
  }

}
