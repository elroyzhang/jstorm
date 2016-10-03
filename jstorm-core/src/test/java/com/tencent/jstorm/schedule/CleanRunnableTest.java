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
package com.tencent.jstorm.schedule;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.jstorm.daemon.nimbus.threads.CleanInboxRunnable;
import com.tencent.jstorm.utils.CoreUtil;

public class CleanRunnableTest extends TestCase {
  private String dir_location;
  private String path;

  @BeforeClass
  public void setUp() {
    path = System.getProperty("java.io.tmpdir");
    dir_location = path + "/" + "inbox";
  }

  @Test
  public void testCleanRunnable() {

    try {
      mkdirs(dir_location);
      String file = dir_location + "/" + "1.txt";
      File test = new File(file);
      FileUtils.touch(test);
      Assert.assertTrue(test.exists());
      CoreUtil.sleepSecs(5);
      CleanInboxRunnable clean_inbox = new CleanInboxRunnable(dir_location, 2);
      clean_inbox.run();
      Assert.assertFalse(fileExists(file));
    } catch (Exception e) {
      fail(CoreUtil.stringifyError(e));
    }
  }

  public static void mkdirs(String path) throws IOException {
    new File(path).mkdirs();
  }

  public boolean fileExists(String filename) {
    return new File(filename).exists();
  }

  @AfterClass
  public void cleanUp() {
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      fail(CoreUtil.stringifyError(e));
    }

  }
}
