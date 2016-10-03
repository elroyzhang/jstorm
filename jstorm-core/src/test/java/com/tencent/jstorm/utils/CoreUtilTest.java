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
package com.tencent.jstorm.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.jstorm.stats.Pair;

import org.apache.storm.utils.Utils;
import junit.framework.Assert;

public class CoreUtilTest {
  private static String content;
  private static String tmpPath;
  private static String encoding = "UTF-8";

  @BeforeClass
  public static void setUp() throws Exception {
    content = String.valueOf(System.currentTimeMillis());
    tmpPath =
        System.getProperty("user.dir") + File.separator + content + "test.tmp";
    writeFile(tmpPath, content, encoding);

  }

  @Test
  public void testslurp() throws Exception {

    String result = CoreUtil.slurp(tmpPath);
    Assert.assertEquals(content, result);
  }

  @Test
  public void testslurpFilePath() throws Exception {
    String result = CoreUtil.slurp(tmpPath, encoding);
    Assert.assertEquals(content, result);
  }

  @Test
  public void testslurpFile() throws Exception {
    String result = CoreUtil.slurp(tmpPath, encoding);
    Assert.assertEquals(content, result);
  }

  public static void writeFile(String path, String content, String encoding)
      throws Exception {
    File file = new File(path);
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(file);
      out.write(content.getBytes(encoding));
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @AfterClass
  public static void cleanUp() {
    new File(tmpPath).delete();
  }

  @Test
  public void testShellCmd() throws Exception {
    List<String> cmd = new ArrayList<String>();
    cmd.add("abcd'ddda'adfdf'");
    cmd.add("13353'");
    cmd.add("tesddd''");
    String result = Utils.shellCmd(cmd);
    String respectResult =
        "'abcd'\"'\"'ddda'\"'\"'adfdf'\"'\"'' '13353'\"'\"'' 'tesddd'\"'\"''\"'\"''";
    Assert.assertEquals(respectResult, result);
  }

  @Test
  public void testDivide() throws Exception {
    Assert.assertEquals(1l, CoreUtil.divide((long) 3, (long) 2));
    Assert.assertEquals((int) 1, CoreUtil.divide((int) 3, (int) 2));
    Assert.assertEquals(2.5d, CoreUtil.divide((double) 5.0, (double) 2.0));
    Assert.assertEquals(2.5f, CoreUtil.divide((float) 3.0, (float) 2.0));
  }

  @Test
  public void testCastNumberType() {
    Long longVal = 1l;
    Assert.assertEquals(1, CoreUtil.castNumberType(longVal, Integer.class));
    Assert.assertEquals(1.0f, CoreUtil.castNumberType(longVal, Float.class));
    Assert.assertEquals(1l, CoreUtil.castNumberType(longVal, Long.class));
    Assert.assertEquals(1.0d, CoreUtil.castNumberType(longVal, Double.class));

    Integer intValue = 1;
    Assert.assertEquals(1, CoreUtil.castNumberType(intValue, Integer.class));
    Assert.assertEquals(1.0f, CoreUtil.castNumberType(intValue, Float.class));
    Assert.assertEquals(1l, CoreUtil.castNumberType(intValue, Long.class));
    Assert.assertEquals(1.0d, CoreUtil.castNumberType(intValue, Double.class));

    Float floatValue = 5.0f;
    Assert.assertEquals(5, CoreUtil.castNumberType(floatValue, Integer.class));
    Assert.assertEquals(5.0f, CoreUtil.castNumberType(floatValue, Float.class));
    Assert.assertEquals(5l, CoreUtil.castNumberType(floatValue, Long.class));
    Assert.assertEquals(5.0d,
        CoreUtil.castNumberType(floatValue, Double.class));

    Double doubleValue = 5.0d;
    Assert.assertEquals(5, CoreUtil.castNumberType(doubleValue, Integer.class));
    Assert.assertEquals(5.0f,
        CoreUtil.castNumberType(doubleValue, Float.class));
    Assert.assertEquals(5l, CoreUtil.castNumberType(doubleValue, Long.class));
    Assert.assertEquals(5.0d,
        CoreUtil.castNumberType(doubleValue, Double.class));
  }

  @Test
  public void testAdd() {
    // Integer
    Assert.assertEquals(3, CoreUtil.add(1, 2.0f));
    Assert.assertEquals(3, CoreUtil.add(1, 2.0d));
    Assert.assertEquals(3, CoreUtil.add(1, 2l));
    Assert.assertEquals(3, CoreUtil.add(1, 2));

    // Long
    Assert.assertEquals(3l, CoreUtil.add(1l, 2.0f));
    Assert.assertEquals(3l, CoreUtil.add(1l, 2.0d));
    Assert.assertEquals(3l, CoreUtil.add(1l, 2l));
    Assert.assertEquals(3l, CoreUtil.add(1l, 2));

    // Float
    Assert.assertEquals(3.0f, CoreUtil.add(1f, 2.0f));
    Assert.assertEquals(3.0f, CoreUtil.add(1f, 2.0d));
    Assert.assertEquals(3.0f, CoreUtil.add(1f, 2l));
    Assert.assertEquals(3.0f, CoreUtil.add(1f, 2));
    
    // Double
    Assert.assertEquals(3.0d, CoreUtil.add(1d, 2.0f));
    Assert.assertEquals(3.0d, CoreUtil.add(1d, 2.0d));
    Assert.assertEquals(3.0d, CoreUtil.add(1d, 2l));
    Assert.assertEquals(3.0d, CoreUtil.add(1d, 2));
  }

}
