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
package backtype.storm.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.TupleImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.jstorm.ClojureClass;

import backtype.storm.testing.Testing;
import junit.framework.Assert;

@ClojureClass(className = "backtype.storm.tuple-test")
public class TupleTest {
  private static TupleImpl tuple;

  @BeforeClass
  public static void setUp() {
    List<Object> values = new ArrayList<Object>();
    values.add(12);
    values.add("hello");
    List<String> fields = new ArrayList<String>();
    fields.add("foo");
    fields.add("bar");
    tuple = Testing.testTuple(values, null, null, fields);
  }

  @Test
  @ClojureClass(className = "backtype.storm.tuple-test#test-lookup")
  public void testLookup() {
    Assert.assertEquals(Integer.valueOf(12), tuple.getIntegerByField("foo"));
    Assert.assertEquals("hello", tuple.getStringByField("bar"));
  }

  @Test
  @ClojureClass(className = "backtype.storm.tuple-test#test-indexed")
  public void testIndexed() {
    Assert.assertEquals(Integer.valueOf(12), tuple.getInteger(0));
    Assert.assertEquals("hello", tuple.getString(1));
  }

  @Test
  @ClojureClass(className = "backtype.storm.tuple-test#test-seq")
  public void testSeq() {
    Assert.assertEquals("([\"foo\" 12] [\"bar\" \"hello\"])", tuple.getMap()
        .seq().toString());
  }

  @Test
  @ClojureClass(className = "backtype.storm.tuple-test#test-map")
  public void testMap() {
    Assert.assertEquals("{\"foo\" 12, \"bar\" \"hello\"}", tuple.getMap()
        .toString());

  }
}
