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
package com.tencent.jstorm.counter;

import java.io.IOException;

import junit.framework.TestCase;

@SuppressWarnings("rawtypes")
public class BaseTestCounters extends TestCase {
  enum myCounters {
    TEST1, TEST2
  };

  private static final long MAX_VALUE = 10;

  // Generates enum based counters
  private Counters getEnumCounters(Enum[] keys, boolean update) {
    Counters counters = new Counters();
    for (Enum key : keys) {
      for (long i = 0; i < MAX_VALUE; ++i) {
        if (update) {
          counters.updateCounter(key, i);
        } else {
          counters.incrCounter(key, i);
        }
      }
    }
    return counters;
  }

  private Counters getEnumCounters(String[] gNames, String[] cNames, boolean update) {
    Counters counters = new Counters();
    for (String gName : gNames) {
      for (String cName : cNames) {
        for (long i = 0; i < MAX_VALUE; ++i) {
          if(update){
            counters.updateCounter(gName, cName, i);
          }else{
            counters.incrCounter(gName, cName, i);
          }
        }
      }
    }
    return counters;
  }

  protected void testCounters(boolean update) throws IOException {

    Enum[] keysWithoutResource = { myCounters.TEST1, myCounters.TEST2 };

    String[] groups = { "group1", "group2", "group{}()[]" };
    String[] counters = { "counter1", "counter2", "counter{}()[]" };

    // I. Check enum counters that have resource bundler

    // II. Check enum counters that dont have resource bundler
    Counters cnts = getEnumCounters(keysWithoutResource, update);
    System.out.println(cnts.toString());

    // III. Check string counters
    Counters cnts2 = getEnumCounters(groups, counters, update);
    System.out.println(cnts2.toString());
  }

}
