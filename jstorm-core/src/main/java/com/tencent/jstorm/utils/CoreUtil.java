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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.thread.BaseCallback;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 3:27:45 PM Feb 29, 2016
 */
@ClojureClass(className = "org.apache.storm.util")
@SuppressWarnings({ "unchecked", "rawtypes" })
public class CoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CoreUtil.class);

  @ClojureClass(className = "backtype.storm.util#local-mkdirs")
  public static void localMkdirs(String path) throws IOException {
    LOG.debug("Making dirs at {}", path);
    FileUtils.forceMkdir(new File(path));
  }

  /**
   * Check if process exits
   * 
   * @param processId
   * @return true if process exists, else return false
   */
  public static Boolean isProcessExists(String processId) {
    String path = File.separator + "proc" + File.separator + processId;
    return Utils.checkFileExists(path);
  }

  /**
   * Touching file at path
   * 
   * @param path
   * @throws IOException
   */
  @ClojureClass(className = "backtype.storm.util#touch")
  public static void touch(String path) throws IOException {
    LOG.debug("Touching file at" + path);
    boolean success = (new File(path)).createNewFile();
    if (!success) {
      throw new RuntimeException("Failed to touch " + path);
    }
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure.core/slurp#filePath")
  public static String slurp(String filePath) throws Exception {
    File file = new File(filePath);
    return slurp(file, "UTF-8");
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure.core/slurp#filePath")
  public static String slurp(String filePath, String encoding)
      throws Exception {
    File file = new File(filePath);
    return slurp(file, encoding);
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure.core/slurp#file")
  public static String slurp(File file, String encoding) throws Exception {
    String result = null;
    FileInputStream in = null;
    try {
      Long filelength = file.length();
      if (filelength > 1024 * 1024 * 10) {
        throw new RuntimeException(
            "file size more than 10M ,slurp unsupport  operation");
      }
      byte[] filecontent = new byte[filelength.intValue()];
      in = new FileInputStream(file);
      in.read(filecontent);
      result = new String(filecontent, encoding);
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return result;
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure.core/spit")
  public static void spit(String filePath, String content) throws Exception {
    spit(filePath, content, "UTF-8");
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure.core/spit")
  public static void spit(String filePath, String content, String encoding)
      throws Exception {
    File file = new File(filePath);
    FileUtils.writeStringToFile(file, content, encoding);
  }

  @ClojureClass(className = "backtype.storm.util#to-json")
  public static String to_json(Map m) {
    if (m == null) {
      return null;
    } else {
      return JSONValue.toJSONString(m);
    }
  }

  @ClojureClass(className = "backtype.storm.util#from-json")
  public static Object from_json(String json) {
    if (json == null) {
      return null;
    } else {
      return JSONValue.parse(json);
    }
  }

  @ClojureClass(className = "backtype.storm.util#multi-set")
  public static <V> HashMap<V, Integer> multiSet(List<V> list) {
    // Returns a map of elem to count
    HashMap<V, Integer> rtn = new HashMap<V, Integer>();
    for (V v : list) {
      int cnt = 1;
      if (rtn.containsKey(v)) {
        cnt += rtn.get(v);
      }
      rtn.put(v, cnt);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#zip-contains-dir?")
  public static boolean zipContainsDir(String zipfile, String target) {

    Enumeration<? extends ZipEntry> entries = null;
    ZipFile zipFile = null;
    try {
      zipFile = new ZipFile(zipfile);
      entries = zipFile.entries();
      while (entries != null && entries.hasMoreElements()) {
        ZipEntry ze = entries.nextElement();
        String name = ze.getName();
        if (name.startsWith(target + "/")) {
          return true;
        }
      }
    } catch (IOException e) {
      LOG.error(e + "zipContainsDir error");
    } finally {
      if (zipFile != null) {
        try {
          zipFile.close();
        } catch (IOException e) {
          LOG.error(e + "zipContainsDir error");
        }
      }
    }

    return false;
  }

  @ClojureClass(className = "backtype.storm.util#url-encode")
  public static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("UnsupportedEncodingException: {}", stringifyError(e));
    }
    return s;
  }

  @ClojureClass(className = "backtype.storm.util#url-decode")
  public static String urlDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("UnsupportedEncodingException: {}", stringifyError(e));
    }
    return s;
  }

  @ClojureClass(className = "backtype.storm.util#filter-key")
  public static <K, V> Map<K, V> filterKey(BaseCallback afn, Map<K, V> amap) {
    Map<K, V> result = new HashMap<K, V>();
    for (Map.Entry<K, V> entry : amap.entrySet()) {
      K k = entry.getKey();
      V v = entry.getValue();
      if ((Boolean) afn.execute(k) == true) {
        result.put(k, v);
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.util#map-key")
  public static <K, V> Map<Object, V> mapKey(BaseCallback afn, Map<K, V> amap) {
    Map<Object, V> result = new HashMap<Object, V>();
    for (Map.Entry<K, V> entry : amap.entrySet()) {
      K k = entry.getKey();
      V v = entry.getValue();
      Object obj = afn.execute(k);
      result.put(obj, v);
    }
    return result;
  }

  public static void ensureProcessKilled(Integer pid) {
    // in this function, just kill the process 5 times
    // make sure the process be killed definitely
    for (int i = 0; i < 5; i++) {
      try {
        Utils.execCommand("kill", "-9", String.valueOf(pid));
        LOG.info("kill -9 process " + pid);
        Utils.sleep(100);
      } catch (ExecuteException e) {
        LOG.info("Error when trying to kill " + pid
            + ". Process is probably already dead. ");
      } catch (Exception e) {
        LOG.info("Error when trying to kill " + pid + ".Exception ", e);
      }
    }
  }

  public static void processKilled(Integer pid) {
    try {
      Utils.execCommand("kill", String.valueOf(pid));
      LOG.info("kill process " + pid);
    } catch (ExecuteException e) {
      LOG.info("Error when trying to kill " + pid
          + ". Process is probably already dead. ");
    } catch (Exception e) {
      LOG.info("Error when trying to kill " + pid + ".Exception ", e);
    }
  }

  @ClojureClass(className = "backtype.storm.util#tokenize-path")
  public static List<String> tokenizePath(String path) {
    String[] toks = path.split("/");
    java.util.ArrayList<String> rtn = new ArrayList<String>();
    for (String str : toks) {
      if (!str.isEmpty()) {
        rtn.add(str);
      }
    }
    return rtn;
  }

  public static String localTempPath() {
    return System.getProperty("java.io.tmpdir") + "/" + Utils.uuid();
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure_core/clojure.core/select-keys")
  public static <K, V> Map<K, V> select_keys(Map<K, V> all, Set<K> filter) {
    // user=> (select-keys {:a 1 :b 2} [:a])
    // {:a 1}
    Map<K, V> filterMap = new HashMap<K, V>();

    for (Entry<K, V> entry : all.entrySet()) {
      if (filter.contains(entry.getKey())) {
        filterMap.put(entry.getKey(), entry.getValue());
      }
    }
    return filterMap;
  }

  @ClojureClass(className = "backtype.storm.util#select-keys-pred")
  public static <K, V> Map<K, V> select_keys_pred(Set<K> filter,
      Map<K, V> all) {
    Map<K, V> filterMap = new HashMap<K, V>();

    for (Entry<K, V> entry : all.entrySet()) {
      if (!filter.contains(entry.getKey())) {
        filterMap.put(entry.getKey(), entry.getValue());
      }
    }

    return filterMap;
  }

  @ClojureClass(className = "backtype.storm.util#barr")
  public static byte[] barr(byte v) {
    byte[] byteArray = new byte[1];
    byteArray[0] = v;

    return byteArray;
  }

  @ClojureClass(className = "backtype.storm.util#map-diff")
  public static <K, V> HashMap<K, V> map_diff(Map<K, V> m1, Map<K, V> m2) {
    // "Returns mappings in m2 that aren't in m1"
    Map<K, V> ret = new HashMap<K, V>();
    for (Entry<K, V> entry : m2.entrySet()) {
      K key = entry.getKey();
      V val = entry.getValue();
      if (!m1.containsKey(key)) {
        ret.put(key, val);
      } else if (m1.containsKey(key) && !m1.get(key).equals(val)) {
        ret.put(key, val);
      }
    }
    return (HashMap<K, V>) ret;

  }

  @ClojureClass(className = "backtype.storm.util#sleep-secs")
  public static void sleepSecs(long secs) throws InterruptedException {
    if (secs > 0) {
      Time.sleep(1000L * secs);
    }
  }

  @ClojureClass(className = "backtype.storm.util#sleep-until-secs")
  public static void sleepUntilSecs(long targetSecs)
      throws InterruptedException {
    Time.sleepUntil(targetSecs * 1000);
  }

  @ClojureClass(className = "backtype.storm.util#secs-to-millis-long")
  public static long secsToMillisLong(int secs) {
    return (long) (1000L * secs);
  }

  @ClojureClass(className = "backtype.storm.util#secs-to-millis-long")
  public static long secsToMillisLong(double secs) {
    return (long) (1000L * secs);
  }

  @ClojureClass(className = "backtype.storm.util#current-time-secs")
  public static int current_time_secs() {
    return Time.currentTimeSecs();
  }

  @ClojureClass(className = "backtype.storm.util#time-delta")
  public static int timeDelta(int timeSecs) {
    return current_time_secs() - timeSecs;
  }

  @ClojureClass(className = "backtype.storm.util#time-delta-ms")
  public static long time_delta_ms(long time_ms) {
    return System.currentTimeMillis() - time_ms;
  }

  @ClojureClass(className = "backtype.storm.util#interleave-all")
  public static <T> List<T> interleaveAll(List<List<T>> nodeList) {
    if (null != nodeList && nodeList.size() > 0) {
      List<T> first = new ArrayList<T>();
      List<List<T>> rest = new ArrayList<List<T>>();
      for (List<T> node : nodeList) {
        if (null != node && node.size() > 0) {
          first.add(node.get(0));
          rest.add(node.subList(1, node.size()));
        }
      }
      List<T> interleaveRest = interleaveAll(rest);
      if (null != interleaveRest) {
        first.addAll(interleaveRest);
      }
      return first;
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.util#uptime-computer")
  public static int uptimeComputer() {
    return timeDelta(current_time_secs());
  }

  @ClojureClass(className = "http://clojuredocs.org/clojure_core/clojure.core/bit-xor")
  public static Long bit_xor(Object a, Object b) {
    // Bitwise exclusive or
    Long rtn = 0l;

    if (a instanceof Long && b instanceof Long) {
      rtn = ((Long) a) ^ ((Long) b);
      return rtn;
    } else if (b instanceof Set) {
      Long bs = bit_xor_vals_sets((Set) b);
      return bit_xor(a, bs);
    } else if (a instanceof Set) {
      Long as = bit_xor_vals_sets((Set) a);
      return bit_xor(as, b);
    } else {
      Long ai = Long.parseLong(String.valueOf(a));
      Long bi = Long.parseLong(String.valueOf(b));
      rtn = ai ^ bi;
      return rtn;
    }
  }

  public static <T> Long bit_xor_vals_sets(java.util.Set<T> vals) {
    Long rtn = 0l;
    for (T n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#bit-xor-vals")
  public static Long bit_xor_vals(Object... vals) {
    Long rtn = 0l;
    for (Object n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#bit-xor-vals")
  public static <T> Long bit_xor_vals(java.util.List<T> vals) {
    Long rtn = 0l;
    for (T n : vals) {
      rtn = bit_xor(rtn, n);
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#any-intersection")
  public static List<String> anyIntersection(List<String> list) {

    List<String> rtn = new ArrayList<String>();
    Set<String> idSet = new HashSet<String>();

    for (String id : list) {
      if (idSet.contains(id)) {
        rtn.add(id);
      } else {
        idSet.add(id);
      }
    }

    return rtn;
  }

  @ClojureClass(className = "backtype.storm.util#stringify-error")
  public static String stringifyError(Throwable error) {
    StringWriter result = new StringWriter();
    PrintWriter printer = new PrintWriter(result);
    error.printStackTrace(printer);
    printer.close();
    return result.toString();
  }

  /**
   * Return a set that is the first set without elements of the remaining sets
   * 
   * see https
   * ://google-collections.googlecode.com/svn/trunk/javadoc/com/google/
   * common/collect/Sets.html#difference(java.util.Set, java.util.Set)
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.set/difference")
  public static <T> Set<T> set_difference(Set<T> keySet, Set<T> keySet2) {
    return Sets.difference(keySet, keySet2);
  }

  public static Object add(Object oldValue, Object newValue) {
    if (oldValue == null) {
      return newValue;
    }
    if (oldValue instanceof Long) {
      if (newValue == null) {
        return (Long) oldValue;
      } else {
        return (Long) oldValue
            + castNumberType((Number) newValue, Long.class).longValue();
      }
    } else if (oldValue instanceof Integer) {
      if (newValue == null) {
        return (Integer) oldValue;
      } else {
        return (Integer) oldValue
            + castNumberType((Number) newValue, Integer.class).intValue();
      }
    } else if (oldValue instanceof Double) {
      if (newValue == null) {
        return (Double) oldValue;
      } else {
        return (Double) oldValue
            + castNumberType((Number) newValue, Double.class).doubleValue();
      }
    } else if (oldValue instanceof Float) {
      if (newValue == null) {
        return (Float) oldValue;
      } else {
        return (Float) oldValue
            + castNumberType((Number) newValue, Float.class).floatValue();
      }
    } else {
      return null;
    }
  }

  public static Number castNumberType(Number value, Class targetType) {
    if (value instanceof Long) {
      if (Long.class == targetType) {
        return value;
      } else if (Integer.class == targetType) {
        return new Long((Long) value).intValue();
      } else if (Double.class == targetType) {
        return new Long((Long) value).doubleValue();
      } else if (Float.class == targetType) {
        return new Long((Long) value).floatValue();
      }
    } else if (value instanceof Integer) {
      if (Integer.class == targetType) {
        return value;
      } else if (Long.class == targetType) {
        return new Integer((Integer) value).longValue();
      } else if (Double.class == targetType) {
        return new Integer((Integer) value).doubleValue();
      } else if (Float.class == targetType) {
        return new Integer((Integer) value).floatValue();
      }
    } else if (value instanceof Double) {
      if (Double.class == targetType) {
        return value;
      } else if (Long.class == targetType) {
        return new Double((Double) value).longValue();
      } else if (Integer.class == targetType) {
        return new Double((Double) value).intValue();
      } else if (Float.class == targetType) {
        return new Double((Double) value).floatValue();
      }
    } else if (value instanceof Float) {
      if (Float.class == targetType) {
        return value;
      } else if (Long.class == targetType) {
        return new Float((Float) value).longValue();
      } else if (Integer.class == targetType) {
        return new Float((Float) value).intValue();
      } else if (Double.class == targetType) {
        return new Float((Float) value).doubleValue();
      }
    }
    return value;
  }

  public static <V1, V2> Object multiply(V1 value1, V2 value2) {
    Object ret = null;
    if (null == value1 && null == value2) {
      ret = 0;
    } else if (null == value1) {
      ret = value2;
    } else if (null == value2) {
      ret = value1;
    } else {
      if (value1 instanceof Integer) {
        if (value2 instanceof Integer) {
          ret = (Integer) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Integer) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Integer) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Integer) value1 * (Double) value2;
        } else {
          ret = 0;
        }
      } else if (value1 instanceof Long) {
        if (value2 instanceof Integer) {
          ret = (Long) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Long) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Long) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Long) value1 * (Double) value2;
        } else {
          ret = 0L;
        }
      } else if (value1 instanceof Float) {
        if (value2 instanceof Integer) {
          ret = (Float) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Float) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Float) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Float) value1 * (Double) value2;
        } else {
          ret = 0.0f;
        }
      } else if (value1 instanceof Double) {
        if (value2 instanceof Integer) {
          ret = (Double) value1 * (Integer) value2;
        } else if (value2 instanceof Long) {
          ret = (Double) value1 * (Long) value2;
        } else if (value2 instanceof Float) {
          ret = (Double) value1 * (Float) value2;
        } else if (value2 instanceof Double) {
          ret = (Double) value1 * (Double) value2;
        } else {
          ret = 0.0d;
        }
      } else {
        return 0;
      }
    }
    return ret;
  }

  public static <V> Object divide(V dividend, V divisor) {
    if (null == divisor) {
      throw new IllegalArgumentException("divisor can't be null");
    } else if (null == dividend) {
      return 0;
    } else {
      if (dividend instanceof Integer) {
        if (Integer.valueOf(String.valueOf(divisor)) == 0) {
          throw new IllegalArgumentException("divisor can't be 0");
        }
        return (Integer) dividend
            / castNumberType((Number) divisor, Integer.class).intValue();
      } else if (dividend instanceof Long) {
        if (Long.valueOf(String.valueOf(divisor)) == 0L) {
          throw new IllegalArgumentException("divisor can't be 0");
        }
        return (Long) dividend
            / castNumberType((Number) divisor, Long.class).longValue();
      } else if (dividend instanceof Float) {
        if (Float.valueOf(String.valueOf(divisor)).equals(0.0)) {
          throw new IllegalArgumentException("divisor can't be 0");
        }
        return (Float) dividend
            / castNumberType((Number) divisor, Float.class).floatValue();
      } else if (dividend instanceof Double) {
        if (Double.valueOf(String.valueOf(divisor)).equals(0.0)) {
          throw new IllegalArgumentException("divisor can't be 0");
        }
        return (Double) dividend
            / castNumberType((Number) divisor, Double.class).doubleValue();
      } else {
        return 0;
      }
    }
  }

  public static Object mergeList(List<Object> list) {
    Object ret = null;

    for (Object value : list) {
      ret = add(ret, value);
    }

    return ret;
  }

  public static List<Object> mergeList(List<Object> result, Object add) {
    if (add instanceof Collection) {
      for (Object o : (Collection) add) {
        result.add(o);
      }
    } else if (add instanceof Set) {
      for (Object o : (Collection) add) {
        result.add(o);
      }
    } else {
      result.add(add);
    }

    return result;
  }

  /**
   * merger with for map
   * 
   * @param list
   * @return
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/merge-with")
  public static <K, V> Map<K, V> mergeWith(List<Map<K, V>> list) {
    Map<K, V> ret = new HashMap<K, V>();

    for (Map<K, V> listEntry : list) {
      if (listEntry == null) {
        continue;
      }
      for (Entry<K, V> mapEntry : listEntry.entrySet()) {
        K key = mapEntry.getKey();
        V value = mapEntry.getValue();

        V retValue = (V) add(ret.get(key), value);

        ret.put(key, retValue);
      }
    }

    return ret;
  }

  /**
   * merge with for map
   * 
   * @param maps
   * @return
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/merge-with")
  public static <K, V> Map<K, V> mergeWith(Map<K, V>... maps) {
    return mergeWith(Arrays.asList(maps));
  }

  /**
   * Returns a new set containing all elements that are contained in both given
   * 
   * @see https
   *      ://google-collections.googlecode.com/svn/trunk/javadoc/com/google/
   *      common/collect/Sets.html#intersection(java.util.Set, java.util.Set)
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.set/intersection")
  public static <E> Set<E> SetIntersection(final Set<? extends E> set1,
      final Set<? extends E> set2) {
    return (Set<E>) Sets.intersection(set1, set2);
  }

  /**
   * Returns a lazy sequence of the elements of coll with duplicates removed
   * 
   * @param <T>
   */
  @ClojureClass(className = "http://clojuredocs.org/clojure.core/distinct")
  public static <T> List<T> distinctList(List<T> input) {
    List<T> retList = new ArrayList<T>();

    for (T object : input) {
      if (retList.contains(object)) {
        continue;
      } else {
        retList.add(object);
      }

    }

    return retList;
  }

  public static <K> List<K> parseList(Object o, List<K> defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return (List<K>) o;
  }

  public static <K, V> Map<K, V> parseMap(Object o, Map<K, V> defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return (Map<K, V>) o;
  }

  public static <K, V> V mapValue(Map<K, V> map, K key) {
    return null != map ? map.get(key) : null;
  }

  @ClojureClass(className = "backtype.storm.util#logs-rootname")
  private static String logsRootname(String stormId, Integer port) {
    return stormId + "-worker-" + port;
  }

  @ClojureClass(className = "backtype.storm.util#logs-filename")
  public static String logsFilename(String stormId, Integer port) {
    StringBuffer sb = new StringBuffer();
    sb.append(stormId).append(Utils.FILE_PATH_SEPARATOR).append(port)
        .append(Utils.FILE_PATH_SEPARATOR).append("worker.log");
    return sb.toString();
  }

  @ClojureClass(className = "backtype.storm.util#event-logs-filename")
  public static String eventLogsFilename(String stormId, Integer port) {
    StringBuffer sb = new StringBuffer();
    sb.append(stormId).append(Utils.FILE_PATH_SEPARATOR).append(port)
        .append(Utils.FILE_PATH_SEPARATOR).append("events.log");
    return sb.toString();
  }

  @ClojureClass(className = "add by JStorm develper, http://git.code.oa.com/trc/jstorm/issues/29")
  public static String dumpsDirname(String stormId, Integer port) {
    Map conf = Utils.readStormConfig();
    StringBuffer sb = new StringBuffer(Utils.getString(
        conf.get(Config.STORM_WORKERS_ARTIFACTS_DIR), "workers-artifacts"));
    sb.append(Utils.FILE_PATH_SEPARATOR).append(stormId)
        .append(Utils.FILE_PATH_SEPARATOR).append(port)
        .append(Utils.FILE_PATH_SEPARATOR).append("dumps");
    return sb.toString();
  }

  @ClojureClass(className = "backtype.storm.util#map-val")
  public static <K, V> Map<K, V> mapVal(RunnableCallback afn, Map<K, V> amap) {
    Map<K, V> retMap = new HashMap<K, V>();
    for (Map.Entry<K, V> tmp : amap.entrySet()) {
      retMap.put(tmp.getKey(), (V) afn.execute(tmp.getValue()));
    }
    return retMap;
  }

  @ClojureClass(className = "backtype.storm.util#retry-on-exception")
  public static Object retryOnException(int retries, String taskDescription,
      RunnableCallback cb) {
    Object res = null;
    try {
      res = cb.execute();
    } catch (Exception e) {
      if (retries < 0) {
        throw e;
      } else {
        res = e;
      }
    }
    if (res instanceof Exception) {
      LOG.error("Failed to " + taskDescription + ". Will make [" + retries
          + "] more attempts.");
      retryOnException(--retries, taskDescription, cb);
    } else {
      LOG.debug("Successful " + taskDescription + ".");
    }
    return res;
  }

  public static String setToString(Set<String> set, String splitor) {
    String ret = "";
    if (set == null) {
      return ret;
    }
    for (String str : set) {
      ret += str + splitor;
    }
    return ret;
  }

  public static Float getFloat(Object o, Float defaultValue) {
    if (null == o) {
      return defaultValue;
    }
    if (o instanceof String) {
      return Float.valueOf(String.valueOf(o));
    } else if (o instanceof Integer) {
      Integer value = (Integer) o;
      return Float.valueOf((Integer) value);
    } else if (o instanceof Long) {
      final long l = (Long) o;
      if (l <= Float.MAX_VALUE && l >= Float.MIN_VALUE || l == 0) {
        return Float.valueOf((Long) l);
      }
    } else if (o instanceof Double) {
      final double d = (Double) o;
      if (d <= Float.MAX_VALUE && d >= Float.MIN_VALUE || d == 0.0) {
        return (float) d;
      }
    } else if (o instanceof Float) {
      return (Float) o;
    }
    throw new IllegalArgumentException(
        "Don't know how to convert " + o + " to float");
  }

  public static Long getLong(Object o, Long defaultValue) {
    if (null == o) {
      return defaultValue;
    }
    if (o instanceof Long || o instanceof Integer || o instanceof Short
        || o instanceof Byte) {
      return ((Number) o).longValue();
    } else if (o instanceof String) {
      return Long.parseLong((String) o);
    }
    throw new IllegalArgumentException(
        "Don't know how to convert " + o + " to long");
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#mk-halting-timer")
  public static StormTimer mkHaltingTimer(String timerName) {
    return new StormTimer(timerName, new OnkillFn());
  }

  public static void rebalanceTopology(String topologyName, int waitSeconds,
      int numWorkers, String executors) throws Exception {
    NimbusClient client = getNimbusClient();
    try {
      RebalanceOptions options = new RebalanceOptions();
      options.set_wait_secs(waitSeconds);
      options.set_num_workers(numWorkers);
      String info =
          "Topology " + topologyName + " is rebalancing " + " with delaySesc "
              + waitSeconds + " number of workers " + numWorkers;
      if (executors != null) {
        HashMap<String, Integer> executorsMap = parseExecutor(executors);
        options.set_num_executors(executorsMap);
        info += " with executor " + executors.toString() + " ";
      }
      client.getClient().rebalance(topologyName, options);
      LOG.info(info);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public static void killTopology(String topologyName, int waitSeconds)
      throws Exception {
    NimbusClient client = getNimbusClient();
    List<String> successKillName = new ArrayList<String>();
    try {
      KillOptions options = new KillOptions();
      options.set_wait_secs(waitSeconds);
      String[] tnameArr = topologyName.split("\\,");
      for (String tname : tnameArr) {
        client.getClient().killTopologyWithOpts(tname, options);
        successKillName.add(tname);
      }
      String info = "Successfully submit command kill " + tnameArr.toString()
          + " with delay " + waitSeconds + " secs";
      LOG.info(info);
    } catch (Exception e) {
      String msg = "";
      if (successKillName.size() > 0) {
        msg = "after success kill topology : " + successKillName.toString()
            + "it exists exception: " + CoreUtil.stringifyError(e);
      } else {
        msg = CoreUtil.stringifyError(e);
      }
      LOG.error(msg);
      throw new Exception(msg);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public static void activateTopology(String topologyName) throws Exception {
    NimbusClient client = getNimbusClient();
    try {
      client.getClient().activate(topologyName);
      String info = "Successfully submit command activate " + topologyName;
      LOG.info(info);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public static void deactivateTopology(String topologyName) throws Exception {
    NimbusClient client = getNimbusClient();
    try {
      client.getClient().deactivate(topologyName);
      String info = "Successfully submit command deactivate " + topologyName;
      LOG.info(info);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public static List<TopologySummary> listTopologies() throws Exception {
    NimbusClient client = getNimbusClient();
    try {
      ClusterSummary clusterInfo = client.getClient().getClusterInfo();
      return clusterInfo.get_topologies();
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public static NimbusClient getNimbusClient() {
    Map conf = Utils.readStormConfig();
    return NimbusClient.getConfiguredClient(conf);
  }

  private static HashMap<String, Integer> parseExecutor(String str) {
    str = org.apache.commons.lang.StringUtils.deleteWhitespace(str);
    HashMap<String, Integer> executor = new HashMap<String, Integer>();
    String[] strs = str.split(",");
    Properties properties = new Properties();
    try {
      for (String s : strs) {
        properties.load(new StringBufferInputStream(s));
        for (final String name : properties.stringPropertyNames()) {
          Integer value = Utils.getInt(properties.getProperty(name));
          executor.put(name, value.intValue());
        }
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    return executor;
  };

  public static Map<String, Object> jsonToMap(JSONObject json)
      throws JSONException {
    Map<String, Object> map = new HashMap<String, Object>();
    if (json == null || json == JSONObject.NULL) {
      return map;
    }
    Iterator<String> keysItr = json.keys();
    while (keysItr.hasNext()) {
      String key = keysItr.next();
      Object value = json.get(key);
      if (value instanceof JSONArray) {
        value = jsonToList((JSONArray) value);
      } else if (value instanceof JSONObject) {
        value = jsonToMap((JSONObject) value);
      }
      map.put(key, value);
    }
    return map;
  }

  public static List<Object> jsonToList(JSONArray array) throws JSONException {
    List<Object> list = new ArrayList<Object>();
    for (int i = 0; i < array.length(); i++) {
      Object value = array.get(i);
      if (value instanceof JSONArray) {
        value = jsonToList((JSONArray) value);
      } else if (value instanceof JSONObject) {
        value = jsonToMap((JSONObject) value);
      }
      list.add(value);
    }
    return list;
  }
}
