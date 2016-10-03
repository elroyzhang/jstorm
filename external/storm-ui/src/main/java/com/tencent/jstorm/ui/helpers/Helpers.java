package com.tencent.jstorm.ui.helpers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.StormCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.tencent.jstorm.utils.CoreUtil;
import com.google.common.collect.Lists;
import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.ui.helpers")
public class Helpers {
  private static final Logger LOG = LoggerFactory.getLogger(Helpers.class);

  private static final String STORM_WEB_CONF_NAME = "STORM_CONF_DIR";

  private static final String DEFAULT_STORM_WEB_CONF_PATH;

  static {
    DEFAULT_STORM_WEB_CONF_PATH = System.getProperty("user.home") + "/.storm";
  }

  @ClojureClass(className = "backtype.storm.ui.helpers#PRETTY-SEC-DIVIDERS")
  private static final String[][] PRETTY_SEC_DIVIDERS =
      { new String[] { "s", "60" }, new String[] { "m", "60" },
          new String[] { "h", "24" }, new String[] { "d", null } };

  @ClojureClass(className = "backtype.storm.ui.helpers#PRETTY-MS-DIVIDERS")
  private static final String[][] PRETTY_MS_DIVIDERS =
      { new String[] { "ms", "1000" }, new String[] { "s", "60" },
          new String[] { "m", "60" }, new String[] { "h", "24" },
          new String[] { "d", null } };

  /**
   * Pretty UptimeSec
   * 
   * @param secs
   * @return pretty uptime sec
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#pretty-uptime-sec")
  public static String prettyUptimeSec(int secs) {
    return prettyUptimeStr(secs, PRETTY_SEC_DIVIDERS);
  }

  /**
   * Pretty UptimeMs
   * 
   * @param secs
   * @return pretty uptime ms
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#pretty-uptime-ms")
  public static String prettyUptimeMs(int secs) {
    return prettyUptimeStr(secs, PRETTY_MS_DIVIDERS);
  }

  /**
   * Pretty Uptime to String
   * 
   * @param secs
   * @param dividers
   * @return pretty uptime string
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#pretty-uptime-str*")
  private static String prettyUptimeStr(int secs, String[][] dividers) {
    int diversize = dividers.length;

    List<String> tmp = new ArrayList<String>();
    int div = secs;
    for (int i = 0; i < diversize; i++) {
      if (dividers[i][1] != null) {
        Integer d = Integer.parseInt(dividers[i][1]);
        tmp.add(div % d + dividers[i][0]);
        div = div / d;
      } else {
        tmp.add(div + dividers[i][0]);
      }
    }

    StringBuilder rtn = new StringBuilder();
    rtn.append("");
    int tmpSzie = tmp.size();
    for (int j = tmpSzie - 1; j > -1; j--) {
      rtn.append(tmp.get(j));
    }
    return rtn.toString();
  }

  /**
   * Url Format
   * 
   * @param fmt
   * @param args
   * @return url format
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#url-format")
  public static String urlFormat(String fmt, Object... args) {
    if (fmt == null || args == null) {
      return null;
    }

    String[] ret = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      ret[i] = CoreUtil.urlEncode(String.valueOf(args[i]));
    }
    return String.format(fmt, ret);
  }

  /**
   * Change ExecutorInfo to Task ids
   * 
   * @param e
   * @return task ids
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#to-tasks")
  public static List<Integer> toTasks(ExecutorInfo e) {
    List<Long> list = Lists.newArrayList(Long.valueOf(e.get_task_start()), Long.valueOf(e.get_task_end()));
    return StormCommon.executorIdToTasks(list);
  }

  /**
   * Sum tasks num
   * 
   * @param executors
   * @return task sum num
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#sum-tasks")
  public static int sumTasks(List<ExecutorSummary> executors) {
    int tasksNum = 0;
    for (ExecutorSummary e : executors) {
      ExecutorInfo executorInfo = e.get_executor_info();
      List<Integer> tasks = toTasks(executorInfo);
      tasksNum += tasks.size();
    }
    return tasksNum;
  }

  /**
   * Pretty ExecutorInfo
   * 
   * @param e
   * @return string executorinfo eg: [1, 3]
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#pretty-executor-info")
  public static String prettyExecutorInfo(ExecutorInfo e) {
    return "[" + e.get_task_start() + "-" + e.get_task_end() + "]";
  }

  /**
   * Format Float Value to String
   * 
   * @param n
   * @return float str
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#float-str")
  public static String floatStr(Float n) {
    if (n != null) {
      return String.format("%.3f", n);
    }
    return "0";

  }

  /**
   * swapMapOrder
   * 
   * @param m
   * @return map
   */
  @ClojureClass(className = "backtype.storm.ui.helpers#swap-map-order")
  public static <K2, V2, K> Map<K2, Map<K, V2>> swapMapOrder(
      Map<K, Map<K2, V2>> m) {
    Map<K2, Map<K, V2>> res = new HashMap<K2, Map<K, V2>>();
    for (Map.Entry<K, Map<K2, V2>> entry : m.entrySet()) {
      K k = entry.getKey();
      Map<K2, V2> value = entry.getValue();
      for (Map.Entry<K2, V2> e : value.entrySet()) {
        K2 k2 = e.getKey();
        V2 v2 = e.getValue();
        if (res.containsKey(k2)) {
          res.get(k2).put(k, v2);
        } else {
          Map<K, V2> tmp = new HashMap<K, V2>();
          tmp.put(k, v2);
          res.put(k2, tmp);
        }
      }
    }
    return res;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Map readStormConfig() {
    Map conf = new HashMap();//Utils.readStormConfig();

    String stormConfigDir = System.getenv(STORM_WEB_CONF_NAME);
    String defFile = StringUtils.isNotBlank(stormConfigDir)
        ? (stormConfigDir + "/defaults.yaml")
        : DEFAULT_STORM_WEB_CONF_PATH + "/defaults.yaml";
    if (!Utils.checkFileExists(defFile)) {
      LOG.error("Can't find default configuration file!");
    }
    Map<String, Object> defaultMap = loadRaw(defFile);
    conf.putAll(defaultMap);
    String confFile = StringUtils.isNotBlank(stormConfigDir)
        ? (stormConfigDir + "/storm.yaml")
        : DEFAULT_STORM_WEB_CONF_PATH + "/storm.yaml";

    if (!Utils.checkFileExists(confFile)) {
      LOG.error("Can't find storm configuration file!");
    }
    Map<String, Object> storm = loadRaw(confFile);
    conf.putAll(storm);
    ConfigValidation.validateFields(conf);
    return conf;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> loadRaw(String file) {
    Map<String, Object> result = new HashMap<String, Object>();
    if (file == null) {
      return result;
    }
    Yaml yaml = new Yaml();
    InputStream in;
    try {
      in = new FileInputStream(new File(file));
      Object obj = yaml.load(in);
      if (!(obj instanceof Map)) {
        return Collections.<String, Object> emptyMap();
      }
      result = (Map<String, Object>) obj;
    } catch (FileNotFoundException e) {
      LOG.error("Invalid file.", e);
    }
    return result;
  }
}
