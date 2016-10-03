package com.tencent.jstorm.ui.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * methods for crawler restapi
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 3:23:22 PM Apr 6, 2016
 */
public class CrawlerCore {
  public static String CRAWLER_JOBS_DIR = "crawler.jobs.dir";
  public static String SUBMIT_CRAWLER_DIR = "submit.crawler.dir";
  private static String FILE_PATH_SPLITOR = Utils.FILE_PATH_SEPARATOR;
  private static String JOB_ID_ALL = "ALL";
  private static String CRAWL_JOB_NAME_PREFIX = "tdspider-job-";

  private static Logger LOG = LoggerFactory.getLogger(CrawlerCore.class);

  public static void writeJobConfFile(Map<String, String> jobConfMap)
      throws Exception {
    Yaml yaml = new Yaml(new SafeConstructor());
    String fileName = getJobsDirPath(false) + FILE_PATH_SPLITOR
        + jobConfMap.get(ApiCommon.SITE_ID) + ".yaml";
    FileWriter writer = new FileWriter(fileName);
    yaml.dump(assembleJobMap(jobConfMap), writer);
  }

  public static void writeJobConfFile(String siteId, String fileString)
      throws Exception {
    String fileName =
        getJobsDirPath(false) + FILE_PATH_SPLITOR + siteId + ".yaml";
    File file = new File(fileName);
    if (!file.exists()) {
      file.createNewFile();
    }
    FileOutputStream out = new FileOutputStream(file, false);
    out.write(fileString.getBytes("utf-8"));
    out.flush();
    out.close();
  }

  public static void updateJobConfFile(Map<String, String> jobConfMap)
      throws Exception {
    Yaml yaml = new Yaml(new SafeConstructor());
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    File file =
        getFileBySiteId(jobsDir, (String) jobConfMap.get(ApiCommon.SITE_ID));
    Map<String, Object> oldData = readYamlConfigFile(file);
    FileWriter writer = new FileWriter(file.getCanonicalPath());
    Map<String, Object> data = assembleJobMap(jobConfMap);
    oldData.putAll(data);
    yaml.dump(oldData, writer);
  }

  public static void updateJobConfFile(String siteId, String fileString)
      throws Exception {
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    File file = getFileBySiteId(jobsDir, siteId);
    if (!file.exists()) {
      file.createNewFile();
    }
    FileOutputStream out = new FileOutputStream(file, false);
    out.write(fileString.getBytes("utf-8"));
    out.flush();
    out.close();
  }

  public static void deleteJobConfFile(String jobIds) throws Exception {
    String[] jobIdArr = jobIds.split(ApiCommon.ARGS_SPLITOR);
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    for (String jobId : jobIdArr) {
      File file = getFileBySiteId(jobsDir, jobId);
      if (file != null) {
        FileUtils.deleteQuietly(file);
      }
    }
  }

  private static File getFileBySiteId(File jobsDir, String siteId)
      throws IOException, TException {
    if (!jobsDir.exists()) {
      return null;
    }
    for (File tmpFile : jobsDir.listFiles()) {
      if (tmpFile.isDirectory()) {
        File subDir = new File(tmpFile.getCanonicalPath());
        for (File subFile : subDir.listFiles()) {
          if (!subFile.getName().endsWith(".yaml")) {
            continue;
          }
          String tmpSiteId =
              subFile.getName().substring(0, subFile.getName().length() - 5);
          if (tmpSiteId.equals(siteId)) {
            return subFile;
          }
        }
      } else {
        if (!tmpFile.getName().endsWith(".yaml")) {
          continue;
        }
        String tmpSiteId =
            tmpFile.getName().substring(0, tmpFile.getName().length() - 5);
        if (tmpSiteId.equals(siteId)) {
          return tmpFile;
        }
      }
    }
    return null;
  }

  public static List<Map<String, Object>> getJobConfFile(String siteId)
      throws Exception {
    List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    if (!jobsDir.exists()) {
      throw new TException(
          "jobs dir [" + jobsDirPath + "] is not exists, can not getjob file");
    }
    if (JOB_ID_ALL.equals(siteId)) {
      for (File tmpFile : jobsDir.listFiles()) {
        if (tmpFile.isDirectory()) {
          File subDir = new File(tmpFile.getCanonicalPath());
          for (File subFile : subDir.listFiles()) {
            Map<String, Object> jobMap =
                readYamlConfigFile(new File(subFile.getCanonicalPath()));
            ret.add(jobMap);
          }
        } else {
          Map<String, Object> jobMap =
              readYamlConfigFile(new File(tmpFile.getCanonicalPath()));
          ret.add(jobMap);
        }
      }
    } else {
      String[] siteIdArr = siteId.split(ApiCommon.ARGS_SPLITOR);
      for (String tmpSiteId : siteIdArr) {
        File tmpFile = getFileBySiteId(jobsDir, tmpSiteId);
        Map<String, Object> jobMap = readYamlConfigFile(tmpFile);
        ret.add(jobMap);
      }
    }
    if (ret.size() > 1) {
      Collections.sort(ret, new Comparator<Map<String, Object>>() {
        @Override
        public int compare(Map<String, Object> o1, Map<String, Object> o2) {
          int siteId1 = Integer.valueOf(String.valueOf(o1.get("site.id")));
          int siteId2 = Integer.valueOf(String.valueOf(o2.get("site.id")));
          return siteId2 - siteId1;
        }
      });
    }
    return ret;
  }

  public static String getJobConfFileString(String siteId) throws Exception {
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    if (!jobsDir.exists()) {
      throw new TException(
          "jobs dir [" + jobsDirPath + "] is not exists, can not getjob file");
    }
    StringBuffer sb = new StringBuffer();
    String tempstr = null;
    File siteFile = getFileBySiteId(jobsDir, siteId);
    FileInputStream fis = new FileInputStream(siteFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    while ((tempstr = br.readLine()) != null) {
      sb.append(tempstr).append("\n");
    }
    return sb.toString();
  }

  public static List<String> getAllUserJobsId() throws Exception {
    List<String> ret = new ArrayList<String>();
    String jobsDirPath = getJobsDirPath(true);
    File jobsDir = new File(jobsDirPath);
    for (File tmpFile : jobsDir.listFiles()) {
      if (tmpFile.isDirectory()) {
        File subDir = new File(tmpFile.getCanonicalPath());
        for (File subFile : subDir.listFiles()) {
          if (!subFile.getName().endsWith(".yaml")) {
            continue;
          }
          String siteId =
              subFile.getName().substring(0, subFile.getName().length() - 5);
          ret.add(siteId);
        }
      } else {
        if (!tmpFile.getName().endsWith(".yaml")) {
          continue;
        }
        String siteId =
            tmpFile.getName().substring(0, tmpFile.getName().length() - 5);
        ret.add(siteId);
      }
    }
    Collections.sort(ret, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int siteId1 = Integer.valueOf(o1);
        int siteId2 = Integer.valueOf(o2);
        return siteId2 - siteId1;
      }
    });
    return ret;
  }

  public static List<Map<String, String>> getAllJobsSummary() throws Exception {
    List<Map<String, String>> ret = new ArrayList<Map<String, String>>();
    List<String> jobIds = getAllUserJobsId();
    NimbusClient nimbus = Core.withNimbus();
    try {
      List<TopologySummary> topologies =
          nimbus.getClient().getClusterInfo().get_topologies();
      for (String jobId : jobIds) {
        Map<String, String> tmp = new HashMap<String, String>();
        tmp.put("site-id", jobId);
        TopologySummary topology = getTopologyNameBySiteId(topologies, jobId);
        if (topology != null) {
          tmp.put("topology-name", topology.get_name());
          tmp.put("topology-id", CoreUtil.urlEncode(topology.get_id()));
        } else {
          tmp.put("topology-name", "");
          tmp.put("topology-id", "");
        }
        ret.add(tmp);
      }
      return ret;
    } finally {
      nimbus.close();
    }
  }

  private static TopologySummary getTopologyNameBySiteId(
      List<TopologySummary> topologies, String siteId) throws Exception {
    String crawlTopologyName = CRAWL_JOB_NAME_PREFIX + siteId;
    for (TopologySummary topology : topologies) {
      if (crawlTopologyName.equals(topology.get_name())) {
        return topology;
      }
    }
    return null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void submitTopologyForRestApi(String uploadJarLocation,
      String mainClass, String siteId, String args) throws Exception {
    try {
      String[] argsArr = translateArgs(args, uploadJarLocation, siteId);
      File file = new File(uploadJarLocation);
      if (!file.exists()) {
        throw new Exception(
            "Jar file[path: " + uploadJarLocation + "] not exits! ");
      }
      URL jarurl = new URL("file:" + uploadJarLocation);
      ClassLoader classLoader = new URLClassLoader(new URL[] { jarurl });
      Thread.currentThread().setContextClassLoader(classLoader);
      mainClass = mainClass.trim();
      Class clazz = classLoader.loadClass(mainClass);
      Method mainMethod = clazz.getDeclaredMethod("main", String[].class);
      System.setProperty("storm.jar", uploadJarLocation);
      LOG.info("submit topology  jarPath '{}' mainClass '{}' args '{}' ",
          uploadJarLocation, mainClass, args);
      mainMethod.invoke(clazz.newInstance(), (Object) argsArr);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      throw new Exception(e);
    }
  }

  private static String[] translateArgs(String args, String uploadJarLocation,
      String siteId) throws Exception {
    String[] argsArr = args.split(ApiCommon.ARGS_SPLITOR);
    List<String> argsList = new ArrayList<String>();
    int count = 0;
    for (String tmpArgs : argsArr) {
      tmpArgs = tmpArgs.trim();
      if (tmpArgs.startsWith("-") && tmpArgs.indexOf(" ") > -1) {
        String[] tmpArgsArr = tmpArgs.split(" +");
        argsList.add(tmpArgsArr[0].trim());
        if (count == 1) {
          argsList.add(getJobFilePath(siteId));
        } else {
          argsList.add(tmpArgsArr[1].trim());
        }
      } else {
        if (count == 1) {
          argsList.add(getJobFilePath(siteId));
        } else {
          argsList.add(tmpArgs.trim());
        }
      }
      count++;
    }
    // if arg is a file parameter, then use file path
    File file = new File(uploadJarLocation);
    File paramFileDir = new File(file.getCanonicalFile() + "-param");
    if (paramFileDir.exists()) {
      for (File paramFile : paramFileDir.listFiles()) {
        if (paramFile.isDirectory()) {
          continue;
        }
        for (int i = 0; i < argsList.size(); i++) {
          if (argsList.get(i).equals(paramFile.getName())) {
            argsList.set(i, paramFile.getCanonicalPath());
          }
        }
      }
    }
    String[] retArgsArr = new String[argsList.size()];
    retArgsArr = argsList.toArray(retArgsArr);
    return retArgsArr;
  }

  private static String getJobFilePath(String siteId) throws Exception {
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    File jobFile = getFileBySiteId(jobsDir, siteId);
    if (!jobFile.exists()) {
      throw new TException(
          "can not get tdspider job file by site.id: " + siteId);
    }
    return jobFile.getCanonicalPath();
  }

  /**
   * 如果是List类型，才需要在JobFile中增加定义，如果是普通类型，直接增加传参即可实现写入
   * 
   * @param jobConfig
   * @return
   */
  private static Map<String, Object> assembleJobMap(
      Map<String, String> jobConfig) {
    Map<String, Object> ret = new HashMap<String, Object>();
    Field[] fields = JobFile.class.getDeclaredFields();
    for (Map.Entry<String, String> entry : jobConfig.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      Class type = getValueTypeByKey(fields, key);
      if (type == String.class) {
        ret.put(key, value);
      } else if (type == Boolean.class) {
        ret.put(key, Boolean.valueOf(value));
      } else if (type == Integer.class) {
        ret.put(key, Integer.valueOf(value));
      } else if (type == List.class) {
        String[] fieldValueArr = value.split("\\,");
        List<String> fieldValueList = Arrays.asList(fieldValueArr);
        ret.put(key, fieldValueList);
      }
    }
    return ret;
  }

  private static Class getValueTypeByKey(Field[] fields, String key) {
    for (Field field : fields) {
      JobElement jobElement = field.getAnnotation(JobElement.class);
      if (jobElement == null) {
        continue;
      }
      if (key.equals(jobElement.key())) {
        return jobElement.type();
      }
    }
    return String.class;
  }

  private static String getJobsConfDir() throws TException {
    String crawlerJobsDir =
        Utils.getString(Core.STORM_CONF.get(CRAWLER_JOBS_DIR),
            System.getProperty("tdspider.local.dir") + Utils.FILE_PATH_SEPARATOR
                + "jobs");
    if (crawlerJobsDir == null || "".equals(crawlerJobsDir)) {
      throw new TException(
          "'crawler.job.dir' is empty, please config it in storm.yaml");
    }
    return crawlerJobsDir;
  }

  private static String getJobsDirPath(boolean mustExists)
      throws TException, IOException {
    String jobsDirPath = getJobsConfDir();
    File jobsDir = new File(jobsDirPath);
    if (!jobsDir.exists()) {
      if (mustExists) {
        throw new TException(
            "jobs dir path [" + jobsDirPath + "] is not exists!");
      }
      FileUtils.forceMkdir(jobsDir);
    }
    return jobsDir.getCanonicalPath();
  }

  /**
   * get crawler jar urls
   * 
   * @return
   * @throws TException
   * @throws IOException
   */
  private static String getCrawlerJarPath() throws TException, IOException {
    String stormHome = System.getProperty("storm.home");
    String crawlerJarDirPath = stormHome + FILE_PATH_SPLITOR + "external"
        + FILE_PATH_SPLITOR + "storm-crawler";
    File jobsDir = new File(crawlerJarDirPath);
    if (!jobsDir.exists()) {
      throw new TException(
          "crawler jar path [" + crawlerJarDirPath + "]  is not exists!");
    }
    String crawlerJarPath = "";
    for (File file : jobsDir.listFiles()) {
      if (!file.isDirectory() && file.getName().startsWith("storm-crawler-")
          && file.getName().endsWith(".jar")) {
        crawlerJarPath = file.getCanonicalPath();
        break;
      }
    }
    if ("".equals(crawlerJarPath)) {
      throw new TException("crawler jar path [" + crawlerJarDirPath
          + "]  has not jar name start with 'storm-crawler'");
    } else {
      return crawlerJarPath;
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> readYamlConfigFile(File file) {
    if (!file.exists()) {
      return new HashMap<String, Object>();
    }
    try {
      Yaml yaml = new Yaml(new SafeConstructor());
      Map<String, Object> ret = null;
      InputStream input = new FileInputStream(file);
      try {
        ret = (Map<String, Object>) yaml.load(new InputStreamReader(input));
      } finally {
        input.close();
      }
      if (ret == null)
        ret = new HashMap<String, Object>();

      return new HashMap<String, Object>(ret);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String getTableNameBySiteId(String siteId) throws Exception {
    List<Map<String, Object>> jobConfs = getJobConfFile(siteId);
    if (jobConfs.size() < 1) {
      throw new TException("can not find job file by site.id :" + siteId);
    }
    return (String) jobConfs.get(0).get("persistent.table");
  }

  public static String getCrawlerResultByDrpc(String funcName, String param)
      throws Exception {
    DRPCClient client = DRPCClient.getConfiguredClientAs(Core.STORM_CONF);
    try {
      return client.execute(funcName, param);
    } catch (Exception t) {
      throw t;
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  private static String getSubmitCrawlerDir() throws TException {
    String submitCrawlerDir =
        Utils.getString(Core.STORM_CONF.get(SUBMIT_CRAWLER_DIR), "");
    if (StringUtils.isEmpty(submitCrawlerDir)) {
      throw new TException(
          "'submit.crawler.dir' is empty, please config it in storm.yaml");
    }
    return submitCrawlerDir;
  }

  private static String getSubmitCrawlerDirPath(boolean mustExists)
      throws TException, IOException {
    String submitCrawlerDir = getSubmitCrawlerDir();
    File jobsDir = new File(submitCrawlerDir);
    if (!jobsDir.exists()) {
      if (mustExists) {
        throw new TException("submit crawler dir path [" + submitCrawlerDir
            + "] is not exists!");
      }
      FileUtils.forceMkdir(jobsDir);
    }
    return jobsDir.getCanonicalPath();
  }

  public static Map<String, String> submitCralwerJob(String jarType,
      String siteId, String jarName) throws Exception {
    String siteFilePath = getJobFilePath(siteId);
    File siteFile = new File(siteFilePath);
    File topologyJarDir = Core.getTopologyJarDir(jarType);
    String submitCrawlerDirPath = getSubmitCrawlerDirPath(false);
    File submitCrawlerJarFile =
        new File(submitCrawlerDirPath + Utils.FILE_PATH_SEPARATOR + siteId
            + Utils.FILE_PATH_SEPARATOR + siteId + ".jar");
    if (StringUtils.isNotEmpty(jarName)) {
      String jarLocation = topologyJarDir.getCanonicalPath()
          + Utils.FILE_PATH_SEPARATOR + Core.SITE_JOB_DIR_PREFIX + siteId
          + Utils.FILE_PATH_SEPARATOR + jarName;
      File jarFile = new File(jarLocation);
      FileUtils.copyFile(jarFile, submitCrawlerJarFile);
    }
    File submitCrawlerJobFile =
        new File(submitCrawlerDirPath + Utils.FILE_PATH_SEPARATOR + siteId
            + Utils.FILE_PATH_SEPARATOR + siteId + ".yaml");
    FileUtils.copyFile(siteFile, submitCrawlerJobFile);
    String[] cmd = { "start-user-crawler.sh", "dis_site", siteId };
    Process process = Runtime.getRuntime().exec(cmd);
    Map<String, String> ret = new HashMap<String, String>();
    ret.put("errorMsg", getStringByInputStream(process.getErrorStream()));
    ret.put("inputMsg", getStringByInputStream(process.getInputStream()));
    return ret;
  }

  private static String getStringByInputStream(InputStream inputStream) {
    BufferedReader in = null;
    String str = null;
    try {
      in = new BufferedReader(new InputStreamReader(inputStream));
      while ((str = in.readLine()) != null) {
        str += str.trim() + "\n";
      }
    } catch (IOException e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return str;
  }

  public static void deleteSubmitCralwerFile(String siteId)
      throws TException, IOException {
    String submitCrawlerDirPath = getSubmitCrawlerDirPath(false);
    File submitCrawlerJarFile =
        new File(submitCrawlerDirPath + Utils.FILE_PATH_SEPARATOR + siteId);
    FileUtils.deleteDirectory(submitCrawlerJarFile);
  }
}
