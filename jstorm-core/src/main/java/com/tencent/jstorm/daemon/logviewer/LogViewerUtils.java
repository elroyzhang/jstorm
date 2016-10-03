package com.tencent.jstorm.daemon.logviewer;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.daemon.DirectoryCleaner;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.ui.InvalidRequestException;
import org.apache.storm.ui.UIHelpers;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.logviewer.api.LogviewerServlet;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 2:50:54 PM Jul 7, 2016
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class LogViewerUtils {

  private static Logger LOG = LoggerFactory.getLogger(LogViewerUtils.class);
  public static Meter logviewerNumWebRequest =
      StormMetricsRegistry.registerMeter("logviewer-num-web-requests");

  public static final String PAGE_NOT_FOUND = "Page not found";
  public static final Map STORM_CONF = Utils.readStormConfig();

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#http-creds-handler")
  public static IHttpCredentialsPlugin httpcredshandler =
      AuthUtils.GetUiHttpCredentialsPlugin(STORM_CONF);

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#cleanup-cutoff-age-millis")
  public static Long cleanupCutoffAgeMillis(Map conf, long nowMillis)
      throws Exception {
    return nowMillis
        - (Utils.getInt(conf.get(Config.LOGVIEWER_CLEANUP_AGE_MINS), 10080) * 60
            * 1000);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-stream-for-dir")
  public static DirectoryStream<Path> getStreamForDir(File f) throws Exception {
    try {
      return Files.newDirectoryStream(f.toPath());
    } catch (Exception ex) {
      LOG.error(CoreUtil.stringifyError(ex));
    }
    return null;
  }

  /**
   * Return the last modified time for all log files in a worker's log dir.
   * Using stream rather than File.listFiles is to avoid large mem usage when a
   * directory has too many files"
   * 
   * @param f
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#last-modifiedtime-worker-logdir")
  public static Long lastModifiedtimeWorkerLogdir(File logDir)
      throws Exception {
    DirectoryStream<Path> stream = getStreamForDir(logDir);
    long dirModified = logDir.lastModified();
    try {
      long maximum = 0;
      while (stream.iterator().hasNext()) {
        Path path = stream.iterator().next();
        long curr = path.toFile().lastModified();
        if (curr > maximum) {
          maximum = curr;
        }
      }
      return maximum + dirModified;
    } catch (Exception ex) {
      LOG.error(CoreUtil.stringifyError(ex));
      return dirModified;
    } finally {
      if (stream instanceof DirectoryStream) {
        stream.close();
      }
    }
  }

  /**
   * Return the sum of lengths for all log files in a worker's log dir. Using
   * stream rather than File.listFiles is to avoid large mem usage when a
   * directory has too many files
   * 
   * @param f
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-size-for-logdir")
  public static Long getSizeForLogdir(File logDir) throws Exception {
    DirectoryStream<Path> stream = getStreamForDir(logDir);
    long sum = 0;
    try {
      while (stream.iterator().hasNext()) {
        Path path = stream.iterator().next();
        sum += path.toFile().length();
      }
    } catch (Exception ex) {
      LOG.error(CoreUtil.stringifyError(ex));
    } finally {
      if (stream instanceof DirectoryStream) {
        stream.close();
      }
    }
    return sum;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#mk-FileFilter-for-log-cleanup")
  public static boolean mkFileFilterForLogCleanup(Map conf, Long nowMillis,
      File file) throws Exception {
    Long cutoffAgeMillis = cleanupCutoffAgeMillis(conf, nowMillis);
    if (!file.isFile()
        && lastModifiedtimeWorkerLogdir(file) <= cutoffAgeMillis) {
      return true;
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#select-dirs-for-cleanup")
  public static List<File> selectDirsForCleanup(Map conf, Long nowMillis,
      String rootDir) throws Exception {
    List<File> cleanupDirs = new ArrayList<File>();
    File rootDirFile = new File(rootDir);
    for (File file : rootDirFile.listFiles()) {
      if (mkFileFilterForLogCleanup(conf, nowMillis, file)) {
        cleanupDirs.add(file);
      }
    }
    return cleanupDirs;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-topo-port-workerlog")
  public static String getTopoPortWorkerlog(File file) throws Exception {
    return getFnameLastNumberPath(file.getCanonicalPath(), 3);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-topo-port-workerlog")
  public static String getFnameLastNumberPath(String fname, int number)
      throws Exception {
    String[] filePath = fname.split(Utils.FILE_PATH_SEPARATOR);
    if (filePath.length < number) {
      return "";
    }
    String ret = "";
    while (number > 0) {
      if (number == 1) {
        ret += filePath[filePath.length - number];
      } else {
        ret += filePath[filePath.length - number] + Utils.FILE_PATH_SEPARATOR;
      }

      number--;
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-metadata-file-for-log-root-name")
  public static File getMetadataFileForLogRootName(String rootName,
      String rootDir) throws Exception {
    File metaFile = new File(rootDir + Utils.FILE_PATH_SEPARATOR + "metadata",
        rootName + ".yaml");
    if (metaFile.exists()) {
      return metaFile;
    } else {
      LOG.warn("Could not find " + metaFile.getCanonicalPath()
          + " to clean up for " + rootName);
      return null;
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-metadata-file-for-wroker-logdir")
  public static File getMetadataFileForWorkerLogdir(String logDir)
      throws Exception {
    File metaFile = new File(logDir, "worker.yaml");
    if (metaFile.exists()) {
      return metaFile;
    } else {
      LOG.warn("Could not find " + metaFile.getCanonicalPath()
          + " to clean up for " + logDir);
      return null;
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-worker-id-from-metadata-file")
  public static String getWorkerIdMetaDataFile(File metaFile) throws Exception {
    Map yaml = (Map) Utils.readYamlFile(metaFile.getCanonicalPath());
    if (yaml != null) {
      return (String) yaml.get("worker-id");
    }
    return "";
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-topo-owner-from-metadata-file")
  public static String getTopoOwnerFromMetadataFile(File metaFile)
      throws Exception {
    Map yaml = (Map) Utils.readYamlFile(metaFile.getCanonicalPath());
    if (yaml != null) {
      return (String) yaml.get(Config.TOPOLOGY_SUBMITTER_USER);
    }
    return "";
  }

  /**
   * return the workerid to worker-log-dir map
   * 
   * @param metaFile
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#identify-worker-log-dirs")
  public static Map<String, File> identifyWorkerLogDirs(List<File> logDirs)
      throws Exception {
    Map<String, File> id2dir = new HashMap<String, File>();
    for (File logDir : logDirs) {
      File metaFile = getMetadataFileForWorkerLogdir(logDir.getCanonicalPath());
      if (metaFile != null) {
        String workerId = getWorkerIdMetaDataFile(metaFile);
        id2dir.put(workerId, logDir);
      } else {
        id2dir.put("", logDir);
      }
    }
    return id2dir;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-alive-ids")
  public static List<String> getAliveIds(Map conf, Long nowSecs)
      throws Exception {
    List<String> aliveWorkerIds = new ArrayList<String>();
    Map<String, LSWorkerHeartbeat> workerHeartBeats =
        SupervisorUtils.readWorkerHeartbeats(conf);
    for (Map.Entry<String, LSWorkerHeartbeat> entry : workerHeartBeats
        .entrySet()) {
      LSWorkerHeartbeat workerHeartBeat = entry.getValue();
      if (StringUtils.isEmpty(workerHeartBeat.get_topology_id())
          || SupervisorUtils.isWorkerHbTimedOut(nowSecs.intValue(),
              workerHeartBeat, conf)) {
        continue;
      }
      aliveWorkerIds.add(entry.getKey());
    }
    return aliveWorkerIds;
  }

  /**
   * Return a sorted set of java.io.Files that were written by workers that are
   * now dead
   * 
   * @param conf
   * @param nowSecs
   * @param logDirs
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-dead-worker-dirs")
  public static List<File> getDeadWorkerDirs(Map conf, Long nowSecs,
      List<File> logDirs) throws Exception {
    List<File> deadDirs = new ArrayList<File>();
    if (logDirs != null && !logDirs.isEmpty()) {
      List<String> aliveIds = getAliveIds(conf, nowSecs);
      Map<String, File> id2dir = identifyWorkerLogDirs(logDirs);
      for (Map.Entry<String, File> entry : id2dir.entrySet()) {
        if (!aliveIds.contains(entry.getKey())) {
          deadDirs.add(entry.getValue());
        }
      }
    }
    return deadDirs;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-all-worker-dirs")
  public static List<File> getAllWorkerDirs(File rootDir) throws Exception {
    List<File> workerDirs = new ArrayList<File>();
    for (File topoFile : rootDir.listFiles()) {
      workerDirs.addAll(Arrays.asList(topoFile.listFiles()));
    }
    return workerDirs;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-alive-worker-dirs")
  public static List<String> getAliveWorkerDirs(Map conf, File rootDir)
      throws Exception {
    List<String> workerDirs = new ArrayList<String>();
    List<String> aliveIds = getAliveIds(conf, (long) Time.currentTimeSecs());
    List<File> logDirs = getAllWorkerDirs(rootDir);
    Map<String, File> id2dir = identifyWorkerLogDirs(logDirs);
    for (Map.Entry<String, File> entry : id2dir.entrySet()) {
      if (aliveIds.contains(entry.getKey())) {
        workerDirs.add(entry.getValue().getCanonicalPath());
      }
    }
    return workerDirs;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-all-logs-for-rootdir")
  public static List<File> getAllLogsForRootdir(File logDir) throws Exception {
    List<File> logs = new ArrayList<File>();
    for (File portDir : getAllWorkerDirs(logDir)) {
      logs.addAll(DirectoryCleaner.getFilesForDir(portDir));
    }
    return logs;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#is-active-log")
  public static boolean isActiveLog(File file) throws Exception {
    Pattern p = Pattern.compile("\\.(log|err|out|current|yaml|pid)$");
    Matcher m = p.matcher(file.getName());
    if (m.find()) {
      return true;
    }
    return false;
  }

  /**
   * Given a sequence of Files, sum their sizes.
   * 
   * @param files
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#sum-file-size")
  public static Long sumFileSize(List<File> files) throws Exception {
    Long sumFileSize = 0L;
    for (File file : files) {
      sumFileSize += file.length();
    }
    return sumFileSize;
  }

  /**
   * Delete the oldest files in each overloaded worker log dir
   * 
   * @param rootDir
   * @param size
   * @param cleaner
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#per-workerdir-cleanup!")
  public static void perWorkerdirCleanup(File rootDir, Long size,
      DirectoryCleaner cleaner) throws Exception {
    for (File workerDir : getAllWorkerDirs(rootDir)) {
      cleaner.deleteOldestWhileTooLarge(Arrays.asList(workerDir), size, true,
          null);
    }
  }

  /**
   * Delete the oldest files in overloaded worker-artifacts globally
   * 
   * @param rootDir
   * @param size
   * @param cleaner
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#global-log-cleanup!")
  public static void globalLogCleanup(File rootDir, Long size,
      DirectoryCleaner cleaner) throws Exception {
    List workerDirs = getAllWorkerDirs(rootDir);
    Set<String> aliveWorkerDirs =
        new HashSet<String>(getAliveWorkerDirs(STORM_CONF, rootDir));
    cleaner.deleteOldestWhileTooLarge(workerDirs, size, false, aliveWorkerDirs);
  }

  /**
   * Delete the topo dir if it contains zero port dirs
   * 
   * @param dir
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#cleanup-empty-topodir!")
  public static void cleanupEmptyTopodir(File dir) throws Exception {
    File topodir = dir.getParentFile();
    if (topodir.listFiles() == null || topodir.listFiles().length == 0) {
      Utils.forceDelete(topodir.getCanonicalPath());
    }
  }

  /**
   * Delete old log dirs for which the workers are no longer alive
   * 
   * @param logRootDir
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#cleanup-fn!")
  public static void cleanupFn(String logRootDir) throws Exception {
    Long nowSecs = (long) Time.currentTimeSecs();
    List<File> oldLogDirs =
        selectDirsForCleanup(STORM_CONF, nowSecs * 1000, logRootDir);
    int totalSize = Utils.getInt(
        STORM_CONF.get(Config.LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB), 4096);
    int perDirSize = Utils.getInt(
        STORM_CONF.get(Config.LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB), 2048);
    perDirSize = (int) Math.min(perDirSize, totalSize * 0.5);
    DirectoryCleaner cleaner = new DirectoryCleaner();
    List<File> deadWorkerDirs =
        getDeadWorkerDirs(STORM_CONF, nowSecs, oldLogDirs);
    String oldLogDirsName = oldLogDirs.toString();
    String deadWorkerDirsName = "";
    LOG.debug("log cleanup: now={} old log dirs={} dead worker dirs={}",
        nowSecs, oldLogDirsName, deadWorkerDirsName);
    for (File dir : deadWorkerDirs) {
      String path = dir.getCanonicalPath();
      LOG.info("Cleaning up: Removing " + path);
      try {
        Utils.forceDelete(path);
        cleanupEmptyTopodir(dir);
      } catch (Exception e) {
        LOG.error(CoreUtil.stringifyError(e));
      }
    }
    perWorkerdirCleanup(new File(logRootDir), (long) perDirSize * 1024 * 1024,
        cleaner);
    long size = totalSize * 1024 * 1024;
    globalLogCleanup(new File(logRootDir), size, cleaner);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#start-log-cleaner!")
  public static void startLogCleaner(Map conf, String logRootDir)
      throws Exception {
    Object intervalSecs = conf.get(Config.LOGVIEWER_CLEANUP_INTERVAL_SECS);
    if (intervalSecs != null) {
      LOG.debug("starting log cleanup thread at interval: {}", intervalSecs);
      StormTimer timer = new StormTimer("logviewer-cleanup",
          new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
              // TODO Auto-generated method stub
              LOG.error(t.getName() + " Error when doing logs cleanup");
              Utils.exitProcess(20, "Error when doing log cleanup");
            }
          });
      timer.scheduleRecurring(0, (int) intervalSecs, new Runnable() {
        @Override
        public void run() {
          // TODO Auto-generated method stub
          try {
            cleanupFn(logRootDir);
          } catch (Exception e) {
            LOG.error(CoreUtil.stringifyError(e));
          }
        }
      });
    }
  }

  /**
   * FileInputStream#skip may not work the first time, so ensure it successfully
   * skips the given number of bytes.
   * 
   * @param stream
   * @param n
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#skip-bytes")
  public static void skipBytes(InputStream stream, Long n) throws Exception {
    long skipped = 0;
    while (skipped < n) {
      skipped = skipped + stream.skip(n - skipped);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#logfile-matches-filter?")
  public static boolean logfileMatchesFilter(String logFileName)
      throws Exception {
    Pattern p = Pattern.compile("worker.log.*");
    Matcher m = p.matcher(logFileName);
    if (m.find()) {
      return true;
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#page-file")
  public static String pageFile(String path, long tail) throws Exception {
    boolean isZipFile = path.endsWith(".gz");
    long flen = 0;
    if (isZipFile) {
      flen = Utils.zipFileSize(new File(path));
    } else {
      flen = new File(path).length();
    }
    long skip = flen - tail;
    return pageFile(path, skip, tail);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#page-file")
  public static String pageFile(String path, long start, long length)
      throws Exception {
    boolean isZipFile = path.endsWith(".gz");
    long flen = 0;
    if (isZipFile) {
      flen = Utils.zipFileSize(new File(path));
    } else {
      flen = new File(path).length();
    }
    InputStream input = null;
    ByteArrayOutputStream output = null;
    try {
      if (isZipFile) {
        input = new GZIPInputStream(new FileInputStream(path));
      } else {
        input = new FileInputStream(path);
      }
      output = new ByteArrayOutputStream();
      if (start >= flen) {
        throw new InvalidRequestException(
            "Cannot start past the end of the file");
      }
      if (start > 0) {
        input.skip(start);
      }
      byte[] buffer = new byte[1024];
      while (output.size() < length) {
        int size =
            input.read(buffer, 0, (int) Math.min(1024, length - output.size()));
        if (size > 0) {
          output.write(buffer, 0, size);
        } else {
          break;
        }
      }
      return output.toString();
    } finally {
      if (input != null) {
        input.close();
      }
      if (output != null) {
        output.close();
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-log-user-group-whitelist")
  public static Map<String, List<String>> getLogUserGroupWhitelist(
      String fname) {
    Map ret = new HashMap<String, List<String>>();
    File wlFile = ConfigUtils.getLogMetaDataFile(fname);
    Map m = Utils.findAndReadConfigFile(wlFile.getName(), false);
    if (!m.isEmpty()) {
      List<String> userWl = m.get(Config.LOGS_USERS) == null
          ? new ArrayList<String>() : (List<String>) m.get(Config.LOGS_USERS);
      List<String> groupWl = m.get(Config.LOGS_GROUPS) == null
          ? new ArrayList<String>() : (List<String>) m.get(Config.LOGS_GROUPS);
      ret.put("user-wl", userWl);
      ret.put("group-wl", groupWl);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#igroup-mapper")
  public static IGroupMappingServiceProvider igroupMapper =
      AuthUtils.GetGroupMappingServiceProviderPlugin(STORM_CONF);

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#user-groups")
  public static Set<String> userGroups(String user) throws IOException {
    if (StringUtils.isEmpty(user)) {
      return new HashSet<String>();
    } else {
      return igroupMapper.getGroups(user);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#authorized-log-user")
  public static boolean authorizedLogUser(String user, String fname, Map conf)
      throws IOException {
    if (StringUtils.isEmpty(user) || StringUtils.isEmpty(fname)
        || getLogUserGroupWhitelist(fname).isEmpty()) {
      return true;
    } else {
      Set<String> groups = userGroups(user);
      Map<String, List<String>> logUserGroup = getLogUserGroupWhitelist(fname);
      List<String> logsUsers = logUserGroup.get("user-wl");
      logsUsers.addAll((List<String>) conf.get(Config.LOGS_USERS));
      logsUsers.addAll((List<String>) conf.get(Config.NIMBUS_ADMINS));
      List<String> logsGroups = logUserGroup.get("group-wl");
      logsGroups.addAll((List<String>) conf.get(Config.LOGS_USERS));
      if (logsUsers.contains(user)) {
        return true;
      }
      for (String group : groups) {
        if (logsGroups.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#log-root-dir")
  public static String logRootDir(String appendName) {
    Appender appender = ((LoggerContext) LogManager.getContext())
        .getConfiguration().getAppender(appendName);
    if (!StringUtils.isEmpty(appendName) && appender != null
        && appender instanceof RollingFileAppender) {
      File appenderFile =
          new File(((RollingFileAppender) appender).getFileName());
      return appenderFile.getParent();
    } else {
      throw new RuntimeException(
          "Log viewer could not find configured appender, or the appender is not a FileAppender. "
              + " Please check that the appender name configured in storm and log4j agree.");
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#to-btn-link")
  public static String toBtnLink(String url, String text, boolean enabled)
      throws Exception {
    String enabledVal = enabled == true ? "enabled" : "disabled";
    String retStr = "<a href=" + new java.net.URI(url) + " class='btn btn-info "
        + enabledVal + "'>" + text + "</a>";
    return retStr;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#search-file-form")
  public static String searchFileForm(String fname, String isDaemon)
      throws Exception {
    StringBuffer retSb =
        new StringBuffer("<div id='divSearch' id='search-box'>");
    retSb.append("Search this file: ");
    retSb.append("<input type='text' id='search'></input>");
    retSb.append("<input type='hidden' id='is-daemon' value='").append(isDaemon)
        .append("'></input>");
    retSb.append("<input type='hidden' id='file' value='").append(fname)
        .append("'></input>");
    retSb.append(
        "<input type='submit' value='Search' onClick='searchFile()'></input>");
    retSb.append("</div>");
    return retSb.toString();
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#log-file-selection-form")
  public static String logFileSelectionForm(List<String> logFiles, String type)
      throws Exception {
    StringBuffer retSb =
        new StringBuffer("<div id='divLogFileSelect' name='" + type + "'>");
    retSb.append("<select id='selFile'>");
    for (String logFile : logFiles) {
      retSb.append("<option id='" + logFile + "'>" + logFile + "</option>");
    }
    retSb.append("</select>");
    retSb.append(
        "<input type='button' value='Switch file' onClick='logFileSelect()'></input>");
    retSb.append("</div>");
    return retSb.toString();
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#pager-links")
  public static String pagerLinks(boolean isDaemon, String fname, long start,
      long length, long fileSize) throws Exception {
    String logLinkPrefix = isDaemon ? "/daemonlog?file=" : "/log?file=";
    long prevStart = Math.max(0, start - length);
    long nextStart =
        fileSize > 0 ? Math.min(Math.max(0, fileSize - length), start + length)
            : start + length;
    StringBuffer retSb = new StringBuffer("<div>");
    retSb.append(
        toBtnLink(logLinkPrefix + fname + "&start=" + 0 + "&length=" + length,
            "First", fileSize > 0))
        .append("&nbsp;&nbsp;&nbsp;&nbsp;");
    retSb
        .append(
            toBtnLink(
                logLinkPrefix + fname + "&start=" + Math.max(0, start - length)
                    + "&length=" + length,
                "Prev", prevStart < start && start > -1))
        .append("&nbsp;&nbsp;&nbsp;&nbsp;");
    retSb
        .append(
            toBtnLink(
                logLinkPrefix + fname + "&start="
                    + Math.min(Math.max(0, fileSize - length), start + length)
                    + "&length=" + length,
                "Next", nextStart > start && start > -1))
        .append("&nbsp;&nbsp;&nbsp;&nbsp;");
    retSb.append(toBtnLink(logLinkPrefix + fname + "&length=" + length, "Last",
        fileSize > 0));
    retSb.append("</div>");
    return retSb.toString();
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#download-link")
  public static String downloadLink(String fname) {
    String url = UIHelpers.urlFormat("/download?file=%s", fname);
    String retStr = "<p><a href='" + url + "'>Download Full File<a></p>";
    return retStr;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#daemon-download-link")
  public static String daemonDownloadLink(String fname) {
    String url = UIHelpers.urlFormat("/daemondownload?file=%s", fname);
    String retStr = "<p><a href='" + url + "'>Download Full File<a></p>";
    return retStr;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#is-text-file")
  public static boolean isTxtFile(String fname) {
    Pattern p = Pattern.compile("\\.(log.*|txt|yaml|pid)$");
    Matcher m = p.matcher(fname);
    if (m.find()) {
      return true;
    }
    return false;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#unauthorized-user-html")
  public static String unauthorizedUserHtml(String user) {
    String retStr = "<h2>User '" + user + "' is not authorized.</h2>";
    return retStr;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#ring-response-from-exception")
  public static void ringResponseFromException(HttpServletResponse resp,
      String errMsg) throws IOException {
    Map ret = new HashMap();
    // ret.put("headers", new HashMap());
    // ret.put("status", 400);
    // ret.put("body", ex.getMessage());
    resp.setStatus(400);
    resp.getWriter().write(errMsg);
    resp.getWriter().flush();
    resp.getWriter().close();
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#default-bytes-per-page")
  public final static Long DEFAULT_BYTES_PER_PAGE = 51200L;

  @ClojureClass(className = "org.apadoubache.storm.daemon.logviewer#log-page")
  public static String logPage(String fname, long start, long length,
      String grep, String user, String rootDir) throws Exception {
    StringBuffer html = new StringBuffer();
    if (StringUtils.isEmpty(user)
        || authorizedLogUser(user, fname, STORM_CONF)) {
      File file = new File(rootDir, fname).getCanonicalFile();
      String path = file.getCanonicalPath();
      boolean zipFile = path.endsWith(".gz");
      File topoDir = file.getParentFile().getParentFile();
      if (file.exists() && new File(rootDir).getCanonicalPath()
          .equals(topoDir.getParentFile().getCanonicalPath())) {
        long fileLength = new File(path).length();
        if (zipFile) {
          fileLength = Utils.zipFileSize(new File(path));
        }
        List<File> logFiles = new ArrayList<File>();
        for (File portDir : topoDir.listFiles()) {
          logFiles.addAll(DirectoryCleaner.getFilesForDir(portDir));
        }
        List<String> filesStr = new ArrayList<String>();
        for (File logFile : logFiles) {
          filesStr.add(getTopoPortWorkerlog(logFile));
        }
        List<String> recoredFilesStr = new ArrayList<String>();
        recoredFilesStr.add(fname);
        for (String str : filesStr) {
          if (!str.equals(fname)) {
            recoredFilesStr.add(str);
          }
        }
        if (length != -1) {
          length = Math.min(10485760, length);
        } else {
          length = DEFAULT_BYTES_PER_PAGE;
        }
        String logString = "";
        if (isTxtFile(fname)) {
          if (start != -1) {
            logString = pageFile(path, start, length);
          } else {
            logString = pageFile(path, length);
          }
        } else {
          logString =
              "This is a binary file and cannot display! You may download the full file.";
        }
        start = start == -1 ? fileLength - length : start;
        if (!StringUtils.isEmpty(grep)) {
          html.append("<pre id='logContent'>");
          String[] logStringArr = logString.split("\n");
          for (String str : logStringArr) {
            if (str.indexOf(grep) > -1) {
              html.append(str).append("\n");
            }
          }
          html.append("</pre>");
        } else {
          String pagerData = "";
          if (isTxtFile(fname)) {
            pagerData = pagerLinks(false, fname, start, length, fileLength);
          }
          html.append(searchFileForm(fname, "no"));
          html.append("<br>");
          html.append(logFileSelectionForm(recoredFilesStr, "log"));
          html.append("<br>");
          html.append(pagerData);
          html.append("<br>");
          html.append(downloadLink(fname));
          html.append("<br>");
          html.append("<pre id='logContent'>" + logString + "</pre>");
          html.append("<br>");
          html.append(pagerData);
        }
      } else {
        html = new StringBuffer(PAGE_NOT_FOUND);
      }
    } else {
      if (getLogUserGroupWhitelist(fname).isEmpty()) {
        html = new StringBuffer(PAGE_NOT_FOUND);
      } else {
        return unauthorizedUserHtml(user);
      }
    }
    return html.toString();
  }

  @ClojureClass(className = "org.apadoubache.storm.daemon.logviewer#daemonlog-page")
  public static String daemonlogPage(String fname, long start, long length,
      String grep, String user, String rootDir) throws Exception {
    StringBuffer html = new StringBuffer();
    File file = new File(rootDir, fname).getCanonicalFile();
    String path = file.getCanonicalPath();
    boolean zipFile = path.endsWith(".gz");
    long fileLength = file.length();

    if (new File(rootDir).getCanonicalPath()
        .equals(file.getParentFile().getCanonicalPath()) && file.exists()) {
      fileLength =
          zipFile ? Utils.zipFileSize(new File(path)) : new File(path).length();
      length =
          length != -1 ? Math.min(10485760, length) : DEFAULT_BYTES_PER_PAGE;
      List<File> logFiles = new ArrayList<File>();
      List<String> filesStr = new ArrayList<String>();
      for (File tmp : new File(rootDir).listFiles()) {
        if (tmp.isFile()) {
          logFiles.add(tmp);
          filesStr.add(tmp.getName());
        }
      }
      List<String> recorderedFilesStr = new ArrayList<String>();
      for (String str : filesStr) {
        if (!str.equals(fname)) {
          recorderedFilesStr.add(str);
        }
      }
      String logString = "";
      if (isTxtFile(fname)) {
        if (start != -1) {
          logString = pageFile(path, start, length);
        } else {
          logString = pageFile(path, length);
        }
      } else {
        logString =
            "This is a binary file and cannot display! You may download the full file.";
      }
      start = start == -1 ? fileLength - length : start;
      if (!StringUtils.isEmpty(grep)) {
        html.append("<pre id='logContent'>");
        String[] logStringArr = logString.split("\n");
        for (String str : logStringArr) {
          if (str.indexOf(grep) > -1) {
            html.append(str).append("\n");
          }
        }
        html.append("</pre>");
      } else {
        String pagerData = "";
        if (isTxtFile(fname)) {
          pagerData = pagerLinks(true, fname, start, length, fileLength);
        }
        html.append(searchFileForm(fname, "yes"));
        html.append("<br>");
        html.append(logFileSelectionForm(recorderedFilesStr, "daemonlog"));
        html.append("<br>");
        html.append(pagerData);
        html.append("<br>");
        html.append(daemonDownloadLink(fname));
        html.append("<br>");
        html.append("<pre id='logContent'>" + logString + "</pre>");
        html.append("<br>");
        html.append(pagerData);
      }
    } else {
      html = new StringBuffer(PAGE_NOT_FOUND);
    }
    return html.toString();
  }

  public static void responseFile(HttpServletResponse resp, File file)
      throws IOException {
    ServletOutputStream out = resp.getOutputStream();
    InputStream in = new FileInputStream(file);
    resp.setContentType("application/octet-stream");
    try {
      byte[] buf = new byte[4096];
      int readLength;
      while (((readLength = in.read(buf)) != -1)) {
        out.write(buf, 0, readLength);
      }
      out.flush();
    } catch (IOException e) {
      LOG.error("down load file error {}", CoreUtil.stringifyError(e));
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  @ClojureClass(className = "org.apadoubache.storm.daemon.logviewer#download-log-file")
  public static void downloadLogFile(String fname, HttpServletRequest req,
      HttpServletResponse resp, String user, String rootDir) throws Exception {
    File file = new File(rootDir, fname).getCanonicalFile();
    if (file.exists()) {
      if (StringUtils.isEmpty((String) STORM_CONF.get(Config.UI_FILTER))
          || authorizedLogUser(user, fname, STORM_CONF)) {
        responseFile(resp, file);
      } else {
        unauthorizedUserHtml(user);
      }
    } else {
      resp.setStatus(404);
      resp.getWriter().write(PAGE_NOT_FOUND);
      resp.getWriter().flush();
      resp.getWriter().close();
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#grep-max-search-size")
  public static final int GREP_MAX_SEARCH_SIZE = 1024;

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#grep-buf-size")
  public static final int GREP_BUF_SIZE = 2048;

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#grep-context-size")
  public static final int GREP_CONTEXT_SIZE = 128;

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#logviewer-port")
  public static final int LOGVIEWER_PORT =
      Utils.getInt(STORM_CONF.get(Config.LOGVIEWER_PORT), 8000);

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#log-template")
  public static void logTemplate(HttpServletRequest req,
      HttpServletResponse resp, String body) throws Exception {
    logTemplate(req, resp, body, null, null);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#url-to-match-centered-in-log-page")
  public static String urlToMatchCenteredInLogPage(byte[] neddle, String fname,
      int offset, int port) throws Exception {
    String host = Utils.localHostname();
    port = LOGVIEWER_PORT;
    fname = Utils.FILE_PATH_SEPARATOR + getFnameLastNumberPath(fname, 3);
    int start = Math.max(0, offset - ((int) (DEFAULT_BYTES_PER_PAGE / 2))
        - ((int) (neddle.length / 2)));
    String url = "http://" + host + ":" + port + "/log?file=" + fname
        + "&start=" + start + "&length=" + DEFAULT_BYTES_PER_PAGE;
    return url;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#url-to-match-centered-in-log-page-daemon-file")
  public static String urlToMatchCenteredInLogPageDaemonFile(byte[] neddle,
      String fname, int offset, int port) throws Exception {
    String host = Utils.localHostname();
    port = LOGVIEWER_PORT;
    fname = Utils.FILE_PATH_SEPARATOR + getFnameLastNumberPath(fname, 1);
    int start = Math.max(0, offset - ((int) (DEFAULT_BYTES_PER_PAGE / 2))
        - ((int) (neddle.length / 2)));
    String url = "http://" + host + ":" + port + "/daemonlog?file=" + fname
        + "&start=" + start + "&length=" + DEFAULT_BYTES_PER_PAGE;
    return url;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#mk-match-data")
  public static Map<String, String> mkMatchData(byte[] neddle,
      ByteBuffer haystack, int haystackOffset, int fileOffset, String fname,
      boolean isDaemon, byte[] beforeBytes, byte[] afterBytes)
          throws Exception {
    Map<String, String> ret = new HashMap<String, String>();
    String url = "";
    if (isDaemon) {
      url = urlToMatchCenteredInLogPageDaemonFile(neddle, fname, fileOffset,
          LOGVIEWER_PORT);
    } else {
      url = urlToMatchCenteredInLogPage(neddle, fname, fileOffset,
          LOGVIEWER_PORT);
    }
    byte[] haystackBytes = haystack.array();
    String beforeString = "";
    if (haystackOffset >= GREP_CONTEXT_SIZE) {
      beforeString = new String(haystackBytes,
          haystackOffset - GREP_CONTEXT_SIZE, GREP_CONTEXT_SIZE, "UTF-8");
    } else {
      int numDesired = Math.max(0, GREP_CONTEXT_SIZE - haystackOffset);
      int beforeSize = beforeBytes == null ? 0 : beforeBytes.length;
      int numExcepted = Math.min(beforeSize, numDesired);
      if (numExcepted > 0) {
        beforeString = new String(beforeBytes, beforeSize - numExcepted,
            numExcepted, "UTF-8").toString()
            + new String(haystackBytes, 0, haystackOffset, "UTF-8").toString();
      } else {
        beforeString = new String(haystackBytes, 0, haystackOffset, "UTF-8");
      }
    }
    String afterString = "";
    int neddleSize = neddle.length;
    int afterOffset = haystackOffset + neddleSize;
    int haystackSize = haystack.limit();
    if (GREP_CONTEXT_SIZE + afterOffset < haystackSize) {
      afterString =
          new String(haystackBytes, afterOffset, GREP_CONTEXT_SIZE, "UTF-8");
    } else {
      int numDesired = GREP_CONTEXT_SIZE - haystackSize - afterOffset;
      int afterSize = afterBytes == null ? 0 : afterBytes.length;
      int numExcepted = Math.min(afterSize, numDesired);
      if (numExcepted > 0) {
        afterString = new String(haystackBytes, afterOffset,
            haystackSize - afterOffset, "UTF-8").toString()
            + new String(afterBytes, 0, numExcepted, "UTF-8").toString();
      } else {
        afterString = new String(haystackBytes, afterOffset,
            haystackSize - afterOffset, "UTF-8");
      }
    }
    ret.put("byteOffset", String.valueOf(fileOffset));
    ret.put("beforeString", beforeString);
    ret.put("afterString", afterString);
    ret.put("matchString", new String(neddle, "UTF-8"));
    ret.put("logviewerURL", url);
    return ret;
  }

  /**
   * Tries once to read ahead in the stream to fill the context and resets the
   * stream to its position before the call.
   * 
   * @param stream
   * @param haystack
   * @param offset
   * @param fileLen
   * @param bytesRead
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#try-read-ahead!")
  public static byte[] tryReadAhead(BufferedInputStream stream,
      ByteBuffer haystack, int offset, int fileLen, int bytesRead)
          throws Exception {
    int numExpected = Math.min(fileLen - bytesRead, GREP_CONTEXT_SIZE);
    byte[] afterBytes = new byte[numExpected];
    stream.mark(numExpected);
    // Only try reading once.
    stream.read(afterBytes, 0, numExpected);
    stream.reset();
    return afterBytes;
  }

  /**
   * Searches a given byte array for a match of a sub-array of bytes. Returns
   * the offset to the byte that matches, or -1 if no match was found.
   * 
   * @param buf
   * @param value
   * @param initOffset
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#offset-of-bytes")
  public static int offsetOfBytes(byte[] buf, byte[] value, int initOffset)
      throws Exception {
    int offset = initOffset;
    int candidateOffset = initOffset;
    int valOffset = 0;
    if (value.length > 0 && initOffset >= 0) {
      while (buf.length > offset) {
        while (buf.length > offset && value[valOffset] != buf[offset]) {
          // The match at this candidate offset failed, so start over with the
          // next candidate byte from the buffer.
          offset++;
        }
        candidateOffset = offset;
        if (candidateOffset >= buf.length) {
          return -1;
        }
        int newOffset = offset;
        boolean match = true;
        for (int i = 0; i < value.length; i++) {
          if (newOffset < buf.length && value[i] == buf[newOffset]) {
            newOffset++;
          } else {
            offset++;
            match = false;
            break;
          }
        }
        if (match) {
          return candidateOffset;
        }
      }
    }
    return -1;
  }

  /**
   * As the file is read into a buffer, 1/2 the buffer's size at a time, we
   * search the buffer for matches of the substring and return a list of zero or
   * more matches
   * 
   * @param isDaemon
   * @param file
   * @param fileLen
   * @param offsetToBuf
   * @param initBufOffset
   * @param stream
   * @param bytesToSkipped
   * @param bytesRead
   * @param haystack
   * @param needle
   * @param initialMatches
   * @param numMatches
   * @param beforeBytes
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#buffer-substring-search!")
  public static byte[] bufferSubstringSearch(boolean isDaemon, File file,
      int fileLen, AtomicInteger offsetToBuf, int initBufOffset,
      BufferedInputStream stream, int bytesSkipped, AtomicLong bytesRead,
      ByteBuffer haystack, byte[] needle,
      List<Map<String, String>> initialMatches, int numMatches,
      byte[] beforeBytes) throws Exception {
    int bufOffset = initBufOffset;
    List<Map<String, String>> matches = initialMatches;
    byte[] haystackArray = haystack.array();
    while (matches.size() < numMatches && bufOffset < haystack.limit()) {
      int offset = offsetOfBytes(haystackArray, needle, bufOffset);
      if (matches.size() < numMatches && offset >= 0) {
        int fileOffset = offset + offsetToBuf.get();
        int bytesNeededAfterMatch =
            haystack.limit() - GREP_CONTEXT_SIZE - needle.length;
        byte[] beforeArg = offset < GREP_CONTEXT_SIZE ? beforeBytes : null;
        byte[] afterArg = offset > bytesNeededAfterMatch ? tryReadAhead(stream,
            haystack, offset, fileLen, bytesRead.intValue()) : null;
        bufOffset = offset + needle.length;
        matches.add(mkMatchData(needle, haystack, offset, fileOffset,
            file.getCanonicalPath(), isDaemon, beforeArg, afterArg));
      } else {
        int beforeStrToOffset = Math.min(haystack.limit(), GREP_CONTEXT_SIZE);
        int beforeStrFromOffset =
            Math.max(0, beforeStrToOffset - GREP_CONTEXT_SIZE);
        byte[] newBeforeBytes = Arrays.copyOfRange(haystack.array(),
            beforeStrFromOffset, beforeStrToOffset);
        // It's OK if new-byte-offset is negative. This is normal if
        // we are out of bytes to read from a small file.
        int newByteOffset = offsetToBuf.get();
        if (matches.size() > numMatches) {
          newByteOffset =
              Integer.valueOf(matches.get(matches.size() - 1).get("byteOffset"))
                  + needle.length;
        } else {
          newByteOffset =
              bytesSkipped + bytesRead.intValue() - GREP_MAX_SEARCH_SIZE;
        }
        beforeBytes = newBeforeBytes;
        offsetToBuf.set(newByteOffset);
        break;
      }
    }
    return beforeBytes;
  }

  /**
   * This response data only includes a next byte offset if there is more of the
   * file to read.
   * 
   * @param searchBytes
   * @param offset
   * @param matches
   * @param nextByteOffset
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#mk-grep-response")
  public static Map mkGrepResponse(byte[] searchBytes, int offset,
      List<Map<String, String>> matches, int nextByteOffset) throws Exception {
    Map ret = new HashMap();
    ret.put("searchString", new String(searchBytes, "UTF-8"));
    ret.put("startByteOffset", offset);
    ret.put("matches", matches);
    if (nextByteOffset != -1) {
      ret.put("nextByteOffset", nextByteOffset);
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#rotate-grep-buffer!")
  public static AtomicLong rotateGrepBuffer(ByteBuffer buf,
      BufferedInputStream stream, AtomicLong totalBytesRead, File file,
      int fileLen) throws Exception {
    byte[] bufArr = buf.array();
    System.arraycopy(bufArr, GREP_MAX_SEARCH_SIZE, bufArr, 0,
        GREP_MAX_SEARCH_SIZE);
    Arrays.fill(bufArr, GREP_MAX_SEARCH_SIZE, bufArr.length, (byte) 0);
    int bytesRead = stream.read(bufArr, GREP_MAX_SEARCH_SIZE,
        Math.min(fileLen, GREP_MAX_SEARCH_SIZE));
    buf.limit(bytesRead + GREP_MAX_SEARCH_SIZE);
    totalBytesRead.addAndGet(bytesRead);
    return totalBytesRead;
  }

  /**
   * Searches for a substring in a log file, starting at the given offset,
   * returning the given number of matches, surrounded by the given number of
   * context lines. Other information is included to be useful for progressively
   * searching through a file for display in a UI. The search string must
   * grep-max-search-size bytes or fewer when decoded with UTF-8.
   * 
   * @param file
   * @param searchString
   * @param isDaemon
   * @param numMatches
   * @param startByteOffset
   * @return
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#substring-search")
  public static Map substringSearch(File file, String searchString,
      boolean isDaemon, int numMatches, long startByteOffset) throws Exception {
    Map ret = new HashMap();
    FileInputStream fInputStream = null;
    InputStream gzippedInputStream = null;
    BufferedInputStream stream = null;
    try {
      if (!StringUtils.isEmpty(searchString)
          && searchString.getBytes("UTF-8").length <= GREP_MAX_SEARCH_SIZE) {
        boolean isZipFile = file.getName().endsWith(".gz");
        fInputStream = new FileInputStream(file);
        gzippedInputStream = isZipFile == true
            ? new GZIPInputStream(fInputStream) : fInputStream;
        stream = new BufferedInputStream(gzippedInputStream);
        long fileLen =
            isZipFile == true ? Utils.zipFileSize(file) : file.length();
        ByteBuffer buf = ByteBuffer.allocate(GREP_BUF_SIZE);
        byte[] bufArr = buf.array();
        AtomicLong totalBytesRead = new AtomicLong(0);
        List<Map<String, String>> matches = new ArrayList<Map<String, String>>();
        byte[] searchBytes = searchString.getBytes("UTF-8");
        numMatches = numMatches > 0 ? numMatches : 10;
        startByteOffset = startByteOffset >= 0 ? startByteOffset : 0;
        // Start at the part of the log file we are interested in.
        // Allow searching when start-byte-offset == file-len so it doesn't blow
        // up on 0-length files
        if (startByteOffset > fileLen) {
          throw new InvalidRequestException(
              "Cannot search past the end of the file");
        }
        if (startByteOffset > 0) {
          skipBytes(stream, startByteOffset);
        }
        Arrays.fill(bufArr, (byte) 0);
        int bytesRead =
            stream.read(bufArr, 0, (int) Math.min(fileLen, GREP_BUF_SIZE));
        buf.limit(bytesRead);
        totalBytesRead.addAndGet(bytesRead);
        // loop
        int initBufOffset = 0;
        AtomicInteger byteOffset = new AtomicInteger((int) startByteOffset);
        byte[] beforeBytes = null;
        while (matches.size() < numMatches && totalBytesRead.get() < fileLen) {
          beforeBytes =
              bufferSubstringSearch(isDaemon, file, (int) fileLen, byteOffset,
                  initBufOffset, stream, (int) startByteOffset, totalBytesRead,
                  buf, searchBytes, matches, numMatches, beforeBytes);
          if (matches.size() < numMatches
              && totalBytesRead.addAndGet(startByteOffset) < fileLen) {
            // The start index is positioned to find any possible
            // occurrence search string that did not quite fit in the
            // buffer on the previous read
            int newBufOffset = Math.min(buf.limit(), GREP_MAX_SEARCH_SIZE)
                - searchBytes.length;
            initBufOffset = newBufOffset;
            rotateGrepBuffer(buf, stream, totalBytesRead, file, (int) fileLen);
            if (totalBytesRead.get() < 0) {
              throw new InvalidRequestException(
                  "Cannot search past the end of the file");
            }
          } else {
            int nextByteOffset = -1;
            if (!(matches.size() < numMatches
                && totalBytesRead.get() >= fileLen)) {
              nextByteOffset =
                  Integer.valueOf(matches.get(matches.size() - 1).get("byteOffset"))
                      + searchBytes.length;
              if (fileLen > nextByteOffset) {

              } else {
                nextByteOffset = -1;
              }
            }
            ret = mkGrepResponse(searchBytes, (int) startByteOffset, matches,
                nextByteOffset);
            ret.put("isDaemon", isDaemon == true ? "yes" : "no");
            return ret;
          }
        }
      }
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    } finally {
      if (stream != null) {
        stream.close();
      }
    }

    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#try-parse-int-param")
  public static int tryParseIntParam(String nam, String value)
      throws Exception {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      String errMsg = "Could not parse " + nam + " to an integer";
      throw new InvalidRequestException(errMsg);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#search-log-file")
  public static void searchLogFile(HttpServletRequest req,
      HttpServletResponse resp, String fname, String user, String rootDir,
      boolean isDaemon, String search, String numMatches, String offset,
      String callback, String origin) throws Exception {
    File file = new File(rootDir, fname).getCanonicalFile();
    if (file.exists()) {
      if (StringUtils.isEmpty((String) STORM_CONF.get(Config.UI_FILTER))
          || authorizedLogUser(user, fname, STORM_CONF)) {
        int numMatchesInt = 0;
        if (numMatches != null) {
          numMatchesInt = tryParseIntParam("num-matches", numMatches);
        }
        int offsetInt = 0;
        if (offset != null) {
          offsetInt =
              numMatchesInt = tryParseIntParam("start-byte-offset", offset);
        }
        try {
          if (!StringUtils.isEmpty(search)
              && search.getBytes("UTF-8").length <= GREP_MAX_SEARCH_SIZE) {
            Map searchResult = substringSearch(file, search, isDaemon,
                numMatchesInt, (long) offsetInt);
            searchLogResponse(resp, searchResult);
            // Map headers = new HashMap();
            // headers.put("Access-Control-Allow-Origin", origin);
            // headers.put("Access-Control-Allow-Credentials", "true");
            // jsonResponse(resp, searchResult, callback, true, 200, headers);
          } else {
            throw new InvalidRequestException(
                "Search substring must be between 1 and 1024 UTF-8 bytes in size (inclusive)");
          }
        } catch (Exception ex) {
          jsonResponse(resp, UIHelpers.exceptionToJson(ex), callback, true, 500,
              new HashMap());
        }
      } else {
        jsonResponse(resp, UIHelpers.unauthorizedUserJson(user), callback, true,
            401, new HashMap());
      }
    } else {
      Map data = new HashMap();
      data.put("error", "Not Found");
      data.put("errorMessage", "The file was not found on this node.");
      jsonResponse(resp, data, callback, true, 404, new HashMap());
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#find-n-matches#wrap-matches-fn")
  private static Map wrapMatchesFn(List<Map> matches, int fileOffset,
      String search) {
    Map ret = new HashMap();
    ret.put("fileOffset", fileOffset);
    ret.put("searchString", search);
    ret.put("matches", matches);
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#find-n-matches")
  public static Map findNMatches(List<File> logs, int n, int fileOffset,
      int offset, String search) throws Exception {
    Map ret = new HashMap();
    logs = logs.subList(fileOffset, logs.size());
    List<Map> matches = new ArrayList<Map>();
    int matchCount = 0;
    while (true) {
      if (logs.isEmpty()) {
        ret = wrapMatchesFn(matches, fileOffset, search);
        break;
      } else {
        Map theseMatches = new HashMap();
        File firtFile = logs.get(0);
        try {
          LOG.debug("Looking through " + firtFile);
          theseMatches = substringSearch(firtFile, search, false,
              n - matchCount, (long) offset);
        } catch (InvalidRequestException e) {
          LOG.error(e.getMessage() + "Can't search past end of file.");
        }
        String fileName = getTopoPortWorkerlog(firtFile);
        theseMatches.put("fileName", fileName);
        theseMatches.put("port",
            getFnameLastNumberPath(firtFile.getCanonicalPath(), 2));
        matches.add(theseMatches);
        int theseMatchesSize = theseMatches.get("matches") == null ? 0
            : ((List) theseMatches.get("matches")).size();
        matchCount += theseMatchesSize;
        if (theseMatches.isEmpty()) {
          logs.remove(0);
          fileOffset++;
        } else {
          if (matchCount >= n) {
            ret = wrapMatchesFn(matches, fileOffset, search);
            break;
          } else {
            logs.remove(0);
            fileOffset++;
          }
        }
      }
    }
    return ret;
  }

  /**
   * Get the filtered, authorized, sorted log files for a port.
   * 
   * @param user
   * @param portDir
   * @throws Exception
   */
  @ClojureClass(className = "org.apache.storm.daemon.logviewer#logs-for-port")
  public static List<File> logsForPort(String user, File portDir)
      throws Exception {
    List<File> files = DirectoryCleaner.getFilesForDir(portDir);
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return (int) (o2.lastModified() - o1.lastModified());
      }
    });
    List<File> ret = new ArrayList<File>();
    for (File file : files) {
      if (StringUtils.isEmpty((String) STORM_CONF.get(Config.UI_FILTER))
          || authorizedLogUser(user, getTopoPortWorkerlog(file), STORM_CONF)) {
        ret.add(file);
      }
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#deep-search-logs-for-topology")
  public static void deepSearchLogsForTopology(HttpServletResponse resp,
      String topologyId, String user, String rootDir, String search,
      String numMatches, String port, String fileOffset, String offset,
      boolean searchArchived, String callback, String origin) throws Exception {
    Map data = new HashMap();
    if (search == null
        || new File(rootDir + Utils.FILE_PATH_SEPARATOR + topologyId)
            .exists()) {

    } else {
      int fileOffsetInt =
          StringUtils.isEmpty(fileOffset) ? 0 : Integer.parseInt(fileOffset);
      int offsetInt =
          StringUtils.isEmpty(offset) ? 0 : Integer.parseInt(offset);
      int numMatchesInt =
          StringUtils.isEmpty(numMatches) ? 0 : Integer.parseInt(numMatches);
      List<File> portDirs = Arrays
          .asList(new File(rootDir + Utils.FILE_PATH_SEPARATOR + topologyId)
              .listFiles());
      if (StringUtils.isEmpty(port) || "*".equals(port)) {
        // Check for all ports
        List<File> filteredLogs = new ArrayList<File>();
        List<File> filteredFirstLogs = new ArrayList<File>();
        for (File tmp : portDirs) {
          List<File> logsForPort = logsForPort(user, tmp);
          if (logsForPort.isEmpty()) {
            continue;
          }
          filteredLogs.addAll(logsForPort);
          filteredFirstLogs.add(logsForPort.get(0));
        }
        if (searchArchived) {
          data = findNMatches(filteredLogs, numMatchesInt, 0, 0, search);
        } else {
          data = findNMatches(filteredFirstLogs, numMatchesInt, 0, 0, search);
        }
      } else {
        // ;; Check just the one port
        List<Integer> supervisorSlotsPorts =
            (List<Integer>) STORM_CONF.get(Config.SUPERVISOR_SLOTS_PORTS);
        if (!supervisorSlotsPorts.contains(Integer.valueOf(port))) {

        } else {
          File portDir = new File(rootDir, Utils.FILE_PATH_SEPARATOR
              + topologyId + Utils.FILE_PATH_SEPARATOR + port);
          if (!portDir.exists() || logsForPort(user, portDir).isEmpty()) {

          } else {
            List<File> filteredLogs = logsForPort(user, portDir);
            if (searchArchived) {
              data = findNMatches(filteredLogs, numMatchesInt, fileOffsetInt,
                  offsetInt, search);
            } else {
              data = findNMatches(Arrays.asList(filteredLogs.get(0)),
                  numMatchesInt, 0, offsetInt, search);
            }
          }
        }
      }
    }
    Map headers = new HashMap();
    headers.put("Access-Control-Allow-Origin", origin);
    headers.put("Access-Control-Allow-Credentials", "true");
    jsonResponse(resp, data, null, true, 200, headers);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#log-template")
  public static void logTemplate(HttpServletRequest req,
      HttpServletResponse resp, String body, String fname, String user)
          throws Exception {
    resp.setContentType("text/html;charset=UTF-8");
    resp.getWriter().println("<html>");
    // head
    resp.getWriter().println("<head>");
    resp.getWriter().println("<meta charset='UTF-8'>");
    resp.getWriter().println("<title>" + fname + " - Strom Log Viewer</title>");
    resp.getWriter().println("<base href='' />");
    resp.getWriter().println("<base target='_self' />");
    resp.getWriter().println(
        "<link href='/css/bootstrap-3.3.1.min.css' rel='stylesheet' type='text/css'>");
    resp.getWriter().println(
        "<link href='/css/jquery.dataTables.1.10.4.min.css' rel='stylesheet' type='text/css'>");
    resp.getWriter().println(
        "<link href='/css/style.css' rel='stylesheet' type='text/css'>");
    resp.getWriter().println(
        "<script src='/js/jquery-1.11.1.min.js' type='text/javascript'></script>");
    resp.getWriter().println(
        "<script src='/js/logviewer.js' type='text/javascript'></script>");
    resp.getWriter().println("</head>");
    // body
    resp.getWriter().println("<body>");
    if (StringUtils.isNotEmpty(user)) {
      resp.getWriter()
          .println("<div class='ui-user'><p> User: " + user + "</p></div>");

    }
    resp.getWriter().println(
        "<div class='ui-note'><p>Note: the drop-list shows at most 1024 files for each worker directory.</p></div>");
    resp.getWriter().println("<h3>" + fname + "</h3>");
    resp.getWriter().println(body);
    resp.getWriter().println("</body>");
    resp.getWriter().println("</html>");
    resp.getWriter().flush();
    resp.getWriter().close();
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#parse-long-from-map")
  public static Long parseLongFromMap(String n, String k) throws Exception {
    try {
      return Long.parseLong(k);
    } catch (NumberFormatException ex) {
      throw new InvalidRequestException(
          "Could not make an integer out of the query parameter '" + n + k
              + "'");
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#list-log-files")
  public static void listLogFiles(HttpServletResponse resp, String user,
      String topoId, Integer port, String logRoot, String callback,
      String origin) throws Exception {
    List<File> fileResults = new ArrayList<File>();
    if (StringUtils.isEmpty(topoId)) {
      if (port == null) {
        fileResults = getAllLogsForRootdir(new File(logRoot));
      } else {
        for (File topoDir : new File(logRoot).listFiles()) {
          for (File portDir : topoDir.listFiles()) {
            if (portDir.getName().equals(String.valueOf(port))) {
              fileResults.addAll(DirectoryCleaner.getFilesForDir(portDir));
            }
          }
        }
      }
    } else {
      if (port == null) {
        File topoDir = new File(logRoot + Utils.FILE_PATH_SEPARATOR + topoId);
        if (topoDir.exists()) {
          for (File portDir : topoDir.listFiles()) {
            fileResults.addAll(DirectoryCleaner.getFilesForDir(portDir));
          }
        }
      } else {
        File portDir = ConfigUtils.getWorkerDirFromRoot(logRoot, topoId, port);
        if (portDir.exists()) {
          fileResults.addAll(DirectoryCleaner.getFilesForDir(portDir));
        }
      }
    }
    List<String> fileStrs = new ArrayList<String>();
    for (File file : fileResults) {
      fileStrs.add(getTopoPortWorkerlog(file));
    }
    Map headers = new HashMap();
    headers.put("Access-Control-Allow-Origin", origin);
    headers.put("Access-Control-Allow-Credentials", "true");
    jsonResponse(resp, fileStrs, callback, true, 200, headers);
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#get-profiler-dump-files")
  public static List<File> getProfilerDumpFiles(File dir) throws Exception {
    List<File> dumpFiles = new ArrayList<File>();
    for (File f : DirectoryCleaner.getFilesForDir(dir)) {
      String name = f.getName();
      if (name.endsWith(".txt") || name.endsWith(".jfr")
          || name.endsWith(".bin")) {
        dumpFiles.add(f);
      }
    }
    return dumpFiles;
  }

  @ClojureClass(className = "org.apache.storm.daemon.logviewer#conf-middlerware")
  public static LogviewerServlet confMiddleware(LogviewerServlet app,
      String logRoot, String daemonlogRoot) {
    app.setLogRoot(logRoot);
    app.setDaemonlogRoot(daemonlogRoot);
    return app;
  }

  @ClojureClass(className = "org.apache.storm.ui.helpers#json-response")
  public static void jsonResponse(HttpServletResponse resp, Object data,
      String callback, boolean needSerialize, int status, Map headers)
          throws Exception {
    resp.setStatus(status);
    UIHelpers.getJsonResponseHeaders(callback, headers);
    UIHelpers.getJsonResponseBody(data, callback, needSerialize);
    // TODO
  }

  private static void searchLogResponse(HttpServletResponse resp,
      Map<String, Object> data) throws IOException {
    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      String searchString = (String) data.get("searchString");
      String isDaemon = (String) data.get("isDaemon");
      int startByteOffset = (int) data.get("startByteOffset");
      List<Map<String, String>> matches =
          (List<Map<String, String>>) data.get("matches");
      int nextByteOffset = data.get("nextByteOffset") == null ? -1
          : (int) data.get("nextByteOffset");

      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField("searchString", searchString);
      dumpGenerator.writeStringField("isDaemon", isDaemon);
      dumpGenerator.writeNumberField("startByteOffset", startByteOffset);
      dumpGenerator.writeNumberField("nextByteOffset", nextByteOffset);
      dumpGenerator.writeFieldName("matches");
      dumpGenerator.writeStartArray();
      for (Map<String, String> map : matches) {
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, String> entry : map.entrySet()) {
          dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } finally {
      out.close();
    }
  }

}
