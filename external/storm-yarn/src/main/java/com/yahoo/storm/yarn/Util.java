/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import org.apache.storm.utils.Utils;

import com.google.common.base.Joiner;
import com.tencent.jstorm.scheduler.DefaultScheduler;
import com.tencent.jstorm.utils.ReflectionUtils;
import com.tencent.jstorm.utils.ServerUtils;
import org.apache.storm.utils.VersionInfo;
import com.tencent.storm.yarn.container.DefaultCalculator;
import com.tencent.storm.yarn.container.IContainerCalculator;

public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final String STORM_CONF_PATH_STRING = "conf" + Path.SEPARATOR
      + "storm.yaml";
  private static final String PROCFS_FILESYSTEM = "/proc";
  private static final String PROCFS_CMDLINE_FILE = "cmdline";

  private static final Pattern heapPattern = Pattern
      .compile("-Xmx([0-9]+)([mMgG])");

  static String getStormHome() {
    String ret = System.getProperty("storm.home");
    if (ret == null) {
      throw new RuntimeException("storm.home is not set");
    }
    return ret;
  }

  public static String getJavaHome() {
    return getJavaHome(null);
  }

  public static String getJavaHome(String defaultJavaHome) {
    if (defaultJavaHome != null && !defaultJavaHome.isEmpty()) {
      return defaultJavaHome;
    }
    String javaHome = "java";
    if (System.getenv("JAVA_HOME") != null) {
      javaHome = System.getenv("JAVA_HOME") + "/bin/java";
    }
    return javaHome;
  }

  public static String getStormVersion() throws IOException {
    return VersionInfo.getVersion() + "-r" + VersionInfo.getRevision();
  }

  /**
   * for debug
   */
  static void sleep() {
    try {
      Thread.sleep(30 * 60 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static String getStormHomeInZip(FileSystem fs, Path zip, String stormVersion)
      throws IOException, RuntimeException {
    FSDataInputStream fsInputStream = fs.open(zip);
    ZipInputStream zipInputStream = new ZipInputStream(fsInputStream);
    ZipEntry entry = zipInputStream.getNextEntry();
    while (entry != null) {
      String entryName = entry.getName();
      if (entryName.matches("^storm(-" + stormVersion + ")?/")
          || entryName.matches("^jstorm(-" + stormVersion + ")?/")) {
        fsInputStream.close();
        return entryName.replace("/", "");
      }
      entry = zipInputStream.getNextEntry();
    }
    fsInputStream.close();
    throw new RuntimeException(
        "Can not find storm home entry in storm zip file. Wanted storm-"
            + stormVersion + " or jstorm-" + stormVersion);
  }

  static LocalResource newYarnAppResource(FileSystem fs, Path path,
      LocalResourceType type, LocalResourceVisibility vis) throws IOException {
    Path qualified = fs.makeQualified(path);
    FileStatus status = fs.getFileStatus(qualified);
    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setType(type);
    resource.setVisibility(vis);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
    resource.setTimestamp(status.getModificationTime());
    resource.setSize(status.getLen());
    return resource;
  }

  @SuppressWarnings("rawtypes")
  static void rmNulls(Map map) {
    Set s = map.entrySet();
    Iterator it = s.iterator();
    while (it.hasNext()) {
      Map.Entry m = (Map.Entry) it.next();
      if (m.getValue() == null)
        it.remove();
    }
  }

  @SuppressWarnings("rawtypes")
  static Path createConfigurationFileInFs(FileSystem fs, String appHome,
      Map stormConf, YarnConfiguration yarnConf) throws IOException {
    // dump stringwriter's content into FS conf/storm.yaml
    Path confDst =
        new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR
            + STORM_CONF_PATH_STRING);
    Path dirDst = confDst.getParent();
    fs.mkdirs(dirDst);

    // storm.yaml
    FSDataOutputStream out = fs.create(confDst);
    Yaml yaml = new Yaml();
    OutputStreamWriter writer = new OutputStreamWriter(out);
    rmNulls(stormConf);
    yaml.dump(stormConf, writer);
    writer.close();
    out.close();

    // storm.yarn.master.yaml
    Path storm_yarn_master_yaml = new Path(dirDst, "storm.yarn.master.yaml");
    out = fs.create(storm_yarn_master_yaml);
    createLogFile(out, "storm.yarn.master.yaml");
    out.close();

    // yarn-site.xml
    Path yarn_site_xml = new Path(dirDst, "yarn-site.xml");
    out = fs.create(yarn_site_xml);
    writer = new OutputStreamWriter(out);
    yarnConf.writeXml(writer);
    writer.close();
    out.close();

    // storm.log4j.properties
    Path log4j_properties = new Path(dirDst, "storm.log4j.properties");
    out = fs.create(log4j_properties);
    createLogFile(out, "storm.log4j.properties");
    out.close();

    return dirDst;
  }

  static LocalResource newYarnAppResource(FileSystem fs, Path path)
      throws IOException {
    return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
  }

  private static void createLogFile(OutputStream out, String fileName)
      throws IOException {
    Enumeration<URL> logback_xml_urls;
    logback_xml_urls =
        Thread.currentThread().getContextClassLoader().getResources(fileName);
    while (logback_xml_urls.hasMoreElements()) {
      URL logback_xml_url = logback_xml_urls.nextElement();
      if (logback_xml_url.getProtocol().equals("file")) {
        // Case 1: fileName as simple file
        FileInputStream is = new FileInputStream(logback_xml_url.getPath());
        while (is.available() > 0) {
          out.write(is.read());
        }
        is.close();
        return;
      }
      if (logback_xml_url.getProtocol().equals("jar")) {
        // Case 2: fileName included in a JAR
        String path = logback_xml_url.getPath();
        String jarFile = path.substring("file:".length(), path.indexOf("!"));
        java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
        Enumeration<JarEntry> enums = jar.entries();
        while (enums.hasMoreElements()) {
          java.util.jar.JarEntry file = enums.nextElement();
          if (!file.isDirectory() && file.getName().equals(fileName)) {
            InputStream is = jar.getInputStream(file); // get the input stream
            while (is.available() > 0) {
              out.write(is.read());
            }
            is.close();
            jar.close();
            return;
          }
        }
        jar.close();
      }
    }

    throw new IOException("Failed to locate a " + fileName);
  }

  /**
   * build launch user topology jar example: storm --config storm_config_dir jar
   * ./UserTopology.jar storm.starter.WordCountTopology wordcountTop
   * 
   * @param conf
   * @param topologyMainClass
   * @param userArg
   * @return
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  static List<String> buildUserTopologyCommands(Map conf,
      String topologyMainClass, String userArg) throws IOException {
    List<String> toRet = new ArrayList<String>();
    toRet.add(getStormHome() + "/bin/storm");
    toRet.add("--config");
    toRet.add(System.getProperty("storm.conf.dir"));
    toRet.add("jar");
    toRet.add("./UserTopology.jar");
    toRet.add(topologyMainClass);
    if (userArg != null) {
      String[] tokens = userArg.trim().split(",");
      for (int i = 0; i < tokens.length; i++) {
        toRet.add(tokens[i].trim());
      }
    }
    return toRet;
  }

  @SuppressWarnings("rawtypes")
  private static List<String> buildCommandPrefix(Map conf, String childOptsKey)
      throws IOException {
    String stormHomePath = getStormHome();
    List<String> toRet = new ArrayList<String>();
    if (System.getenv("JAVA_HOME") != null)
      toRet.add(System.getenv("JAVA_HOME") + "/bin/java");
    else
      toRet.add("java");
    toRet.add("-server");

    if (conf.containsKey(childOptsKey) && conf.get(childOptsKey) != null) {
      toRet.add((String) conf.get(childOptsKey));
    }

    toRet.add("-Dstorm.home=" + stormHomePath);
    toRet.add("-Djava.library.path="
        + conf.get(backtype.storm.Config.JAVA_LIBRARY_PATH));
    toRet.add("-Dstorm.conf.dir=" + System.getProperty("storm.conf.dir"));
    toRet.add("-Dlog4j.configuration=File:"
        + System.getProperty("storm.conf.dir") + "/storm.log4j.properties");

    toRet.add("-cp");
    toRet.add(buildClassPathArgument());
    return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildUICommands(Map conf) throws IOException {
    List<String> toRet =
        buildCommandPrefix(conf, backtype.storm.Config.UI_CHILDOPTS);
    toRet.add("-Dstorm.log.dir=" + System.getProperty("storm.log.dir"));
    toRet.add("-Dstorm.options=" + backtype.storm.Config.NIMBUS_HOST
        + "=localhost");
    toRet.add("-Dstorm.log.file=" + "ui.log");
    toRet.add("com.tencent.jstorm.ui.core.UIServer");

    return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildNimbusCommands(Map conf) throws IOException {
    List<String> toRet =
        buildCommandPrefix(conf, backtype.storm.Config.NIMBUS_CHILDOPTS);
    toRet.add("-Dstorm.log.dir=" + System.getProperty("storm.log.dir"));
    toRet.add("-Dstorm.log.file=" + "nimbus.log");
    toRet.add("com.tencent.jstorm.daemon.nimbus.NimbusServer");

    return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildSupervisorCommands(Map conf) throws IOException {
    List<String> toRet =
        buildCommandPrefix(conf, backtype.storm.Config.SUPERVISOR_CHILDOPTS);
    toRet.add("-Dstorm.log.dir=$STORM_LOG_DIR");
    toRet.add("-Dworker.logdir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    toRet.add("-Dstorm.log.file=supervisor.log");
    toRet.add("org.apache.storm.daemon.supervisor.Supervisor");

    return toRet;
  }

  @SuppressWarnings("rawtypes")
  static List<String> buildDrpcCommands(Map conf) throws IOException {
    List<String> toRet =
        buildCommandPrefix(conf, backtype.storm.Config.DRPC_CHILDOPTS);

    return toRet;
  }

  private static String buildClassPathArgument() throws IOException {
    List<String> paths = new ArrayList<String>();
    paths.add(new File(STORM_CONF_PATH_STRING).getParent());
    paths.add(getStormHome());
    for (String jarPath : findAllJarsInPaths(getStormHome(), getStormHome()
        + File.separator + "lib")) {
      paths.add(jarPath);
    }
    return Joiner.on(File.pathSeparatorChar).join(paths);
  }

  private static interface FileVisitor {
    public void visit(File file);
  }

  public static void traverse(File file, FileVisitor visitor) {
    if (file.isDirectory()) {
      File childs[] = file.listFiles();
      if (childs.length > 0) {
        for (int i = 0; i < childs.length; i++) {
          File child = childs[i];
          traverse(child, visitor);
        }
      }
    } else {
      visitor.visit(file);
    }
  }

  private static List<String> findAllJarsInPaths(String... pathStrs)
      throws IOException {
    final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();
    FileVisitor visitor = new FileVisitor() {
      @Override
      public void visit(File file) {
        String name = file.getName();
        if (name.endsWith(".jar")) {
          pathSet.add(file.getPath());
        }
      }
    };
    for (String path : pathStrs) {
      File file = new File(path);
      traverse(file, visitor);
    }
    final List<String> toRet = new ArrayList<String>();
    for (String p : pathSet) {
      toRet.add(p);
    }
    return toRet;
    // java.nio.file.FileSystem fs = FileSystems.getDefault();
    // final PathMatcher matcher = fs.getPathMatcher("glob:**.jar");
    // final LinkedHashSet<String> pathSet = new LinkedHashSet<String>();
    /*
     * for (String pathStr : pathStrs) { java.nio.file.Path start =
     * fs.getPath(pathStr); Files.walkFileTree(start, new
     * SimpleFileVisitor<java.nio.file.Path>() {
     * 
     * @Override public FileVisitResult visitFile(java.nio.file.Path path,
     * BasicFileAttributes attrs) throws IOException { if (attrs.isRegularFile()
     * && matcher.matches(path) && !pathSet.contains(path)) { java.nio.file.Path
     * parent = path.getParent(); pathSet.add(parent + File.separator + "*");
     * return FileVisitResult.SKIP_SIBLINGS; } return FileVisitResult.CONTINUE;
     * } }); }
     */
    // final List<String> toRet = new ArrayList<String>();
    // for (String p : pathSet) {
    // toRet.add(p);
    // }
    // return toRet;
  }

  static String getApplicationHomeForId(String id) {
    if (id.isEmpty()) {
      throw new IllegalArgumentException(
          "The ID of the application cannot be empty.");
    }
    return ".storm" + Path.SEPARATOR + id;
  }

  /**
   * Returns a boolean to denote whether a cache file is visible to all(public)
   * or not
   * 
   * @param fs Hadoop file system
   * @param path file path
   * @return true if the path is visible to all, false otherwise
   * @throws IOException
   */
  static boolean isPublic(FileSystem fs, Path path) throws IOException {
    // the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, path, FsAction.READ)) {
      return false;
    }
    return ancestorsHaveExecutePermissions(fs, path.getParent());
  }

  /**
   * Checks for a given path whether the Other permissions on it imply the
   * permission in the passed FsAction
   * 
   * @param fs
   * @param path
   * @param action
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  private static boolean checkPermissionOfOther(FileSystem fs, Path path,
      FsAction action) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    if (otherAction.implies(action)) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse the
   * directory hierarchy to the given path)
   */
  static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path)
      throws IOException {
    Path current = path;
    while (current != null) {
      // the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }

  static void redirectStreamAsync(final InputStream input,
      final PrintStream output) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner scanner = new Scanner(input);
        while (scanner.hasNextLine()) {
          output.println(scanner.nextLine());
        }
      }
    }).start();
  }

  /**
   * the same as ApplicationId.appIdFormat
   */
  static final ThreadLocal<NumberFormat> appIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  /**
   * the same as ApplicationAttemptId.attemptIdFormat
   */
  static final ThreadLocal<NumberFormat> appAttemptIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };

  public static int assignServerSocketPort(int port) {
    if (port < 0) {// allocate an appmaster thrift port
      ServerSocket serverSocket;
      try {
        serverSocket = new ServerSocket(0);
        port = serverSocket.getLocalPort();
        serverSocket.close();
        LOG.info("assignServerSocketPort port=" + port);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return port;
  }

  /**
   * 获得Application所在的目录的中间部分字符串:application_xxxxxxxx_xxx/
   * container_xxxxxxxxx_xxxxxx
   * 
   * @param appAttemptID
   * @return
   */
  public static String containerIdMatchStr(ApplicationAttemptId appAttemptID) {
    return toApplicationId(appAttemptID) + "/container_"
        + appAttemptID.getApplicationId().getClusterTimestamp() + "_"
        + appIdFormat.get().format(appAttemptID.getApplicationId().getId());
  }

  /**
   * generate applicationId by ApplicatonAttemptId
   * 
   * @param appAttemptID
   * @return
   */
  public static String toApplicationId(ApplicationAttemptId appAttemptID) {
    return "application" + "_"
        + appAttemptID.getApplicationId().getClusterTimestamp() + "_"
        + appIdFormat.get().format(appAttemptID.getApplicationId().getId());
    // return appAttemptID.getApplicationId().toString();
  }

  public static int getHeapMegabytes(String opt) {
    int heapMegabytes = 128;
    Matcher matcher = heapPattern.matcher(opt);
    if (matcher.find()) {
      String heapSize = matcher.group(1);

      heapMegabytes = Integer.parseInt(heapSize);

      if (matcher.group(2).equalsIgnoreCase("G")) {
        heapMegabytes *= 1024;
      }
    }
    return heapMegabytes;
  }

  @SuppressWarnings("rawtypes")
  public static int storm_zookeeper_session_timeout(Map storm_conf) {
    return (Integer) storm_conf
        .get(backtype.storm.Config.STORM_ZOOKEEPER_SESSION_TIMEOUT);
  }

  public static void writeIdToLocalFile(String fileName, int id)
      throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, false));
    bw.write(String.valueOf(id));
    bw.close();
  }

  public static Integer readIdFromLocalFile(String fileName) throws IOException {
    File file = new File(fileName);
    if (file.exists()) {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String idStr = br.readLine();
      br.close();
      return new Integer(Integer.valueOf(idStr));
    }
    return null;
  }

  public static boolean isExistResponseId() throws IOException {
    File file = new File(Config.YARN_AM_RESPONSEID_FILE);
    if (file.exists()) {
      return true;
    }
    return false;
  }

  @SuppressWarnings("rawtypes")
  public static Integer getMasterContainerRetartNum(Map storm_conf) {
    return Utils
        .getInt(storm_conf.get(Config.MASTER_CONTAINER_RESTART_NUM), 16);
  }

  @SuppressWarnings("rawtypes")
  public static Integer getSupervisorContainerRetartNum(Map storm_conf) {
    return Utils.getInt(
        storm_conf.get(Config.SUPERVISOR_CONTAINER_RESTART_NUM), 16);
  }

  @SuppressWarnings("rawtypes")
  public static Integer getNumVcorePerWorker(Map storm_conf) {
    return Utils.getInt(storm_conf.get(Config.WORKER_NUM_VCORE), 1);
  }

  public static void copyToLocalFile(FileSystem srcFS, Path src, File dst)
      throws IOException {
    FileStatus fileStatus = srcFS.getFileStatus(src);
    if (fileStatus.isDirectory()) {
      if (!dst.exists()) {
        dst.mkdirs();
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        Util.copyToLocalFile(srcFS, contents[i].getPath(), new File(dst,
            contents[i].getPath().getName()));
      }
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = new FileOutputStream(dst);
        IOUtils.copyBytes(in, out, new Configuration(), true);
      } catch (IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
  }

  /**
   * Get the list of all processes in the system.
   */
  private static List<Integer> getProcessList(String procfsDir) {
    String[] processDirs = (new File(procfsDir)).list();
    List<Integer> processList = new ArrayList<Integer>();

    if (processDirs != null) {
      for (String dir : processDirs) {
        try {
          int pd = Integer.parseInt(dir);
          if ((new File(procfsDir, dir)).isDirectory()) {
            processList.add(Integer.valueOf(pd));
          }
        } catch (NumberFormatException n) {
          // skip this directory
        } catch (SecurityException s) {
          // skip this process
        }
      }
    }
    return processList;
  }

  public static String getCmdLine(String procfsDir, Integer pid) {
    String ret = "N/A";
    if (pid == null) {
      return ret;
    }
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      fReader =
          new FileReader(new File(new File(procfsDir, pid.toString()),
              PROCFS_CMDLINE_FILE));
    } catch (FileNotFoundException f) {
      // The process vanished in the interim!
      return ret;
    }

    in = new BufferedReader(fReader);

    try {
      ret = in.readLine(); // only one line
      if (null == ret)
        ret = "N/A";
      ret = ret.replace('\0', ' '); // Replace each null char with a space
      if (ret.equals("")) {
        // The cmdline might be empty because the process is swapped out or is
        // a zombie.
        ret = "N/A";
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
      ret = "N/A";
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }

    return ret;
  }

  static int getProcPid(String procName) {
    int retPid = 0;
    final Pattern PROCFS_NAME_FORMAT =
        Pattern.compile(procName, Pattern.CASE_INSENSITIVE);
    // Get the list of processes
    List<Integer> processList = getProcessList(PROCFS_FILESYSTEM);
    for (Integer proc : processList) {
      String cmdline = getCmdLine(PROCFS_FILESYSTEM, proc);
      Matcher m = PROCFS_NAME_FORMAT.matcher(cmdline);
      boolean mat = m.find();
      if (mat) {
        retPid = proc.intValue();
        LOG.info("getProcPid find " + procName + "pid: " + retPid);
        return retPid;
      }
    }
    LOG.info("getProcPid not find " + procName + "pid: " + retPid);
    return retPid;
  }

  static boolean checkProcExist(String procName, int pid) {
    final Pattern PROCFS_NAME_FORMAT =
        Pattern.compile(procName, Pattern.CASE_INSENSITIVE);
    String cmdline = getCmdLine(PROCFS_FILESYSTEM, new Integer(pid));
    Matcher m = PROCFS_NAME_FORMAT.matcher(cmdline);
    boolean mat = m.find();
    LOG.debug("checkProcExist " + procName + " @pid " + pid + "return :" + mat);
    return mat;
  }

  public static String parseString(Object o, String defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    return String.valueOf(o);
  }

  @SuppressWarnings("rawtypes")
  public static IContainerCalculator mkContainerCalculator(
      StormAMRMClient client, Map conf) {
    IContainerCalculator scheduler = null;
    if (conf.get(Config.MASTER_CONTAINER_CALCULATOR) != null) {
      String custormScheduler =
          ServerUtils.parseString(conf.get(Config.MASTER_CONTAINER_CALCULATOR),
              null);
      LOG.info("Using custom scheduler: " + custormScheduler);
      try {
        scheduler =
            (IContainerCalculator) ReflectionUtils
                .newInstance(custormScheduler);

      } catch (Exception e) {
        LOG.error(custormScheduler
            + " : Scheduler initialize error! Using default scheduler", e);
        scheduler = new DefaultCalculator();
      }
    } else {
      LOG.info("Using default scheduler {}", DefaultScheduler.class.getName());
      scheduler = new DefaultCalculator();
    }
    scheduler.prepare(client, conf);
    return scheduler;
  }
}
