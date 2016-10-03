package com.tencent.jstorm.ql.session;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.storm.Config;

import com.tencent.jstorm.ql.exec.Utils;
import com.tencent.jstorm.utils.ServerUtils;

public class SessionState {
	private static final Log LOG = LogFactory.getLog(SessionState.class);

	protected ClassLoader parentLoader;

	protected Config conf;

	/**
	 * silent mode.
	 */
	protected boolean isSilent;

	/**
	 * verbose mode
	 */
	protected boolean isVerbose;

	/**
	 * Streams to read/write from.
	 */
	public InputStream in;
	public PrintStream out;
	public PrintStream info;
	public PrintStream err;
	/**
	 * Standard output from any child process(es).
	 */
	public PrintStream childOut;
	/**
	 * Error output from any child process(es).
	 */
	public PrintStream childErr;

	/**
	 * Temporary file name used to store results of non-Hive commands (e.g.,
	 * set, dfs) and HiveServer.fetch*() function will read results from this
	 * file
	 */
	protected File tmpOutputFile;

	private String lastCommand;

	private Map<String, String> hiveVariables;

	// A mapping from a hadoop job ID to the stack traces collected from the map
	// reduce task logs
	private Map<String, List<List<String>>> stackTraces;

	// This mapping collects all the configuration variables which have been set
	// by the user
	// explicitely, either via SET in the CLI, the hiveconf option, or a System
	// property.
	// It is a mapping from the variable name to its value. Note that if a user
	// repeatedly
	// changes the value of a variable, the corresponding change will be made in
	// this mapping.
	private Map<String, String> overriddenConfigurations;

	private Map<String, List<String>> localMapRedErrors;

	private boolean addedResource;

	private final String userName;

	public Config getConf() {
		return conf;
	}

	public void setConf(Config conf) {
		this.conf = conf;
	}

	public File getTmpOutputFile() {
		return tmpOutputFile;
	}

	public void setTmpOutputFile(File f) {
		tmpOutputFile = f;
	}

	public boolean getIsSilent() {
		if (conf != null) {
			return false;
		} else {
			return isSilent;
		}
	}

	public void setIsSilent(boolean isSilent) {
		if (conf != null) {

			// conf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, isSilent);
		}
		this.isSilent = isSilent;
	}

	public boolean getIsVerbose() {
		return isVerbose;
	}

	public void setIsVerbose(boolean isVerbose) {
		this.isVerbose = isVerbose;
	}

	public SessionState(Config conf) {
		this(conf, null);
	}

	public SessionState(Config conf, String userName) {
		this.conf = conf;
		this.userName = userName;
		isSilent = false;
		// ls = new LineageState();
		overriddenConfigurations = new HashMap<String, String>();
		// overriddenConfigurations.putAll(HiveConf.getConfSystemProperties());
		// // if there isn't already a session name, go ahead and create it.
		// if
		// (StringUtils.isEmpty(conf.getVar(HiveConf.ConfVars.HIVESESSIONID))) {
		// conf.setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
		// }
		// parentLoader = JavaUtils.getClassLoader();
	}

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyyMMddHHmm");

	public void setCmd(String cmdString) {
		// conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, cmdString);
	}

	public String getCmd() {
		// return (conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING));
		return null;
	}

	public String getQueryId() {
		// return (conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
		return null;
	}

	public Map<String, String> getStormVariables() {
		if (hiveVariables == null) {
			hiveVariables = new HashMap<String, String>();
		}
		return hiveVariables;
	}

	public void setHiveVariables(Map<String, String> hiveVariables) {
		this.hiveVariables = hiveVariables;
	}

	public String getSessionId() {
		// return (conf.getVar(HiveConf.ConfVars.HIVESESSIONID));
		return null;
	}

	/**
	 * Singleton Session object per thread.
	 * 
	 **/
	private static ThreadLocal<SessionState> tss = new ThreadLocal<SessionState>();

	/**
	 * start a new session and set it to current session.
	 */
	public static SessionState start(Config conf) {
		SessionState ss = new SessionState(conf);
		return start(ss);
	}

	/**
	 * Sets the given session state in the thread local var for sessions.
	 */
	public static void setCurrentSessionState(SessionState startSs) {
		tss.set(startSs);
		Thread.currentThread().setContextClassLoader(
				startSs.getConf().getClassLoader());
	}

	public static void detachSession() {
		tss.remove();
	}

	/**
	 * set current session to existing session object if a thread is running
	 * multiple sessions - it must call this method with the new session object
	 * when switching from one session to another.
	 * 
	 * @throws HiveException
	 */
	public static SessionState start(SessionState startSs) {

		setCurrentSessionState(startSs);

		if (startSs.getTmpOutputFile() == null) {
			// set temp file containing results to be sent to HiveClient
			try {
				startSs.setTmpOutputFile(createTempFile(startSs.getConf()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return startSs;
	}

	/**
	 * @param conf
	 * @return per-session temp file
	 * @throws IOException
	 */
	private static File createTempFile(Map conf) throws IOException {
		String lScratchDir = "/tmp";

		File tmpDir = new File(lScratchDir);
		String sessionID = "testst";
		if (!tmpDir.exists()) {
			if (!tmpDir.mkdirs()) {
				// Do another exists to check to handle possible race condition
				// Another thread might have created the dir, if that is why
				// mkdirs returned false, that is fine
				if (!tmpDir.exists()) {
					throw new RuntimeException(
							"Unable to create log directory " + lScratchDir);
				}
			}
		}
		File tmpFile = File.createTempFile(sessionID, ".pipeout", tmpDir);
		tmpFile.deleteOnExit();
		return tmpFile;
	}

	/**
	 * get the current session.
	 */
	public static SessionState get() {
		return tss.get();
	}

	/**
	 * Create a session ID. Looks like: $user_$pid@$host_$date
	 * 
	 * @return the unique string
	 */
	private static String makeSessionId() {
		return UUID.randomUUID().toString();
	}

	public String getLastCommand() {
		return lastCommand;
	}

	public void setLastCommand(String lastCommand) {
		this.lastCommand = lastCommand;
	}

	/**
	 * This class provides helper routines to emit informational and error
	 * messages to the user and log4j files while obeying the current session's
	 * verbosity levels.
	 * 
	 * NEVER write directly to the SessionStates standard output other than to
	 * emit result data DO use printInfo and printError provided by LogHelper to
	 * emit non result data strings.
	 * 
	 * It is perfectly acceptable to have global static LogHelper objects (for
	 * example - once per module) LogHelper always emits info/error to current
	 * session as required.
	 */
	public static class LogHelper {

		protected Log LOG;
		protected boolean isSilent;

		public LogHelper(Log LOG) {
			this(LOG, false);
		}

		public LogHelper(Log LOG, boolean isSilent) {
			this.LOG = LOG;
			this.isSilent = isSilent;
		}

		public PrintStream getOutStream() {
			SessionState ss = SessionState.get();
			return ((ss != null) && (ss.out != null)) ? ss.out : System.out;
		}

		public PrintStream getInfoStream() {
			SessionState ss = SessionState.get();
			return ((ss != null) && (ss.info != null)) ? ss.info
					: getErrStream();
		}

		public PrintStream getErrStream() {
			SessionState ss = SessionState.get();
			return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
		}

		public PrintStream getChildOutStream() {
			SessionState ss = SessionState.get();
			return ((ss != null) && (ss.childOut != null)) ? ss.childOut
					: System.out;
		}

		public PrintStream getChildErrStream() {
			SessionState ss = SessionState.get();
			return ((ss != null) && (ss.childErr != null)) ? ss.childErr
					: System.err;
		}

		public boolean getIsSilent() {
			SessionState ss = SessionState.get();
			// use the session or the one supplied in constructor
			return (ss != null) ? ss.getIsSilent() : isSilent;
		}

		public void printInfo(String info) {
			printInfo(info, null);
		}

		public void printInfo(String info, String detail) {
			if (!getIsSilent()) {
				getInfoStream().println(info);
			}
			LOG.info(info + StringUtils.defaultString(detail));
		}

		public void printError(String error) {
			printError(error, null);
		}

		public void printError(String error, String detail) {
			getErrStream().println(error);
			LOG.error(error + StringUtils.defaultString(detail));
		}
	}

	private static LogHelper _console;

	/**
	 * initialize or retrieve console object for SessionState.
	 */
	public static LogHelper getConsole() {
		if (_console == null) {
			Log LOG = LogFactory.getLog("SessionState");
			_console = new LogHelper(LOG);
		}
		return _console;
	}

	public static String validateFile(Set<String> curFiles, String newFile) {
		SessionState ss = SessionState.get();
		LogHelper console = getConsole();
		Config conf = (ss == null) ? new Config() : ss.getConf();
		// TODO
		if (ServerUtils.existsFile(newFile)) {
			return new File(newFile).getAbsolutePath();
		} else {
			console.printError(newFile + " does not exist");
			return null;
		}
	}

	public static boolean registerJar(String newJar) {
		LogHelper console = getConsole();
		try {
			ClassLoader loader = SessionState.get().getConf().getClassLoader();
			ClassLoader newLoader = Utils.addToClassPath(loader,
					StringUtils.split(newJar, ","));
			Thread.currentThread().setContextClassLoader(newLoader);
			SessionState.get().getConf().setClassLoader(newLoader);
			console.printInfo("Added " + newJar + " to class path");
			return true;
		} catch (Exception e) {
			console.printError(
					"Unable to register " + newJar + "\nException: "
							+ e.getMessage(),
					"\n"
							+ com.tencent.jstorm.utils.StringUtils
									.stringifyException(e));
			return false;
		}
	}

	public static boolean unregisterJar(String jarsToUnregister) {
		LogHelper console = getConsole();
		// try {
		// Utilities.removeFromClassPath(StringUtils.split(jarsToUnregister,
		// ","));
		// console.printInfo("Deleted " + jarsToUnregister +
		// " from class path");
		// return true;
		// } catch (Exception e) {
		// console.printError("Unable to unregister " + jarsToUnregister
		// + "\nException: " + e.getMessage(), "\n"
		// + org.apache.hadoop.util.StringUtils.stringifyException(e));
		// return false;
		// }
		return false;
	}

	/**
	 * ResourceHook.
	 * 
	 */
	public static interface ResourceHook {
		String preHook(Set<String> cur, String s);

		boolean postHook(Set<String> cur, String s);
	}

	/**
	 * ResourceType.
	 * 
	 */
	public static enum ResourceType {
		FILE(new ResourceHook() {
			@Override
			public String preHook(Set<String> cur, String s) {
				return validateFile(cur, s);
			}

			@Override
			public boolean postHook(Set<String> cur, String s) {
				return true;
			}
		}),

		JAR(new ResourceHook() {
			@Override
			public String preHook(Set<String> cur, String s) {
				String newJar = validateFile(cur, s);
				if (newJar != null) {
					return (registerJar(newJar) ? newJar : null);
				} else {
					return null;
				}
			}

			@Override
			public boolean postHook(Set<String> cur, String s) {
				return unregisterJar(s);
			}
		}),

		ARCHIVE(new ResourceHook() {
			@Override
			public String preHook(Set<String> cur, String s) {
				return validateFile(cur, s);
			}

			@Override
			public boolean postHook(Set<String> cur, String s) {
				return true;
			}
		});

		public ResourceHook hook;

		ResourceType(ResourceHook hook) {
			this.hook = hook;
		}
	};

	public static ResourceType find_resource_type(String s) {

		s = s.trim().toUpperCase();

		try {
			return ResourceType.valueOf(s);
		} catch (IllegalArgumentException e) {
		}

		// try singular
		if (s.endsWith("S")) {
			s = s.substring(0, s.length() - 1);
		} else {
			return null;
		}

		try {
			return ResourceType.valueOf(s);
		} catch (IllegalArgumentException e) {
		}
		return null;
	}

	private final HashMap<ResourceType, Set<String>> resource_map = new HashMap<ResourceType, Set<String>>();

	public String add_resource(ResourceType t, String value) {
		// By default don't convert to unix
		return add_resource(t, value, false);
	}

	public String add_resource(ResourceType t, String value,
			boolean convertToUnix) {
		try {
			value = downloadResource(value, convertToUnix);
		} catch (Exception e) {
			getConsole().printError(e.getMessage());
			return null;
		}
		Set<String> resourceMap = getResourceMap(t);

		String fnlVal = value;
		if (t.hook != null) {
			fnlVal = t.hook.preHook(resourceMap, value);
			if (fnlVal == null) {
				return fnlVal;
			}
		}
		getConsole().printInfo("Added resource: " + fnlVal);
		resourceMap.add(fnlVal);

		addedResource = true;
		return fnlVal;
	}

	public void add_builtin_resource(ResourceType t, String value) {
		getResourceMap(t).add(value);
	}

	private Set<String> getResourceMap(ResourceType t) {
		Set<String> result = resource_map.get(t);
		if (result == null) {
			result = new HashSet<String>();
			resource_map.put(t, result);
		}
		return result;
	}

	/**
	 * Returns true if it is from any external File Systems except local
	 */
	public static boolean canDownloadResource(String value) {
		// Allow to download resources from any external FileSystem.
		// And no need to download if it already exists on local file system.
		// String scheme = new Path(value).toUri().getScheme();
		String scheme = null;
		return (scheme != null) && !scheme.equalsIgnoreCase("file");
	}

	private String downloadResource(String value, boolean convertToUnix) {
		// if (canDownloadResource(value)) {
		// getConsole().printInfo("converting to local " + value);
		// File resourceDir =
		// new
		// File(getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
		// String destinationName = new Path(value).getName();
		// File destinationFile = new File(resourceDir, destinationName);
		// if (resourceDir.exists() && !resourceDir.isDirectory()) {
		// throw new RuntimeException(
		// "The resource directory is not a directory, resourceDir is set to"
		// + resourceDir);
		// }
		// if (!resourceDir.exists() && !resourceDir.mkdirs()) {
		// throw new RuntimeException("Couldn't create directory " +
		// resourceDir);
		// }
		// try {
		// FileSystem fs = FileSystem.get(new URI(value), conf);
		// fs.copyToLocalFile(new Path(value),
		// new Path(destinationFile.getCanonicalPath()));
		// value = destinationFile.getCanonicalPath();
		//
		// // add "execute" permission to downloaded resource file (needed when
		// // loading dll file)
		// FileUtil.chmod(value, "ugo+rx", true);
		// if (convertToUnix && DosToUnix.isWindowsScript(destinationFile)) {
		// try {
		// DosToUnix.convertWindowsScriptToUnix(destinationFile);
		// } catch (Exception e) {
		// throw new RuntimeException(
		// "Caught exception while converting file " + destinationFile
		// + " to unix line endings", e);
		// }
		// }
		// } catch (Exception e) {
		// throw new RuntimeException("Failed to read external resource " +
		// value,
		// e);
		// }
		// }
		return value;
	}

	public boolean delete_resource(ResourceType t, String value) {
		if (resource_map.get(t) == null) {
			return false;
		}
		if (t.hook != null) {
			if (!t.hook.postHook(resource_map.get(t), value)) {
				return false;
			}
		}
		return (resource_map.get(t).remove(value));
	}

	public Set<String> list_resource(ResourceType t, List<String> filter) {
		Set<String> orig = resource_map.get(t);
		if (orig == null) {
			return null;
		}
		if (filter == null) {
			return orig;
		} else {
			Set<String> fnl = new HashSet<String>();
			for (String one : orig) {
				if (filter.contains(one)) {
					fnl.add(one);
				}
			}
			return fnl;
		}
	}

	public void delete_resource(ResourceType t) {
		if (resource_map.get(t) != null) {
			for (String value : resource_map.get(t)) {
				delete_resource(t, value);
			}
			resource_map.remove(t);
		}
	}

	public void setStackTraces(Map<String, List<List<String>>> stackTraces) {
		this.stackTraces = stackTraces;
	}

	public Map<String, List<List<String>>> getStackTraces() {
		return stackTraces;
	}

	public Map<String, String> getOverriddenConfigurations() {
		if (overriddenConfigurations == null) {
			overriddenConfigurations = new HashMap<String, String>();
		}
		return overriddenConfigurations;
	}

	public void setOverriddenConfigurations(
			Map<String, String> overriddenConfigurations) {
		this.overriddenConfigurations = overriddenConfigurations;
	}

	public Map<String, List<String>> getLocalMapRedErrors() {
		return localMapRedErrors;
	}

	public void addLocalMapRedErrors(String id, List<String> localMapRedErrors) {
		if (!this.localMapRedErrors.containsKey(id)) {
			this.localMapRedErrors.put(id, new ArrayList<String>());
		}

		this.localMapRedErrors.get(id).addAll(localMapRedErrors);
	}

	public void close() throws IOException {
		// JavaUtils.closeClassLoadersTo(conf.getClassLoader(), parentLoader);
		String tmpDir = backtype.storm.utils.Utils.parseString(
				getConf().get(Config.STORM_LOCAL_DIR), "/tmp");
		File resourceDir = new File(tmpDir);
		LOG.debug("Removing resource dir " + resourceDir);
		try {
			if (resourceDir.exists()) {
				FileUtils.deleteDirectory(resourceDir);
			}
		} catch (IOException e) {
			LOG.info("Error removing session resource dir " + resourceDir, e);
		} finally {
			detachSession();
		}

	}

	public String getUserName() {
		return userName;
	}

	public boolean hasAddedResource() {
		return addedResource;
	}

	public void setAddedResource(boolean addedResouce) {
		this.addedResource = addedResouce;
	}
}
