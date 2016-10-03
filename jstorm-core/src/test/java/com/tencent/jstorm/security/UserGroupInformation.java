package com.tencent.jstorm.security;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.log4j.Logger;
import backtype.storm.security.User;
import com.sun.security.auth.NTUserPrincipal;
import com.sun.security.auth.UnixPrincipal;
import com.sun.security.auth.module.Krb5LoginModule;

import backtype.storm.security.Groups;

public class UserGroupInformation {
  private static final Logger LOG =
      Logger.getLogger(UserGroupInformation.class);

  public static class StormLoginModule implements LoginModule {
    private Subject subject;

    @Override
    public boolean abort() throws LoginException {
      return true;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) {
      for (T user : subject.getPrincipals(cls)) {
        return user;
      }
      return null;
    }

    @Override
    public boolean commit() throws LoginException {
      Principal user = null;
      // if we are using kerberos, try it out
      if (useKerberos) {
        user = getCanonicalUser(KerberosPrincipal.class);
      }
      // if we don't have a kerberos user, use the OS user
      if (user == null) {
        user = getCanonicalUser(OS_PRINCIPAL_CLASS);
      }
      // if we found the user, add our principal
      if (user != null) {
        subject.getPrincipals().add(new User(user.getName()));
        return true;
      }
      LOG.error("Can't find user in " + subject);
      throw new LoginException("Can't find user name");
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
        Map<String, ?> sharedState, Map<String, ?> options) {
      this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      return true;
    }
  }

  /** Are the static variables that depend on configuration initialized? */
  private static boolean isInitialized = false;
  /** Should we use Kerberos configuration? */
  private static boolean useKerberos;
  /** Server-side groups fetching service */
  private static Groups groups;
  /** The last authentication time */
  private static long lastUnsuccessfulAuthenticationAttemptTime;

  public static final long MIN_TIME_BEFORE_RELOGIN = 10 * 60 * 1000L;

  /** Environment variable pointing to the token cache file */
  public static final String STORM_TOKEN_FILE_LOCATION =
      "STORM_TOKEN_FILE_LOCATION";

  /**
   * A method to initialize the fields that depend on a configuration. Must be
   * called before useKerberos or groups is used.
   */
  private static synchronized void ensureInitialized() {
    if (!isInitialized) {
      initialize(new HashMap());
    }
  }

  private static synchronized void initialize(Map conf) {
    String value = "simple";// conf.get(STORM_SECURITY_AUTHENTICATION);
    if (value == null || "simple".equals(value)) {
      useKerberos = false;
    } else if ("kerberos".equals(value)) {
      useKerberos = true;
    } else {
      throw new IllegalArgumentException(
          "Invalid attribute value for " + value + " of " + value);
    }
    javax.security.auth.login.Configuration
        .setConfiguration(new StormConfiguration());
    isInitialized = true;
  }

  /**
   * Set the static configuration for UGI. In particular, set the security
   * authentication mechanism and the group look up service.
   * 
   * @param conf the configuration to use
   */
  public static void setConfiguration(Map conf) {
    initialize(conf);
  }

  public static boolean isSecurityEnabled() {
    ensureInitialized();
    return useKerberos;
  }

  private static UserGroupInformation loginUser = null;
  private static String keytabPrincipal = null;
  private static String keytabFile = null;

  private final Subject subject;

  private static LoginContext login;

  private static final String OS_LOGIN_MODULE_NAME;
  private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;
  private static final boolean windows =
      System.getProperty("os.name").startsWith("Windows");

  static {
    if (windows) {
      OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.NTLoginModule";
      OS_PRINCIPAL_CLASS = NTUserPrincipal.class;
    } else {
      OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.UnixLoginModule";
      OS_PRINCIPAL_CLASS = UnixPrincipal.class;
    }
  }

  private static class RealUser implements Principal {
    private final UserGroupInformation realUser;

    RealUser(UserGroupInformation realUser) {
      this.realUser = realUser;
    }

    public String getName() {
      return realUser.getUserName();
    }

    public UserGroupInformation getRealUser() {
      return realUser;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      } else {
        return realUser.equals(((RealUser) o).realUser);
      }
    }

    @Override
    public int hashCode() {
      return realUser.hashCode();
    }

    @Override
    public String toString() {
      return realUser.toString();
    }
  }

  private static class StormConfiguration
      extends javax.security.auth.login.Configuration {
    private static final String SIMPLE_CONFIG_NAME = "storm-simple";
    private static final String USER_KERBEROS_CONFIG_NAME =
        "storm-user-kerberos";
    private static final String KEYTAB_KERBEROS_CONFIG_NAME =
        "storm-keytab-kerberos";

    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
        new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
            LoginModuleControlFlag.REQUIRED, new HashMap<String, String>());
    private static final AppConfigurationEntry STORM_LOGIN =
        new AppConfigurationEntry(StormLoginModule.class.getName(),
            LoginModuleControlFlag.REQUIRED, new HashMap<String, String>());
    private static final Map<String, String> USER_KERBEROS_OPTIONS =
        new HashMap<String, String>();

    static {
      USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
      USER_KERBEROS_OPTIONS.put("renewTGT", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
      }
    }

    private static final AppConfigurationEntry USER_KERBEROS_LOGIN =
        new AppConfigurationEntry(Krb5LoginModule.class.getName(),
            LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS);
    private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS =
        new HashMap<String, String>();

    static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
    }

    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
        new AppConfigurationEntry(Krb5LoginModule.class.getName(),
            LoginModuleControlFlag.REQUIRED, KEYTAB_KERBEROS_OPTIONS);

    private static final AppConfigurationEntry[] SIMPLE_CONF =
        new AppConfigurationEntry[] { OS_SPECIFIC_LOGIN, STORM_LOGIN };

    private static final AppConfigurationEntry[] USER_KERBEROS_CONF =
        new AppConfigurationEntry[] { OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN,
            STORM_LOGIN };

    private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
        new AppConfigurationEntry[] { KEYTAB_KERBEROS_LOGIN, STORM_LOGIN };

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (SIMPLE_CONFIG_NAME.equals(appName)) {
        return SIMPLE_CONF;
      } else if (USER_KERBEROS_CONFIG_NAME.equals(appName)) {
        return USER_KERBEROS_CONF;
      } else if (KEYTAB_KERBEROS_CONFIG_NAME.equals(appName)) {
        KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
        KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
        return KEYTAB_KERBEROS_CONF;
      }
      return null;
    }
  }

  UserGroupInformation(Subject subject) {
    this.subject = subject;
  }

  public static UserGroupInformation getCurrentUser() throws IOException {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    return subject == null ? getLoginUser() : new UserGroupInformation(subject);
  }

  public synchronized static UserGroupInformation getLoginUser()
      throws IOException {
    if (loginUser == null) {
      try {
        if (isSecurityEnabled()) {
          login =
              new LoginContext(StormConfiguration.USER_KERBEROS_CONFIG_NAME);
        } else {
          login = new LoginContext(StormConfiguration.SIMPLE_CONFIG_NAME);
        }
        login.login();
        loginUser = new UserGroupInformation(login.getSubject());
      } catch (LoginException le) {
        throw new IOException("failure to login", le);
      }
    }
    return loginUser;
  }

  public static UserGroupInformation createRemoteUser(String user) {
    if (user == null || "".equals(user)) {
      throw new IllegalArgumentException("Null user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return new UserGroupInformation(subject);
  }

  public static enum AuthenticationMethod {
    SIMPLE, KERBEROS, TOKEN, CERTIFICATE, KERBEROS_SSL, PROXY;
  }

  public static UserGroupInformation createProxyUser(String user,
      UserGroupInformation realUser) {
    if (user == null || "".equals(user)) {
      throw new IllegalArgumentException("Null user");
    }
    if (realUser == null) {
      throw new IllegalArgumentException("Null real user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    subject.getPrincipals().add(new RealUser(realUser));
    return new UserGroupInformation(subject);
  }

  public UserGroupInformation getRealUser() {
    for (RealUser p : subject.getPrincipals(RealUser.class)) {
      return p.getRealUser();
    }
    return null;
  }

  public String getShortUserName() {
    for (User p : subject.getPrincipals(User.class)) {
      return p.getShortName();
    }
    return null;
  }

  public String getUserName() {
    for (User p : subject.getPrincipals(User.class)) {
      return p.getName();
    }
    return null;
  }

  @Override
  public String toString() {
    if (getRealUser() != null) {
      return getUserName() + " via " + getRealUser().toString();
    } else {
      return getUserName();
    }
  }

  public synchronized void setAuthenticationMethod(
      AuthenticationMethod authMethod) {
    for (User p : subject.getPrincipals(User.class)) {
      p.setAuthenticationMethod(authMethod);
    }
  }

  public synchronized AuthenticationMethod getAuthenticationMethod() {
    for (User p : subject.getPrincipals(User.class)) {
      return p.getAuthenticationMethod();
    }
    return null;
  }

  /**
   * Compare the subjects to see if they are equal to each other.
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return subject.equals(((UserGroupInformation) o).subject);
    }
  }

  /**
   * Return the hash of the subject.
   */
  @Override
  public int hashCode() {
    return subject.hashCode();
  }

  /**
   * Get the underlying subject from this ugi.
   * 
   * @return the subject that represents this user.
   */
  protected Subject getSubject() {
    return subject;
  }

  public <T> T doAs(PrivilegedAction<T> action) {
    return Subject.doAs(subject, action);
  }

  /**
   * Run the given action as the user, potentially throwing an exception.
   * 
   * @param <T> the return type of the run method
   * @param action the method to execute
   * @return the value from the run method
   * @throws IOException if the action throws an IOException
   * @throws Error if the action throws an Error
   * @throws RuntimeException if the action throws a RuntimeException
   * @throws InterruptedException if the action throws an InterruptedException
   * @throws UndeclaredThrowableException if the action throws something else
   */
  public <T> T doAs(PrivilegedExceptionAction<T> action)
      throws IOException, InterruptedException {
    try {
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else {
        throw new UndeclaredThrowableException(pae,
            "Unknown exception in doAs");
      }
    }
  }

  private void print() throws IOException {
    System.out.println("User: " + getUserName());
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Getting UGI for current user");
    UserGroupInformation ugi = getCurrentUser();
    ugi.print();
    System.out.println("UGI: " + ugi);
  }

  public synchronized String[] getGroupNames() {
    ensureInitialized();
    try {
      List<String> result = groups.getGroups(getShortUserName());
      return result.toArray(new String[result.size()]);
    } catch (IOException ie) {
      LOG.warn("No groups available for user " + getShortUserName());
      return new String[0];
    }
  }

  public static UserGroupInformation createUserForTesting(String user,
      String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createRemoteUser(user);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups();
    }
    // add the user groups
    ((TestingGroups) groups).setUserGroups(ugi.getShortUserName(), userGroups);
    return ugi;
  }

  private static class TestingGroups extends Groups {
    private final Map<String, List<String>> userToGroupsMapping =
        new HashMap<String, List<String>>();

    private TestingGroups() {
      super();
    }

    @Override
    public List<String> getGroups(String user) {
      List<String> result = userToGroupsMapping.get(user);
      if (result == null) {
        result = new ArrayList<String>();
      }
      return result;
    }

    private void setUserGroups(String user, String[] groups) {
      userToGroupsMapping.put(user, Arrays.asList(groups));
    }
  }

}
