package backtype.storm.security;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Groups {
  private static final Logger LOG = LoggerFactory.getLogger(Groups.class);

  private GroupMappingServiceProvider impl = null;

  private final Map<String, CachedGroups> userToGroupsMap =
      new ConcurrentHashMap<String, CachedGroups>();

  public Groups() {
    try {
      impl =
          this.newInstance("backtype.storm.security.ShellBasedUnixGroupsMapping");
      LOG.info("Group mapping impl=" + impl.getClass().getName());
    } catch (ClassNotFoundException e) {
      LOG.warn("Class not found exception {} ", e.getMessage());
    }
  }

  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  public static <T> T newInstance(String classStr)
      throws ClassNotFoundException {
    T result;

    Class<T> theClass = (Class<T>) Class.forName(classStr);
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Get the group memberships of a given user.
   * 
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException
   */
  public List<String> getGroups(String user) throws IOException {
    // Return cached value if available
    CachedGroups groups = userToGroupsMap.get(user);
    long now = System.currentTimeMillis();
    // if cache has a value and it hasn't expired
    if (groups != null && (groups.getTimestamp() + 300 * 1000 > now)) {
      LOG.debug("Returning cached groups for '" + user + "'");
      return groups.getGroups();
    }

    // Create and cache user's groups
    groups = new CachedGroups(impl.getGroups(user));
    userToGroupsMap.put(user, groups);
    LOG.debug("Returning fetched groups for '" + user + "'");
    return groups.getGroups();
  }

  /**
   * Refresh all user-to-groups mappings.
   */
  public void refresh() {
    LOG.info("clearing userToGroupsMap cache");
    userToGroupsMap.clear();
  }

  private static class CachedGroups {
    final long timestamp;
    final List<String> groups;

    CachedGroups(List<String> groups) {
      this.groups = groups;
      this.timestamp = System.currentTimeMillis();
    }

    public long getTimestamp() {
      return timestamp;
    }

    public List<String> getGroups() {
      return groups;
    }
  }
}
