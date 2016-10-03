package backtype.storm.security;

import java.io.IOException;
import java.util.List;

/**
 * An interface for the implementation of a user-to-groups mapping service used
 * by {@link Groups}.
 */

interface GroupMappingServiceProvider {

  /**
   * Get all various group memberships of a given user. Returns EMPTY list in
   * case of non-existing user
   * 
   * @param user User's name
   * @return group memberships of user
   * @throws IOException
   */
  public List<String> getGroups(String user) throws IOException;
}
