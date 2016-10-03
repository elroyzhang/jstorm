package backtype.storm.security;

import java.security.Principal;

import com.tencent.jstorm.security.UserGroupInformation.AuthenticationMethod;

public class User implements Principal {
  private final String fullName;
  private final String shortName;
  private AuthenticationMethod authMethod = null;

  public User(String name) {
    this(name, null);
  }

  public User(String name, AuthenticationMethod authMethod) {
    fullName = name;
    int atIdx = name.indexOf('@');
    if (atIdx == -1) {
      shortName = name;
    } else {
      int slashIdx = name.indexOf('/');
      if (slashIdx == -1 || atIdx < slashIdx) {
        shortName = name.substring(0, atIdx);
      } else {
        shortName = name.substring(0, slashIdx);
      }
    }
    this.authMethod = authMethod;
  }

  /**
   * Get the full name of the user.
   */
  @Override
  public String getName() {
    return fullName;
  }

  /**
   * Get the user name up to the first '/' or '@'
   * 
   * @return the leading part of the user name
   */
  public String getShortName() {
    return shortName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return ((fullName.equals(((User) o).fullName)) && (authMethod == ((User) o).authMethod));
    }
  }

  @Override
  public int hashCode() {
    return fullName.hashCode();
  }

  @Override
  public String toString() {
    return fullName;
  }

  public void setAuthenticationMethod(AuthenticationMethod authMethod) {
    this.authMethod = authMethod;
  }

  public AuthenticationMethod getAuthenticationMethod() {
    return authMethod;
  }
}
