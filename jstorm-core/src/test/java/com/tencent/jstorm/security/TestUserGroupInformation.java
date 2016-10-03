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
package com.tencent.jstorm.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.junit.Test;

import com.tencent.jstorm.security.UserGroupInformation.AuthenticationMethod;

public class TestUserGroupInformation {

  final private static String USER_NAME = "user1@storm.tencent.com";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME, GROUP3_NAME };

  @Test
  public void testConfigUgi() throws IOException {
    String ugi = "storm,storm";
    String user = ugi.split(",")[0];
    UserGroupInformation newUser =
        UserGroupInformation.createProxyUser(user,
            UserGroupInformation.getCurrentUser());
    newUser.setAuthenticationMethod(AuthenticationMethod.PROXY);
    assertEquals("storm", newUser.getUserName());

    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(user);
    remoteUser.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
    assertEquals("storm", remoteUser.getUserName());
  }

  @Test
  public void testLogin() throws Exception {
    // login from unix
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    assertEquals(UserGroupInformation.getCurrentUser(),
        UserGroupInformation.getLoginUser());
    // assertTrue(ugi.getGroupNames().length >= 1);

    // ensure that doAs works correctly
    UserGroupInformation userGroupInfo =
        UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES);

    UserGroupInformation curUGI =
        userGroupInfo
            .doAs(new PrivilegedExceptionAction<UserGroupInformation>() {
              public UserGroupInformation run() throws IOException {
                return UserGroupInformation.getCurrentUser();
              }
            });
    // make sure in the scope of the doAs, the right user is current
    assertEquals(curUGI, userGroupInfo);
    // make sure it is not the same as the login user
    assertFalse(curUGI.equals(UserGroupInformation.getLoginUser()));
  }

}
