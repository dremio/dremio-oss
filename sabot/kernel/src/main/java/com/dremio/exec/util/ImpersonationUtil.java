/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.util;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Utilities for impersonation purpose.
 */
public class ImpersonationUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImpersonationUtil.class);

  private static final LoadingCache<Key, UserGroupInformation> CACHE = CacheBuilder.newBuilder()
      .maximumSize(100)
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .build(new CacheLoader<Key, UserGroupInformation>() {
        @Override
        public UserGroupInformation load(Key key) throws Exception {
          return UserGroupInformation.createProxyUser(key.proxyUserName, key.loginUser);
        }
      });

  private static class Key {
    final String proxyUserName;
    final UserGroupInformation loginUser;

    public Key(String proxyUserName, UserGroupInformation loginUser) {
      super();
      this.proxyUserName = proxyUserName;
      this.loginUser = loginUser;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((loginUser == null) ? 0 : loginUser.hashCode());
      result = prime * result + ((proxyUserName == null) ? 0 : proxyUserName.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Key other = (Key) obj;
      if (loginUser == null) {
        if (other.loginUser != null) {
          return false;
        }
      } else if (!loginUser.equals(other.loginUser)) {
        return false;
      }
      if (proxyUserName == null) {
        if (other.proxyUserName != null) {
          return false;
        }
      } else if (!proxyUserName.equals(other.proxyUserName)) {
        return false;
      }
      return true;
    }


  }

  /**
   * If the given user is {@link SystemUser#SYSTEM_USERNAME}, return the current process user name. System user is
   * an internal user, when communicating with external entities we use the process username.
   *
   * @param username
   */
  public static String resolveUserName(String username) {
    if (SYSTEM_USERNAME.equals(username)) {
      return getProcessUserName();
    }

    return username;
  }

  /**
   * Create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} of operator owner if operator
   * owner is valid. Otherwise create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} for
   * query user.
   *
   * @param opUserName Name of the user whom to impersonate while setting up the operator.
   * @param queryUserName Name of the user who issues the query. If <i>opUserName</i> is invalid,
   *                      then this parameter must be valid user name.
   * @return
   */
  public static UserGroupInformation createProxyUgi(String opUserName, String queryUserName) {
    if (!Strings.isNullOrEmpty(opUserName)) {
      return createProxyUgi(opUserName);
    }

    if (Strings.isNullOrEmpty(queryUserName)) {
      // TODO(DRILL-2097): Tests that use SimpleRootExec have don't assign any query user name in FragmentContext.
      // Disable throwing exception to modifying the long list of test files.
      // throw new RuntimeException("Invalid value for query user name");
      return getProcessUserUGI();
    }

    return createProxyUgi(queryUserName);
  }

  /**
   * Create and return proxy user {@link org.apache.hadoop.security.UserGroupInformation} for the given user name.  If
   * username is empty/null it throws a IllegalArgumentException.
   *
   * @param proxyUserName Proxy user name (must be valid)
   * @return
   */
  public static UserGroupInformation createProxyUgi(String proxyUserName) {
    try {
      if (Strings.isNullOrEmpty(proxyUserName)) {
        throw new IllegalArgumentException("Invalid value for proxy user name");
      }

      // If the request proxy user is same as process user name or same as system user, return the process UGI.
      if (proxyUserName.equals(getProcessUserName()) || SYSTEM_USERNAME.equals(proxyUserName)) {
        return getProcessUserUGI();
      }

      return CACHE.get(new Key(proxyUserName, UserGroupInformation.getLoginUser()));
    } catch (IOException | ExecutionException e) {
      final String errMsg = "Failed to create proxy user UserGroupInformation object: " + e.getMessage();
      logger.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
  }

  /**
   * Return the name of the user who is running the SabotNode.
   *
   * @return SabotNode process user.
   */
  public static String getProcessUserName() {
    return getProcessUserUGI().getUserName();
  }

  /**
   * Return the {@link org.apache.hadoop.security.UserGroupInformation} of user who is running the SabotNode.
   *
   * @return SabotNode process user {@link org.apache.hadoop.security.UserGroupInformation}.
   */
  public static UserGroupInformation getProcessUserUGI() {
    try {
      return UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      final String errMsg = "Failed to get process user UserGroupInformation object.";
      logger.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
  }

  /**
   * Create FileSystemWrapper for given <i>proxyUserName</i> and configuration.
   *
   * @param proxyUserName Name of the user whom to impersonate while accessing the FileSystem contents.
   * @param fsConf FileSystem configuration.
   * @return
   */
  public static FileSystemWrapper createFileSystem(String proxyUserName, Configuration fsConf) {
    return createFileSystem(createProxyUgi(proxyUserName), fsConf, null, null);
  }

  /** Helper method to create FileSystemWrapper */
  public static FileSystemWrapper createFileSystem(UserGroupInformation proxyUserUgi, final Configuration fsConf,
      final OperatorStats stats, final List<String> connectionUniqueProps) {
    FileSystemWrapper fs;
    try {
      fs = proxyUserUgi.doAs(new PrivilegedExceptionAction<FileSystemWrapper>() {
        @Override
        public FileSystemWrapper run() throws Exception {
          logger.trace("Creating FileSystemWrapper for proxy user: " + UserGroupInformation.getCurrentUser());
          return new FileSystemWrapper(fsConf, stats, connectionUniqueProps);
        }
      });
    } catch (InterruptedException | IOException e) {
      final String errMsg = "Failed to create FileSystemWrapper for proxy user: " + e.getMessage();
      logger.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }

    return fs;
  }

  /**
   * Create FileSystemWrapper for given <i>proxyUserName</i> and configuration and path
   *
   * @param proxyUserName Name of the user whom to impersonate while accessing the FileSystem contents.
   * @param fsConf FileSystem configuration.
   * @param path path for which fileystem is to be created
   * @return
   */
  public static FileSystemWrapper createFileSystem(String proxyUserName, Configuration fsConf, Path path) {
    return createFileSystem(createProxyUgi(proxyUserName), fsConf, path);
  }

  private static FileSystemWrapper createFileSystem(UserGroupInformation proxyUserUgi, final Configuration fsConf, final Path path) {
    FileSystemWrapper fs;
    try {
      fs = proxyUserUgi.doAs(new PrivilegedExceptionAction<FileSystemWrapper>() {
        @Override
        public FileSystemWrapper run() throws Exception {
          logger.trace("Creating FileSystemWrapper for proxy user: " + UserGroupInformation.getCurrentUser());
          return FileSystemWrapper.get(path, fsConf);
        }
      });
    } catch (InterruptedException | IOException e) {
      final String errMsg = "Failed to create FileSystemWrapper for proxy user: " + e.getMessage();
      logger.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }

    return fs;
  }

  // avoid instantiation
  private ImpersonationUtil() {
  }
}
