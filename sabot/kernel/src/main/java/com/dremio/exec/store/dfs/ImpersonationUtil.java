/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.service.cache.AuthorizationCacheException;
import com.dremio.exec.service.cache.AuthorizationCacheService;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Utilities for impersonation purpose.
 */
public final class ImpersonationUtil implements AuthorizationCacheService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImpersonationUtil.class);

  private static final LoadingCache<Key, UserGroupInformation> CACHE = CacheBuilder.newBuilder()
      .maximumSize(100)
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .build(new CacheLoader<Key, UserGroupInformation>(){
        @Override
        public UserGroupInformation load(Key key) throws Exception {
          return UserGroupInformation.createProxyUser(key.proxyUserName, key.loginUser);
        }
      });

  private static class Key {
    private final String proxyUserName;
    private final UserGroupInformation loginUser;

    public Key(String proxyUserName, UserGroupInformation loginUser) {
      this.proxyUserName = proxyUserName;
      this.loginUser = loginUser;
    }

    @Override
    public int hashCode() {
      return Objects.hash(proxyUserName, loginUser);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Key)) {
        return false;
      }
      final Key that = (Key) obj;
      return Objects.equals(this.proxyUserName,  that.proxyUserName)
          && Objects.equals(this.loginUser, that.loginUser);
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
   * Invalidate all proxy users {@link org.apache.hadoop.security.UserGroupInformation}.
   *
   * @return
   */
  public static void deleteAllAuthorizationCache() throws AuthorizationCacheException {
    CACHE.invalidateAll();
  }

  /**
   * Invalidate the proxy user {@link org.apache.hadoop.security.UserGroupInformation} for the given user name.  If
   * username is empty/null it throws a IllegalArgumentException.
   *
   * @param proxyUserName Proxy user name (must be valid)
   * @return
   */
  public static void deleteAuthorizationCache(String proxyUserName) throws AuthorizationCacheException {
    if (Strings.isNullOrEmpty(proxyUserName)) {
      throw new AuthorizationCacheException("Invalid value for proxy user name");
    }
    try {
      CACHE.invalidate(new Key(proxyUserName, UserGroupInformation.getLoginUser()));
    } catch (IOException ex) {
      logger.debug(ex.getMessage());
      throw new AuthorizationCacheException(ex.getMessage());
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
   * Create FileSystemWrapper for given <i>proxyUserName</i> and configuration and path
   *
   * @param proxyUserName Name of the user whom to impersonate while accessing the FileSystem contents.
   * @param fsConf FileSystem configuration.
   * @param path path for which fileystem is to be created
   * @return
   */
  public static FileSystem createFileSystem(String proxyUserName, Configuration fsConf, Path path) {
    return createFileSystem(createProxyUgi(proxyUserName), fsConf, path);
  }

  private static FileSystem createFileSystem(UserGroupInformation proxyUserUgi, final Configuration fsConf,
                                             final Path path) {
    FileSystem fs;
    try {
      fs = proxyUserUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          logger.trace("Creating FileSystemWrapper for proxy user: " + UserGroupInformation.getCurrentUser());
          return HadoopFileSystem.get(path, fsConf);
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
