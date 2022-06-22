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
package com.dremio.exec.store.hive;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.dremio.common.util.Closeable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

/**
 * Helper class for initializing and checking privileges according to authorization configuration set in Hive storage
 * plugin config.
 */
public class HiveAuthorizationHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveAuthorizationHelper.class);

  final HiveAuthorizer authorizerV2;

  public HiveAuthorizationHelper(final IMetaStoreClient mClient, final HiveConf hiveConf, final String user) {
    boolean authEnabled = hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED);
    if (!authEnabled) {
      authorizerV2 = null;
      return;
    }

    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final HiveConf hiveConfCopy = new HiveConf(hiveConf);
      hiveConfCopy.set("user.name", user);
      hiveConfCopy.set("proxy.user.name", user);

      final HiveAuthenticationProvider authenticator = HiveUtils.getAuthenticator(hiveConfCopy,
          HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER);

      // This must be retrieved before creating the session state, because creation of the
      // session state changes the given HiveConf's classloader to a UDF ClassLoader.
      final HiveAuthorizerFactory authorizerFactory =
        HiveUtils.getAuthorizerFactory(hiveConfCopy, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);

      SessionState ss = new SessionState(hiveConfCopy, user);
      authenticator.setSessionState(ss);

      HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();
      authzContextBuilder.setClientType(CLIENT_TYPE.HIVESERVER2); // Dremio is emulating HS2 here

      authorizerV2 = authorizerFactory.createHiveAuthorizer(
          new HiveMetastoreClientFactory() {
            @Override
            public IMetaStoreClient getHiveMetastoreClient() throws HiveAuthzPluginException {
              return mClient;
            }
          },
          hiveConf, authenticator, authzContextBuilder.build());

      authorizerV2.applyAuthorizationConfigPolicy(hiveConfCopy);
    } catch (final HiveException e) {
      throw new RuntimeException("Failed to initialize Hive authorization components: " + e.getMessage(), e);
    }

    logger.trace("Hive authorization enabled");
  }

  /**
   * Check authorization for "SHOW DATABASES" command. A {@link HiveAccessControlException} is thrown
   * for illegal access.
   */
  public void authorizeShowDatabases() throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    authorize(HiveOperationType.SHOWDATABASES, Collections.<HivePrivilegeObject> emptyList(), Collections.<HivePrivilegeObject> emptyList(), "SHOW DATABASES");
  }


  /**
   * Check authorization for "CREATE TABLE" command. A {@link HiveAccessControlException} is thrown
   * for illegal access.
   */
  public void authorizeCreateTable(final String dbName, final String tableName) throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    HivePrivilegeObject toCreate = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    authorize(HiveOperationType.CREATETABLE, Collections.<HivePrivilegeObject>emptyList(), ImmutableList.of(toCreate), "CREATE TABLE");
  }

  /**
   * Check authorization for "DROP TABLE" command. A {@link HiveAccessControlException} is thrown
   * for illegal access.
   */
  public void authorizeDropTable(final String dbName, final String tableName) throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    HivePrivilegeObject toDrop = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    authorize(HiveOperationType.DROPTABLE, ImmutableList.of(toDrop), Collections.<HivePrivilegeObject>emptyList(), "DROP TABLE");
  }

  /**
   * Check authorization for "SHOW TABLES" command in given Hive db. A {@link HiveAccessControlException} is thrown
   * for illegal access.
   * @param dbName
   */
  public void authorizeShowTables(final String dbName) throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    final HivePrivilegeObject toRead = new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbName, null);

    authorize(HiveOperationType.SHOWTABLES, ImmutableList.of(toRead), Collections.<HivePrivilegeObject> emptyList(), "SHOW TABLES");
  }

  /**
   * Check authorization for "READ TABLE" for given db.table. A {@link HiveAccessControlException} is thrown
   * for illegal access.
   * @param dbName
   * @param tableName
   */
  public void authorizeReadTable(final String dbName, final String tableName) throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    HivePrivilegeObject toRead = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    authorize(HiveOperationType.QUERY, ImmutableList.of(toRead), Collections.<HivePrivilegeObject> emptyList(), "READ TABLE");
  }

  public void authorizeDml(String dbName, String tableName, List<HivePrivObjectActionType> actionTypes)
      throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    List<HivePrivilegeObject> input = ImmutableList.of(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName));
    List<HivePrivilegeObject> output = actionTypes.stream()
        .map(type -> new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName, type))
        .collect(Collectors.toList());
    // LOAD is used here as there are no HiveOperationTypes for SQL DML... and LOAD will map to a table update
    authorize(HiveOperationType.LOAD, input, output, "LOAD");
  }

  public void authorizeTruncateTable(String dbName, String tableName)
      throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    HivePrivilegeObject toTruncate = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    authorize(HiveOperationType.TRUNCATETABLE, Collections.emptyList(), ImmutableList.of(toTruncate), "TRUNCATE TABLE");
  }

  public void authorizeAlterTable(String dbName, String tableName)
    throws HiveAccessControlException, HiveAuthzPluginException {
    if (!isAuthEnabled()) {
      return;
    }

    HivePrivilegeObject toAlter = new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    // Hive has multiple ALTERTABLE operation types - just map to one of them for simplicity
    authorize(HiveOperationType.ALTERTABLE_ADDCOLS, Collections.emptyList(), ImmutableList.of(toAlter), "ALTER TABLE");
  }

  @VisibleForTesting
  /* Helper method to check privileges */
  void authorize(final HiveOperationType hiveOpType, final List<HivePrivilegeObject> toRead,
      final List<HivePrivilegeObject> toWrite, final String cmd) throws HiveAccessControlException, HiveAuthzPluginException {
    try {
      HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
      authzContextBuilder.setUserIpAddress("Not available");
      authzContextBuilder.setCommandString(cmd);

      authorizerV2.checkPrivileges(hiveOpType, toRead, toWrite, authzContextBuilder.build());
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      throw e;
    } catch (final Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException("Failed to use the Hive authorization components: " + e.getMessage(), e);
    }
  }

  @VisibleForTesting
  boolean isAuthEnabled() {
    return null != authorizerV2;
  }

  HiveAuthorizer getAuthorizer() {
    assert null != authorizerV2;
    return authorizerV2;
  }
}
