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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * The class responsible to test methods if the methods from {@link HiveAuthorizationHelper} class
 * are being called correctly.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestHiveAuthorizationHelper {

  @Mock
  HiveAuthorizationHelper helperMock;

  /**
   * Sets the behavior of the {@link HiveAuthorizationHelper#isAuthEnabled()} method to return true.
   */
  private void authTrue() {
    when(helperMock.isAuthEnabled()).thenReturn(true);
  }

  /**
   * Sets the behavior of the {@link HiveAuthorizationHelper#isAuthEnabled()} method to return false.
   */
  private void authFalse() {
    when(helperMock.isAuthEnabled()).thenReturn(false);
  }

  /**
   * A helper to check if the {@link HiveAuthorizationHelper#authorize(HiveOperationType, List, List, String)}
   * is being called. If the shouldBeCalled variable is true, the method authorize is supposed to be called,
   * otherwise don't.
   * @param shouldBeCalled    A boolean controller to check if the method should be called.
   * @throws HiveAccessControlException in case of error.
   */
  private void verifyAuthorizeCall(boolean shouldBeCalled) throws HiveAccessControlException {
    if (shouldBeCalled) {
      verify(helperMock).authorize(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    } else {
      verify(helperMock, never()).authorize(any(), any(), any(), any());
    }
  }

  @Test
  public void checkAuthEnableOnAuthorizeShowDatabases() throws HiveAccessControlException {
    authTrue();
    doCallRealMethod().when(helperMock).authorizeShowDatabases();
    helperMock.authorizeShowDatabases();

    verifyAuthorizeCall(true);
  }

  @Test
  public void checkAuthDisableOnAuthorizeShowDatabases() throws HiveAccessControlException {
    authFalse();
    doCallRealMethod().when(helperMock).authorizeShowTables(any());
    helperMock.authorizeShowTables("_db_test_");

    verifyAuthorizeCall(false);

  }

  @Test
  public void checkAuthEnableOnAuthorizeShowTables() throws HiveAccessControlException {
    authTrue();
    doCallRealMethod().when(helperMock).authorizeShowTables(any());
    helperMock.authorizeShowTables("_db_test_");

    verifyAuthorizeCall(true);
  }

  @Test
  public void checkAuthDisableOnAuthorizeShowTables() throws HiveAccessControlException {
    authFalse();
    doCallRealMethod().when(helperMock).authorizeShowTables(any());
    helperMock.authorizeShowTables("_db_test_");

    verifyAuthorizeCall(false);
  }

  @Test
  public void checkAuthEnableOnAuthorizeReadTable() throws HiveAccessControlException {
    authTrue();
    doCallRealMethod().when(helperMock).authorizeReadTable(any(), any());
    helperMock.authorizeReadTable("_dbTest_", "_table_name");

    verifyAuthorizeCall(true);
  }

  @Test
  public void checkAuthDisableOnAuthorizeReadTable() throws HiveAccessControlException {
    authFalse();
    doCallRealMethod().when(helperMock).authorizeReadTable(any(), any());
    helperMock.authorizeReadTable("_dbTest_", "_table_name");
    verifyAuthorizeCall(false);
  }
}
