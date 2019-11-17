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
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.service.users.SystemUser;

/**
 * Tests for ImpersonationUtil
 */
public class TestImpersonationUtil  {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNullUser() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    ImpersonationUtil.createProxyUgi(null);
  }

  @Test
  public void testEmptyUser() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    ImpersonationUtil.createProxyUgi("");
  }

  @Test
  public void testIsSystemUserName() throws Exception {
    assertTrue(SystemUser.isSystemUserName(SYSTEM_USERNAME));
    assertFalse(SystemUser.isSystemUserName("foo"));
  }
}
