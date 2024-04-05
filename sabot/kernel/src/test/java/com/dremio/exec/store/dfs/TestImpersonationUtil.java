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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.service.users.SystemUser;
import org.junit.Test;

/** Tests for ImpersonationUtil */
public class TestImpersonationUtil {

  @Test
  public void testNullUser() {
    assertThatThrownBy(() -> ImpersonationUtil.createProxyUgi(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testEmptyUser() {
    assertThatThrownBy(() -> ImpersonationUtil.createProxyUgi(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testIsSystemUserName() {
    assertTrue(SystemUser.isSystemUserName(SYSTEM_USERNAME));
    assertFalse(SystemUser.isSystemUserName("foo"));
  }
}
