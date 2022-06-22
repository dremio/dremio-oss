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
package com.dremio.exec.catalog;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestResolvedVersionContext {

  private static final String BRANCH_NAME = "branchName";
  private static final String TAG_NAME = "tagName";
  private static final String REASONABLE_HASH = "0123456789ABCDEFabcdef";

  @Test
  public void branch() {
    ResolvedVersionContext version = ResolvedVersionContext.ofBranch(BRANCH_NAME, REASONABLE_HASH);

    assertTrue(version.isBranch());
    assertFalse(version.isBareCommit());

    assertEquals(ResolvedVersionContext.Type.BRANCH, version.getType());
    assertEquals(BRANCH_NAME, version.getRefName());
    assertEquals(REASONABLE_HASH, version.getCommitHash());
  }

  @Test
  public void tag() {
    ResolvedVersionContext version = ResolvedVersionContext.ofTag(TAG_NAME, REASONABLE_HASH);

    assertFalse(version.isBranch());
    assertFalse(version.isBareCommit());

    assertEquals(ResolvedVersionContext.Type.TAG, version.getType());
    assertEquals(TAG_NAME, version.getRefName());
    assertEquals(REASONABLE_HASH, version.getCommitHash());
  }

  @Test
  public void bareCommit() {
    ResolvedVersionContext version = ResolvedVersionContext.ofBareCommit(REASONABLE_HASH);

    assertFalse(version.isBranch());
    assertTrue(version.isBareCommit());

    assertEquals(ResolvedVersionContext.Type.BARE_COMMIT, version.getType());
    assertEquals("DETACHED", version.getRefName());
    assertEquals(REASONABLE_HASH, version.getCommitHash());
  }

  @Test
  public void nullHash() {
    assertThatThrownBy(() -> ResolvedVersionContext.ofBranch(BRANCH_NAME, null))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> ResolvedVersionContext.ofTag(TAG_NAME, null))
      .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> ResolvedVersionContext.ofBareCommit(null))
      .isInstanceOf(NullPointerException.class);
  }

}
