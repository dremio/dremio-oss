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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

public class TestVersionContext {
  private static final String NOT_HEXADECIMAL = "nothexadecimal";
  private static final String EMPTY_STRING = "";
  private static final String REASONABLE_HASH = "0123456789ABCDEFabcdef";
  private static final String HASH_TOO_LONG = // 65 characters
    "012345678901234567890123456789012345678901234567890123456789012345";
  private static final String REF_NAME = "refName";
  private static final String BRANCH_NAME = "branchName";
  private static final String TAG_NAME = "tagName";

  @Test
  public void notSpecified() {
    VersionContext versionContext = VersionContext.NOT_SPECIFIED;

    assertFalse(versionContext.isSpecified());
    assertFalse(versionContext.isRef());
    assertFalse(versionContext.isBranch());
    assertFalse(versionContext.isTag());
    assertFalse(versionContext.isBareCommit());

    assertEquals(VersionContext.Type.UNSPECIFIED, versionContext.getType());
    assertNull(versionContext.getRefName());
    assertNull(versionContext.getCommitHash());
  }

  @Test
  public void ref() {
    VersionContext versionContext = VersionContext.ofRef(REF_NAME);

    assertTrue(versionContext.isSpecified());
    assertTrue(versionContext.isRef());

    assertFalse(versionContext.isBranch());
    assertFalse(versionContext.isTag());
    assertFalse(versionContext.isBareCommit());

    assertEquals(VersionContext.Type.REF, versionContext.getType());
    assertEquals(REF_NAME, versionContext.getRefName());
  }

  @Test
  public void tag() {
    VersionContext versionContext = VersionContext.ofTag(TAG_NAME);

    assertTrue(versionContext.isSpecified());
    assertTrue(versionContext.isTag());

    assertFalse(versionContext.isRef());
    assertFalse(versionContext.isBranch());
    assertFalse(versionContext.isBareCommit());

    assertEquals(VersionContext.Type.TAG, versionContext.getType());
    assertEquals(TAG_NAME, versionContext.getRefName());
    assertNull(versionContext.getCommitHash());
  }

  @Test
  public void branch() {
    VersionContext versionContext = VersionContext.ofBranch(BRANCH_NAME);

    assertTrue(versionContext.isSpecified());
    assertTrue(versionContext.isBranch());

    assertFalse(versionContext.isRef());
    assertFalse(versionContext.isTag());
    assertFalse(versionContext.isBareCommit());

    assertEquals(VersionContext.Type.BRANCH, versionContext.getType());
    assertEquals(BRANCH_NAME, versionContext.getRefName());
    assertNull(versionContext.getCommitHash());
  }

  @Test
  public void bareCommit() {
    VersionContext versionContext = VersionContext.ofBareCommit(REASONABLE_HASH);

    assertTrue(versionContext.isSpecified());
    assertTrue(versionContext.isBareCommit());

    assertFalse(versionContext.isRef());
    assertFalse(versionContext.isBranch());
    assertFalse(versionContext.isTag());

    assertEquals(VersionContext.Type.BARE_COMMIT, versionContext.getType());
    assertNull(versionContext.getRefName());
    assertEquals(REASONABLE_HASH, versionContext.getCommitHash());
  }

  @Test
  public void bareCommitHashNotHexadecimal() {
    Assert.assertThrows(IllegalArgumentException.class,
      () -> VersionContext.ofBareCommit(NOT_HEXADECIMAL));
  }

  @Test
  public void bareCommitHashEmptyString() {
    Assert.assertThrows(IllegalArgumentException.class,
      () -> VersionContext.ofBareCommit(EMPTY_STRING));
  }

  @Test
  public void bareCommitHashTooLong() {
    Assert.assertThrows(IllegalArgumentException.class,
      () -> VersionContext.ofBareCommit(HASH_TOO_LONG));
  }
}
