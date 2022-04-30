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

import javax.annotation.Nullable;

import org.apache.parquet.Strings;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents Dremio's idea of the currently requested version for versioned queries
 *
 * Normal types include:
 *  - BRANCH, specified by name
 *  - TAG, specified by name
 *  - BARE_COMMIT, specified by hexadecimal hash
 *
 *  Special types include:
 *  - UNSPECIFIED, which represents a "not set" case, this will either be
 *      overriden later or use the repository default
 *  - REF, which represents either a BRANCH or a TAG, but we don't yet know which
 *     - Special case for REF type: For ease of parsing user input, if refName
 *         is null or empty, NOT_SPECIFIED will be returned
 *     - TODO: We may add support for REF possibly being a commit too in the future
 */
@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableVersionContext.class)
@JsonDeserialize(as = ImmutableVersionContext.class)
public abstract class VersionContext {
  public enum Type {
    UNSPECIFIED,
    REF,
    BRANCH,
    TAG,
    BARE_COMMIT,
  }

  @Value.Default
  public Type getType() {
    return Type.REF;
  }
  @Nullable
  public abstract String getRefName();
  @Nullable
  public abstract String getCommitHash();

  public static VersionContext ofRef(String refName) {
    if (Strings.isNullOrEmpty(refName)) {
      return NOT_SPECIFIED;
    }
    return ImmutableVersionContext.builder()
      .type(Type.REF)
      .refName(refName)
      .build();
  }

  public static VersionContext ofBranch(String branchName) {
    return ImmutableVersionContext.builder()
      .type(Type.BRANCH)
      .refName(branchName)
      .build();
  }

  public static VersionContext ofTag(String tagName) {
    return ImmutableVersionContext.builder()
      .type(Type.TAG)
      .refName(tagName)
      .build();
  }

  public static VersionContext ofBareCommit(String commitHash) {
    return ImmutableVersionContext.builder()
      .type(Type.BARE_COMMIT)
      .commitHash(commitHash)
      .build();
  }

  @Value.Check
  protected void check() {
    switch (getType()) {
    case UNSPECIFIED:
      Preconditions.checkArgument(getRefName() == null);
      Preconditions.checkArgument(getCommitHash() == null);
      break;
    case REF: // Intentional fallthrough
    case TAG:  // Intentional fallthrough
    case BRANCH:
      Preconditions.checkNotNull(getRefName());
      Preconditions.checkArgument(getCommitHash() == null);
      break;
    case BARE_COMMIT:
      Preconditions.checkArgument(getRefName() == null);
      validateHash();
      break;
    default:
      throw new IllegalStateException("Unexpected value: " + getType());
    }
  }

  private void validateHash() {
    Preconditions.checkNotNull(getCommitHash());
    Preconditions.checkArgument(
      !getCommitHash().isEmpty(),
      "If commitHash is non-null, it must not be empty.");
    Preconditions.checkArgument(
      getCommitHash().length() <= 64,
      "commitHash is too long.");
    Preconditions.checkArgument(
      Lists.charactersOf(getCommitHash()).stream().allMatch(c -> Character.digit(c, 16) >= 0),
      "commitHash must be hexadecimal.");
  }

  public VersionContext orElse(VersionContext other) {
    Preconditions.checkNotNull(other);
    return (getType() == Type.UNSPECIFIED) ? other : this;
  }

  public static VersionContext NOT_SPECIFIED = ImmutableVersionContext.builder()
    .type(Type.UNSPECIFIED)
    .build();

  public String prettyString() {
    String out;
    switch (getType()) {
      case UNSPECIFIED:
        out = "Unspecified version context";
        break;
      case REF:
        out = String.format("Ref %s", getRefName());
        break;
      case BRANCH:
        out = String.format("Branch %s", getRefName());
        break;
      case TAG:
        out = String.format("Tag %s", getRefName());
        break;
      case BARE_COMMIT:
        out = String.format("Commit hash %s", getCommitHash());
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + getType());
    }
    return out;
  }

  @JsonIgnore
  public boolean isSpecified() {
    return getType() != Type.UNSPECIFIED;
  }

  @JsonIgnore
  public boolean isRef() {
    return getType() == Type.REF;
  }

  @JsonIgnore
  public boolean isBranch() {
    return getType() == Type.BRANCH;
  }

  @JsonIgnore
  public boolean isTag() {
    return getType() == Type.TAG;
  }

  @JsonIgnore
  public boolean isBareCommit() {
    return getType() == Type.BARE_COMMIT;
  }
}
