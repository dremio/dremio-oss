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
package com.dremio.catalog.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * Represents Dremio's idea of the currently requested version for versioned queries
 *
 * <p>Normal types include: - BRANCH, specified by name - TAG, specified by name - BARE_COMMIT,
 * specified by hexadecimal hash
 *
 * <p>Special types include: - NOT_SPECIFIED, which represents a "not set" case, this will either be
 * overridden later or use the repository default - REF, which represents a BRANCH, a TAG, or a
 * BARE_COMMIT, but we don't yet know which - Special case for REF type: For ease of parsing user
 * input, if refName is null or empty, NOT_SPECIFIED will be returned - *_AS_OF_TIMESTAMP, which
 * represent a time-based lookup. These will resolve to the hash of the commit in the history of the
 * branch/tag that was active at the requested timestamp.
 */
@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableVersionContext.class)
@JsonDeserialize(as = ImmutableVersionContext.class)
public abstract class VersionContext {
  public enum Type {
    NOT_SPECIFIED,
    REF,
    BRANCH,
    TAG,
    COMMIT,
    REF_AS_OF_TIMESTAMP,
    BRANCH_AS_OF_TIMESTAMP,
    TAG_AS_OF_TIMESTAMP,
  }

  public abstract Type getType();

  @Nullable
  public abstract String getValue();

  @Nullable
  public abstract Instant getTimestamp();

  public static VersionContext ofRef(String refName) {
    if (Strings.isNullOrEmpty(refName)) {
      return NOT_SPECIFIED;
    }
    return ImmutableVersionContext.builder().type(Type.REF).value(refName).build();
  }

  public static VersionContext ofBranch(String branchName) {
    return ImmutableVersionContext.builder().type(Type.BRANCH).value(branchName).build();
  }

  public static VersionContext ofTag(String tagName) {
    return ImmutableVersionContext.builder().type(Type.TAG).value(tagName).build();
  }

  public static VersionContext ofCommit(String commitHash) {
    return ImmutableVersionContext.builder().type(Type.COMMIT).value(commitHash).build();
  }

  public static VersionContext ofRefAsOfTimestamp(String refName, Instant timestamp) {
    return ImmutableVersionContext.builder()
        .type(Type.REF_AS_OF_TIMESTAMP)
        .value(refName)
        .timestamp(timestamp)
        .build();
  }

  public static VersionContext ofBranchAsOfTimestamp(String branchName, Instant timestamp) {
    return ImmutableVersionContext.builder()
        .type(Type.BRANCH_AS_OF_TIMESTAMP)
        .value(branchName)
        .timestamp(timestamp)
        .build();
  }

  public static VersionContext ofTagAsOfTimestamp(String tagName, Instant timestamp) {
    return ImmutableVersionContext.builder()
        .type(Type.TAG_AS_OF_TIMESTAMP)
        .value(tagName)
        .timestamp(timestamp)
        .build();
  }

  @Value.Check
  protected void check() {
    switch (getType()) {
      case NOT_SPECIFIED:
        Preconditions.checkArgument(getTimestamp() == null);
        Preconditions.checkArgument(getValue() == null);
        break;
      case REF: // Intentional fallthrough
      case TAG: // Intentional fallthrough
      case BRANCH:
        Preconditions.checkNotNull(getValue());
        Preconditions.checkArgument(getTimestamp() == null);
        break;
      case COMMIT:
        validateHash(getValue());
        Preconditions.checkArgument(getTimestamp() == null);
        break;
      case REF_AS_OF_TIMESTAMP: // Intentional fallthrough
      case BRANCH_AS_OF_TIMESTAMP: // Intentional fallthrough
      case TAG_AS_OF_TIMESTAMP:
        Preconditions.checkNotNull(getValue());
        Preconditions.checkNotNull(getTimestamp());
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + getType());
    }
  }

  private static void validateHash(String hash) {
    Preconditions.checkNotNull(hash);
    Preconditions.checkArgument(!hash.isEmpty(), "If commit is non-null, it must not be empty.");
    Preconditions.checkArgument(
        hash.length() <= 64, String.format("commitHash %s is too long.", hash));
    Preconditions.checkArgument(
        Lists.charactersOf(hash).stream().allMatch(c -> Character.digit(c, 16) >= 0),
        String.format("commitHash %s must be hexadecimal.", hash));
  }

  public VersionContext orElse(VersionContext other) {
    Preconditions.checkNotNull(other);
    return (getType() == Type.NOT_SPECIFIED) ? other : this;
  }

  // Minor immutables warnings, it's worth having this clearly named singleton
  @SuppressWarnings({"VisibilityModifier", "StaticInitializerReferencesSubClass"})
  public static VersionContext NOT_SPECIFIED =
      ImmutableVersionContext.builder().type(Type.NOT_SPECIFIED).build();

  public String toStringFirstLetterCapitalized() {
    return StringUtils.capitalize(toString());
  }

  @Override
  public String toString() {
    switch (getType()) {
      case NOT_SPECIFIED:
        return "Unspecified version context";
      case REF:
        return String.format("reference %s", getValue());
      case BRANCH:
        return String.format("branch %s", getValue());
      case TAG:
        return String.format("tag %s", getValue());
      case COMMIT:
        return String.format("commit %s", getValue());
      case REF_AS_OF_TIMESTAMP:
        return String.format("reference %s as of timestamp %s", getValue(), getTimestamp());
      case BRANCH_AS_OF_TIMESTAMP:
        return String.format("branch %s as of timestamp %s", getValue(), getTimestamp());
      case TAG_AS_OF_TIMESTAMP:
        return String.format("tag %s as of timestamp %s", getValue(), getTimestamp());
      default:
        throw new IllegalStateException("Unexpected value: " + getType());
    }
  }

  @JsonIgnore
  public boolean isSpecified() {
    return getType() != Type.NOT_SPECIFIED;
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
    return getType() == Type.COMMIT;
  }
}
