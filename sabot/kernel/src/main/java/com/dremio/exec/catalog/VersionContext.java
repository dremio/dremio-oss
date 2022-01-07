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

public final class VersionContext {
  private final String branchOrTagName;
  private final String commitHash;
  private final VersionContextType type;

  private VersionContext(String branchOrTagName, String commitHash, VersionContextType type) {
    this.branchOrTagName = branchOrTagName;
    this.commitHash = commitHash;
    this.type = type;
  }

  public static VersionContext fromBranchName(String branchName) {
    return new VersionContext(branchName, null, VersionContextType.BRANCH);
  }

  public static VersionContext fromTagName(String tagName) {
    return new VersionContext(tagName, null, VersionContextType.TAG);
  }

  public static VersionContext fromCommitHash(String commitHash) {
    return new VersionContext(null, commitHash, VersionContextType.COMMIT_HASH_ONLY);
  }

  @Override
  public String toString() {
    // TODO add support for branch/tag + commit
    String out;
    switch (type) {
      case BRANCH:
        out = String.format("Branch %s", branchOrTagName);
        break;
      case TAG:
        out = String.format("Tag %s", branchOrTagName);
        break;
      case COMMIT_HASH_ONLY:
        out = String.format("Commit %s", commitHash);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + type);
    }
    return out;
  }

  public String getBranchOrTagName() {
    return branchOrTagName;
  }

  public String getCommitHash() {
    return commitHash;
  }

  public VersionContextType getType() {
    return type;
  }

  public enum VersionContextType {
    BRANCH,
    TAG,
    COMMIT_HASH_ONLY
  }
}
