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
package com.dremio.service.autocomplete.nessie;

import java.util.List;
import java.util.Optional;

/**
 * Reader for NessieElements
 */
public abstract class NessieElementReader {
  public abstract List<Branch> getBranches();
  public abstract List<Commit> getCommits();
  public abstract List<Tag> getTags();
  public abstract List<NessieElement> getNessieElements();

  public Optional<Branch> tryGetBranchWithName(String branchName) {
    return this.getBranches()
      .stream()
      .filter(branch -> branch.getName().equals(branchName))
      .findFirst();
  }

  public Optional<Commit> tryGetCommitWithHash(Hash hash) {
    return this.getCommits()
      .stream()
      .filter(commit -> commit.getHash().equals(hash))
      .findFirst();
  }

  public Optional<Tag> tryGetTagWithName(String tagName) {
    return this.getTags()
      .stream()
      .filter(tag -> tag.getName().equals(tagName))
      .findFirst();
  }
}
