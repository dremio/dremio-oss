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
package com.dremio.service.autocomplete.catalog.mock;

import java.time.Instant;
import java.util.List;

import com.dremio.service.autocomplete.nessie.Branch;
import com.dremio.service.autocomplete.nessie.Commit;
import com.dremio.service.autocomplete.nessie.Hash;
import com.dremio.service.autocomplete.nessie.NessieElement;
import com.dremio.service.autocomplete.nessie.NessieElementReader;
import com.dremio.service.autocomplete.nessie.Tag;
import com.google.common.collect.ImmutableList;

/**
 * In memory implementation of NessieElementReader.
 */
public final class MockNessieElementReader extends NessieElementReader {
  public static final MockNessieElementReader INSTANCE = new MockNessieElementReader();

  private static final ImmutableList<Hash> hashes = new ImmutableList.Builder<Hash>()
    .add(new Hash("DEADBEEF"))
    .add(new Hash("CAFEBABE"))
    .add(new Hash("B0BACAFE"))
    .build();

  private static final ImmutableList<Branch> branches = new ImmutableList.Builder<Branch>()
    .add(new Branch("Branch A", hashes.get(0)))
    .add(new Branch("Branch B", hashes.get(1)))
    .add(new Branch("Branch C", hashes.get(2)))
    .build();

  private static final ImmutableList<Commit> commits = new ImmutableList.Builder<Commit>()
    .add(new Commit(hashes.get(0), "Alice", Instant.ofEpochSecond(0), "Adding first commit"))
    .add(new Commit(hashes.get(1), "Bob", Instant.ofEpochSecond(1), "Fixing bug introduced by Alice."))
    .add(new Commit(hashes.get(2), "Alice", Instant.ofEpochSecond(2), "Bob really doesn't know what he is doing"))
    .build();

  private static final ImmutableList<Tag> tags = new ImmutableList.Builder<Tag>()
    .add(new Tag("Tag A", hashes.get(0)))
    .add(new Tag("Tag B", hashes.get(1)))
    .add(new Tag("Tag C", hashes.get(2)))
    .build();

  private static final ImmutableList<NessieElement> elements = new ImmutableList.Builder<NessieElement>()
    .addAll(branches)
    .addAll(commits)
    .addAll(tags)
    .build();

  private MockNessieElementReader() {
  }

  @Override
  public List<Branch> getBranches() {
    return branches;
  }

  @Override
  public List<Commit> getCommits() {
    return commits;
  }

  @Override
  public List<Tag> getTags() {
    return tags;
  }

  @Override
  public List<NessieElement> getNessieElements() {
    return elements;
  }
}
