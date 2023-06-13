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
package com.dremio.dac.service.autocomplete;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;

import com.dremio.service.autocomplete.nessie.Branch;
import com.dremio.service.autocomplete.nessie.Commit;
import com.dremio.service.autocomplete.nessie.Hash;
import com.dremio.service.autocomplete.nessie.NessieElement;
import com.dremio.service.autocomplete.nessie.NessieElementReader;
import com.dremio.service.autocomplete.nessie.Tag;
import com.google.common.base.Preconditions;

/**
 * Implementation of NessieElementReader
 */
public class NessieElementReaderImpl extends NessieElementReader {
  private final NessieApiV1 nessieApi;

  public NessieElementReaderImpl(NessieApiV1 nessieApi) {
    Preconditions.checkNotNull(nessieApi);
    this.nessieApi = nessieApi;
  }

  private static Branch toBranch(Reference reference) {
    if (reference.getType() != ReferenceType.BRANCH) {
      throw new IllegalStateException("Nessie did not respond with branch: " + reference);
    }
    return new Branch(reference.getName(), new Hash(reference.getHash()));
  }

  private static Tag toTag(Reference reference) {
    if (reference.getType() != ReferenceType.TAG) {
      throw new IllegalStateException("Nessie did not respond with tag: " + reference);
    }
    return new Tag(reference.getName(), new Hash(reference.getHash()));
  }

  @Override
  public List<Branch> getBranches() {
    return nessieApi
      .getAllReferences()
      .filter("refType == 'BRANCH'")
      .get()
      .getReferences()
      .stream()
      .map(NessieElementReaderImpl::toBranch)
      .collect(Collectors.toList());
  }

  @Override
  public List<Commit> getCommits() {
    try {
      return nessieApi
        .getCommitLog()
        .get()
        .getLogEntries()
        .stream()
        .map(logEntry -> new Commit(
          new Hash(logEntry.getCommitMeta().getHash()),
          logEntry.getCommitMeta().getAuthor(),
          logEntry.getCommitMeta().getAuthorTime(),
          logEntry.getCommitMeta().getMessage()))
        .collect(Collectors.toList());
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Tag> getTags() {
    return nessieApi
      .getAllReferences()
      .filter("refType == 'TAG'")
      .get()
      .getReferences()
      .stream()
      .map(NessieElementReaderImpl::toTag)
      .collect(Collectors.toList());
  }

  @Override
  public List<NessieElement> getNessieElements() {
    List<NessieElement> nessieElements = new ArrayList<>();
    nessieElements.addAll(this.getBranches());
    nessieElements.addAll(this.getCommits());
    nessieElements.addAll(this.getTags());

    return nessieElements;
  }
}
