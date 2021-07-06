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
package com.dremio.service.nessie;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableDelete;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.ImmutableUnchanged;
import org.projectnessie.versioned.Key;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreCreationFunction;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

/**
 * Creates the KV store for Nessie.
 */
public class NessieCommitKVStoreBuilder implements KVStoreCreationFunction<Hash, NessieCommit> {
  static final String TABLE_NAME = "nessieCommit";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static class SerializableCommit {
    @JsonProperty("hash")
    private org.projectnessie.model.Hash hash;

    @JsonProperty("ancestor")
    private org.projectnessie.model.Hash ancestor;

    @JsonProperty("metadata")
    private CommitMeta metadata;

    @JsonProperty("operations")
    private Operations operations;
  }

  @Override
  public KVStore<Hash, NessieCommit> build(StoreBuildingFactory factory) {
    return factory.<Hash, NessieCommit>newStore()
      .name(TABLE_NAME)
      .keyFormat(Format.wrapped(
        Hash.class,
        Hash::asString,
        Hash::of,
        Format.ofString()))
      .valueFormat(Format.wrapped(
        NessieCommit.class,
        NessieCommitKVStoreBuilder::commitToString,
        NessieCommitKVStoreBuilder::stringToCommit,
        Format.ofString()))
      .build();
  }

  @VisibleForTesting
  static String commitToString(NessieCommit commit) {
    final SerializableCommit serializableCommit = new SerializableCommit();
    serializableCommit.hash = org.projectnessie.model.Hash.of(commit.getHash().asString());
    serializableCommit.ancestor = org.projectnessie.model.Hash.of(commit.getAncestor().asString());
    serializableCommit.metadata = commit.getMetadata();
    serializableCommit.operations = toModel(commit.getOperations());

    try {
      return objectMapper.writer().writeValueAsString(serializableCommit);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException();
    }
  }

  @VisibleForTesting
  static Operations toModel(List<org.projectnessie.versioned.Operation<Contents>> operations) {
    final ImmutableOperations.Builder operationsBuilder = ImmutableOperations.builder();
    operations.forEach(op -> operationsBuilder.addOperations(toModel(op)));
    return operationsBuilder.build();
  }

  @VisibleForTesting
  static Operation toModel(org.projectnessie.versioned.Operation<Contents> operation) {
    if (operation instanceof org.projectnessie.versioned.Delete) {
      return Operation.Delete.of(ContentsKey.of(operation.getKey().getElements()));
    } else if (operation instanceof org.projectnessie.versioned.Put) {
      return Operation.Put.of(
          ContentsKey.of(operation.getKey().getElements()),
          (Contents)((org.projectnessie.versioned.Put)operation).getValue());
    } else if (operation instanceof org.projectnessie.versioned.Unchanged) {
      return Operation.Unchanged.of(ContentsKey.of(operation.getKey().getElements()));
    } else {
      throw new IllegalArgumentException();
    }
  }

  @VisibleForTesting
  static NessieCommit stringToCommit(String s) {
    try {
      final SerializableCommit serializableCommit = (SerializableCommit) objectMapper
          .readerFor(SerializableCommit.class)
          .readValue(new StringReader(s));

      return new NessieCommit(
        org.projectnessie.versioned.Hash.of(serializableCommit.hash.getHash()),
        org.projectnessie.versioned.Hash.of(serializableCommit.ancestor.getHash()),
        serializableCommit.metadata,
        NessieCommitKVStoreBuilder.fromModel(serializableCommit.operations));
    } catch (IOException e) {
      throw new IllegalArgumentException();
    }
  }

  private static List<org.projectnessie.versioned.Operation<Contents>> fromModel(Operations operations) {
    return operations.getOperations().stream().map(NessieCommitKVStoreBuilder::fromModel).collect(Collectors.toList());
  }

  private static org.projectnessie.versioned.Operation<Contents> fromModel(Operation operation) {
    final Key key = Key.of(operation.getKey().getElements().toArray(new String[operation.getKey().getElements().size()]));

    if (operation instanceof Operation.Delete) {
      return ImmutableDelete.<Contents>builder().key(key).build();
    } else if (operation instanceof Operation.Put) {
      return ImmutablePut.<Contents>builder().key(key).value(((Operation.Put) operation).getContents()).build();
    } else if (operation instanceof Operation.Unchanged) {
      return ImmutableUnchanged.<Contents>builder().key(key).build();
    } else {
      throw new IllegalArgumentException();
    }
  }
}
