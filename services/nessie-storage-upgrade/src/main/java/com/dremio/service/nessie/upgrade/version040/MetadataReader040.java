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
package com.dremio.service.nessie.upgrade.version040;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.upgrade.version040.model.ContentsKey;
import com.dremio.service.nessie.upgrade.version040.model.DeleteOperation;
import com.dremio.service.nessie.upgrade.version040.model.NessieCommit;
import com.dremio.service.nessie.upgrade.version040.model.PutOperation;
import com.dremio.service.nessie.upgrade.version040.model.UnchangedOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;

public class MetadataReader040 implements MetadataReader {
  public static final String INITIAL_HASH;

  static {
    final HashFunction hashFunction = Hashing.sha256();
    INITIAL_HASH =
        Hex.encodeHexString(
            hashFunction.newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes());
  }

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MetadataReader040.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final KVStore<NessieRefKVStoreBuilder.NamedRef, String> refKVStore;
  private final KVStore<String, NessieCommit> commitKVStore;

  public MetadataReader040(KVStoreProvider kvStoreProvider) {
    this.refKVStore = kvStoreProvider.getStore(NessieRefKVStoreBuilder.class);
    this.commitKVStore = kvStoreProvider.getStore(NessieCommitKVStoreBuilder.class);
  }

  public KVStore<NessieRefKVStoreBuilder.NamedRef, String> getRefKVStore() {
    return refKVStore;
  }

  public KVStore<String, NessieCommit> getCommitKVStore() {
    return commitKVStore;
  }

  @Override
  public void doUpgrade(CommitConsumer commitConsumer) {
    final Iterable<Document<NessieRefKVStoreBuilder.NamedRef, String>> refIterable =
        refKVStore.find();
    if (refIterable == null) {
      return;
    }

    refIterable.forEach(
        doc -> {
          final NessieRefKVStoreBuilder.NamedRef namedRef = doc.getKey();
          if (namedRef instanceof NessieRefKVStoreBuilder.BranchRef) {
            final String branchName = namedRef.getName();
            final Set<List<String>> migratedKeys = new HashSet<>();

            String currentHash = doc.getValue();
            while (!INITIAL_HASH.equals(currentHash)) {
              final Document<String, NessieCommit> commitDoc = commitKVStore.get(currentHash);
              if (commitDoc == null) {
                break;
              }

              final NessieCommit commit = commitDoc.getValue();
              if (isPutOperation(commit)) {
                final ContentsKey contentsKey =
                    commit.getOperations().getOperations().get(0).getKey();
                final String location =
                    ((PutOperation) commit.getOperations().getOperations().get(0))
                        .getContents()
                        .getMetadataLocation();

                if (contentsKey != null && location != null) {
                  // Never replace a value, since the first one encountered is the latest
                  if (!migratedKeys.contains(contentsKey.getElements())) {
                    commitConsumer.migrateCommit(branchName, contentsKey.getElements(), location);
                    migratedKeys.add(contentsKey.getElements());
                  }
                }
              } else if (isDeleteOperation(commit)) {
                final ContentsKey contentsKey =
                    commit.getOperations().getOperations().get(0).getKey();
                migratedKeys.add(contentsKey.getElements());
              }

              currentHash = commit.getAncestor().getHash();
            }
          }
        });
  }

  private boolean isPutOperation(NessieCommit commit) {
    return commit != null
        && commit.getOperations() != null
        && commit.getOperations().getOperations() != null
        && !commit.getOperations().getOperations().isEmpty()
        && commit.getOperations().getOperations().get(0) instanceof PutOperation;
  }

  private boolean isDeleteOperation(NessieCommit commit) {
    return commit != null
        && commit.getOperations() != null
        && commit.getOperations().getOperations() != null
        && !commit.getOperations().getOperations().isEmpty()
        && commit.getOperations().getOperations().get(0) instanceof DeleteOperation;
  }

  private boolean isUnchangedOperation(NessieCommit commit) {
    return commit != null
        && commit.getOperations() != null
        && commit.getOperations().getOperations() != null
        && !commit.getOperations().getOperations().isEmpty()
        && commit.getOperations().getOperations().get(0) instanceof UnchangedOperation;
  }

  @VisibleForTesting
  static class DataFromIcebergFile {
    private long snapshotId;
    private int schemaId;
    private int specId;
    private int sortOrderId;

    public long getSnapshotId() {
      return snapshotId;
    }

    public void setSnapshotId(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public int getSchemaId() {
      return schemaId;
    }

    public void setSchemaId(int schemaId) {
      this.schemaId = schemaId;
    }

    public int getSpecId() {
      return specId;
    }

    public void setSpecId(int specId) {
      this.specId = specId;
    }

    public int getSortOrderId() {
      return sortOrderId;
    }

    public void setSortOrderId(int sortOrderId) {
      this.sortOrderId = sortOrderId;
    }
  }
}
