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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Locale;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.Unchanged;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Commit class with Contents as commit value and CommitMeta as metadata.
 */
public class NessieCommit extends Commit<Contents, CommitMeta> {

  private static final HashFunction COMMIT_HASH_FUNCTION = Hashing.sha256();

  /**
   * Creates a commit object with a given hash.
   */
  public NessieCommit(Hash hash, Hash ancestor, CommitMeta metadata, List<Operation<Contents>> operations) {
    super(hash, ancestor, metadata, operations);
  }

  /**
   * Creates a commit object.
   * @return a commit object
   */
  public static NessieCommit of(final Serializer<Contents> valueSerializer,
                                final Serializer<CommitMeta> metadataSerializer, Hash ancestor, CommitMeta metadata,
                                List<Operation<Contents>> operations) {
    // Create a hash for the commit
    Hasher hasher = COMMIT_HASH_FUNCTION.newHasher();

    // Previous commit
    hasher.putString("ancestor", UTF_8);
    hash(hasher, ancestor.asBytes());

    // serialize metadata and hash
    hasher.putString("metadata", UTF_8);
    hash(hasher, metadataSerializer.toBytes(metadata));

    // serialize operations and hash
    for (Operation<Contents> operation: operations) {
      if (operation instanceof Put) {
        Put<Contents> put = (Put<Contents>) operation;
        hasher.putString("put", UTF_8);
        hash(hasher, put.getKey());
        hash(hasher, valueSerializer.toBytes(put.getValue()));
      } else if (operation instanceof Delete) {
        Delete<Contents> delete = (Delete<Contents>) operation;
        hasher.putString("delete", UTF_8);
        hash(hasher, delete.getKey());
      } else if (operation instanceof Unchanged) {
        Unchanged<Contents> unchanged = (Unchanged<Contents>) operation;
        hash(hasher, unchanged.getKey());
        unchanged.getKey().getElements().forEach(e -> hasher.putString(e, UTF_8));
      } else {
        throw new IllegalArgumentException("Unknown operation type for operation " + operation);
      }
    }

    final Hash commitHash = Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
    return new NessieCommit(commitHash, ancestor, metadata, operations);
  }

  private static final void hash(Hasher hasher, ByteString bytes) {
    bytes.asReadOnlyByteBufferList().forEach(hasher::putBytes);
  }

  private static final void hash(Hasher hasher, Key key) {
    key.getElements().forEach(e -> hasher.putString(e.toLowerCase(Locale.ROOT), UTF_8));
  }

}
