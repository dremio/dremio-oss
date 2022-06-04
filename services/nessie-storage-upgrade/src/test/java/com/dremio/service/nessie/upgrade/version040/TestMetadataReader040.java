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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.upgrade.version040.model.ContentsKey;
import com.dremio.service.nessie.upgrade.version040.model.DeleteOperation;
import com.dremio.service.nessie.upgrade.version040.model.Hash;
import com.dremio.service.nessie.upgrade.version040.model.IcebergTable;
import com.dremio.service.nessie.upgrade.version040.model.Metadata;
import com.dremio.service.nessie.upgrade.version040.model.NessieCommit;
import com.dremio.service.nessie.upgrade.version040.model.Operations;
import com.dremio.service.nessie.upgrade.version040.model.PutOperation;
import com.google.common.collect.Lists;

@ExtendWith(MockitoExtension.class)
public class TestMetadataReader040 {
  @Mock
  private KVStoreProvider kvStoreProvider;
  @Mock
  private KVStore<NessieRefKVStoreBuilder.NamedRef, String> refKVStore;
  @Mock
  private KVStore<String, NessieCommit> commitKVStore;

  private MetadataReader040 metadataReader;

  private static class MigratedCommit {
    private String branchName;
    private List<String> contentsKey;
    private String location;

    public MigratedCommit(String branchName, List<String> contentsKey, String location) {
      this.branchName = branchName;
      this.contentsKey = contentsKey;
      this.location = location;
    }

    @Override
    public int hashCode() {
      return Objects.hash(branchName, contentsKey, location);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MigratedCommit)) {
        return false;
      }

      final MigratedCommit other = (MigratedCommit) o;
      return Objects.equals(branchName, other.branchName) &&
        Objects.equals(contentsKey, other.contentsKey) &&
        Objects.equals(location, other.location);
    }
  }

  @BeforeEach
  public void setup() {
    when(kvStoreProvider.getStore(NessieRefKVStoreBuilder.class)).thenReturn(refKVStore);
    when(kvStoreProvider.getStore(NessieCommitKVStoreBuilder.class)).thenReturn(commitKVStore);
    metadataReader = new MetadataReader040(kvStoreProvider);
  }

  @Test
  public void testTwoPuts() {
    final NessieCommit commit1 = buildNessieCommitForPut(
      "hash1",
      MetadataReader040.INITIAL_HASH,
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"
    );
    final NessieCommit commit2 = buildNessieCommitForPut(
      "hash2",
      "hash1",
      Lists.newArrayList("part1", "part2"),
      "file://target/test-classes/iceberg-file2.json"
    );

    final Document<NessieRefKVStoreBuilder.NamedRef, String> branchDoc = mock(Document.class);
    when(branchDoc.getKey()).thenReturn(new NessieRefKVStoreBuilder.BranchRef("main"));
    when(branchDoc.getValue()).thenReturn("hash2");
    when(refKVStore.find()).thenReturn(Lists.newArrayList(branchDoc));

    final Document<String, NessieCommit> commitDoc2 = mock(Document.class);
    when(commitDoc2.getValue()).thenReturn(commit2);
    when(commitKVStore.get("hash2")).thenReturn(commitDoc2);

    final Document<String, NessieCommit> commitDoc1 = mock(Document.class);
    when(commitDoc1.getValue()).thenReturn(commit1);
    when(commitKVStore.get("hash1")).thenReturn(commitDoc1);

    final List<MigratedCommit> migratedCommits = new ArrayList<>();
    metadataReader.doUpgrade((branchName, contentsKey, location) ->
      migratedCommits.add(new MigratedCommit(branchName, contentsKey, location)));

    assertEquals(2, migratedCommits.size());
    assertEquals(new MigratedCommit(
      "main",
      Lists.newArrayList("part1", "part2"),
      "file://target/test-classes/iceberg-file2.json"), migratedCommits.get(0));
    assertEquals(new MigratedCommit(
      "main",
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"), migratedCommits.get(1));
  }

  @Test
  public void testDeleteAfterPut() {
    final NessieCommit commit1 = buildNessieCommitForPut(
      "hash1",
      MetadataReader040.INITIAL_HASH,
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"
    );
    final NessieCommit commit2 = buildNessieCommitForDelete(
      "hash2",
      "hash1",
      Lists.newArrayList("foo", "bar")
    );

    final Document<NessieRefKVStoreBuilder.NamedRef, String> branchDoc = mock(Document.class);
    when(branchDoc.getKey()).thenReturn(new NessieRefKVStoreBuilder.BranchRef("main"));
    when(branchDoc.getValue()).thenReturn("hash2");
    when(refKVStore.find()).thenReturn(Lists.newArrayList(branchDoc));

    final Document<String, NessieCommit> commitDoc2 = mock(Document.class);
    when(commitDoc2.getValue()).thenReturn(commit2);
    when(commitKVStore.get("hash2")).thenReturn(commitDoc2);

    final Document<String, NessieCommit> commitDoc1 = mock(Document.class);
    when(commitDoc1.getValue()).thenReturn(commit1);
    when(commitKVStore.get("hash1")).thenReturn(commitDoc1);

    final List<MigratedCommit> migratedCommits = new ArrayList<>();
    metadataReader.doUpgrade((branchName, contentsKey, location) ->
      migratedCommits.add(new MigratedCommit(branchName, contentsKey, location)));

    assertTrue(migratedCommits.isEmpty());
  }

  @Test
  public void testPutAfterDelete() {
    final NessieCommit commit1 = buildNessieCommitForDelete(
      "hash1",
      MetadataReader040.INITIAL_HASH,
      Lists.newArrayList("foo", "bar")
    );
    final NessieCommit commit2 = buildNessieCommitForPut(
      "hash2",
      "hash1",
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"
    );

    final Document<NessieRefKVStoreBuilder.NamedRef, String> branchDoc = mock(Document.class);
    when(branchDoc.getKey()).thenReturn(new NessieRefKVStoreBuilder.BranchRef("main"));
    when(branchDoc.getValue()).thenReturn("hash2");
    when(refKVStore.find()).thenReturn(Lists.newArrayList(branchDoc));

    final Document<String, NessieCommit> commitDoc2 = mock(Document.class);
    when(commitDoc2.getValue()).thenReturn(commit2);
    when(commitKVStore.get("hash2")).thenReturn(commitDoc2);

    final Document<String, NessieCommit> commitDoc1 = mock(Document.class);
    when(commitDoc1.getValue()).thenReturn(commit1);
    when(commitKVStore.get("hash1")).thenReturn(commitDoc1);

    final List<MigratedCommit> migratedCommits = new ArrayList<>();
    metadataReader.doUpgrade((branchName, contentsKey, location) ->
      migratedCommits.add(new MigratedCommit(branchName, contentsKey, location)));

    assertEquals(1, migratedCommits.size());
    assertEquals(new MigratedCommit(
      "main",
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"), migratedCommits.get(0));
  }

  @Test
  public void testTwoPutsSame() {
    final NessieCommit commit1 = buildNessieCommitForPut(
      "hash1",
      MetadataReader040.INITIAL_HASH,
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"
    );
    final NessieCommit commit2 = buildNessieCommitForPut(
      "hash2",
      "hash1",
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"
    );

    final Document<NessieRefKVStoreBuilder.NamedRef, String> branchDoc = mock(Document.class);
    when(branchDoc.getKey()).thenReturn(new NessieRefKVStoreBuilder.BranchRef("main"));
    when(branchDoc.getValue()).thenReturn("hash2");
    when(refKVStore.find()).thenReturn(Lists.newArrayList(branchDoc));

    final Document<String, NessieCommit> commitDoc2 = mock(Document.class);
    when(commitDoc2.getValue()).thenReturn(commit2);
    when(commitKVStore.get("hash2")).thenReturn(commitDoc2);

    final Document<String, NessieCommit> commitDoc1 = mock(Document.class);
    when(commitDoc1.getValue()).thenReturn(commit1);
    when(commitKVStore.get("hash1")).thenReturn(commitDoc1);

    final List<MigratedCommit> migratedCommits = new ArrayList<>();
    metadataReader.doUpgrade((branchName, contentsKey, location) ->
      migratedCommits.add(new MigratedCommit(branchName, contentsKey, location)));

    assertEquals(1, migratedCommits.size());
    assertEquals(new MigratedCommit(
      "main",
      Lists.newArrayList("foo", "bar"),
      "file://target/test-classes/iceberg-file1.json"), migratedCommits.get(0));
  }

  private NessieCommit buildEmptyNessieCommit(String hash,
                                              String ancestor) {
    final NessieCommit commit = new NessieCommit();
    commit.setHash(new Hash());
    commit.getHash().setHash(hash);
    commit.getHash().setName(hash);

    commit.setAncestor(new Hash());
    commit.getAncestor().setHash(ancestor);
    commit.getAncestor().setName(ancestor);

    commit.setMetadata(new Metadata());
    commit.getMetadata().setHash("metadata-hash");
    commit.getMetadata().setCommiter("My Name");
    commit.getMetadata().setEmail("me@there.com");
    commit.getMetadata().setMessage("A message");
    commit.getMetadata().setCommitTime(System.currentTimeMillis() - 5000L);

    return commit;
  }

  private NessieCommit buildNessieCommitForPut(String hash,
                                               String ancestor,
                                               List<String> contentsKey,
                                               String location) {
    final NessieCommit commit = buildEmptyNessieCommit(hash, ancestor);

    commit.setOperations(new Operations());
    commit.getOperations().setOperations(new ArrayList<>());
    commit.getOperations().getOperations().add(new PutOperation());
    commit.getOperations().getOperations().get(0).setKey(new ContentsKey());
    commit.getOperations().getOperations().get(0).getKey().setElements(contentsKey);
    ((PutOperation) commit.getOperations().getOperations().get(0)).setContents(new IcebergTable());
    ((PutOperation) commit.getOperations().getOperations().get(0)).getContents().setMetadataLocation(location);

    return commit;
  }

  private NessieCommit buildNessieCommitForDelete(String hash,
                                                  String ancestor,
                                                  List<String> contentsKey) {
    final NessieCommit commit = buildEmptyNessieCommit(hash, ancestor);

    commit.setOperations(new Operations());
    commit.getOperations().setOperations(new ArrayList<>());
    commit.getOperations().getOperations().add(new DeleteOperation());
    commit.getOperations().getOperations().get(0).setKey(new ContentsKey());
    commit.getOperations().getOperations().get(0).getKey().setElements(contentsKey);

    return commit;
  }
}
