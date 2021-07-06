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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.platform.commons.util.UnrecoverableExceptions;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opentest4j.AssertionFailedError;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.tests.CommitBuilder;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.KVStore;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;

/**
 * Tests for the implementation of VersionStore on KVStore backend.
 * The tests below are adapted from the Test class AbstractITVersionStore.java in project Nessie codebase.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestNessieKVVersionStore {

  private static final TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();

  private VersionStore<Contents, CommitMeta> store;
  @Mock private KVStore<Hash, NessieCommit> mockCommitStore;
  @Mock private KVStore<NamedRef, Hash> mockNamedRefStore;

  protected VersionStore<Contents, CommitMeta> store() {
    return store;
  }

  private LocalKVStoreProvider localKVStoreProvider;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private VersionStore<Contents, CommitMeta> buildMockVersionStore() {
    return NessieKVVersionStore.builder()
      .commits(mockCommitStore)
      .namedReferences(mockNamedRefStore)
      .valueSerializer(storeWorker.getValueSerializer())
      .metadataSerializer(storeWorker.getMetadataSerializer())
      .defaultBranchName(NessieConfig.NESSIE_DEFAULT_BRANCH)
      .maxCommitRetries(4)
      .build();
  }

  @Before
  public void before() throws Exception {
    localKVStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT,
      temporaryFolder.getRoot().toString(), true, true);

    localKVStoreProvider.start();

    final KVStore<Hash, NessieCommit> commitStore = localKVStoreProvider.getStore(NessieCommitKVStoreBuilder.class);
    final KVStore<NamedRef, Hash> namedRefStore = localKVStoreProvider.getStore(NessieRefKVStoreBuilder.class);

    this.store = NessieKVVersionStore.builder()
      .commits(commitStore)
      .namedReferences(namedRefStore)
      .valueSerializer(storeWorker.getValueSerializer())
      .metadataSerializer(storeWorker.getMetadataSerializer())
      .defaultBranchName(NessieConfig.NESSIE_DEFAULT_BRANCH)
      .maxCommitRetries(4)
      .build();
  }

  @After
  public void after() throws Exception {
    this.localKVStoreProvider.close();
    this.store = null;
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithoutCommitStore() {
    final KVStore<NamedRef, Hash> namedRefStore = localKVStoreProvider.getStore(NessieRefKVStoreBuilder.class);

    NessieKVVersionStore.builder()
      .namedReferences(namedRefStore)
      .valueSerializer(storeWorker.getValueSerializer())
      .metadataSerializer(storeWorker.getMetadataSerializer())
      .defaultBranchName(NessieConfig.NESSIE_DEFAULT_BRANCH).build();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithoutNamedRefStore() {
    final KVStore<Hash, NessieCommit> commitStore = localKVStoreProvider.getStore(NessieCommitKVStoreBuilder.class);

    NessieKVVersionStore.builder()
      .commits(commitStore)
      .valueSerializer(storeWorker.getValueSerializer())
      .metadataSerializer(storeWorker.getMetadataSerializer())
      .defaultBranchName(NessieConfig.NESSIE_DEFAULT_BRANCH).build();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithoutDefaultBranch() {
    final KVStore<Hash, NessieCommit> commitStore = localKVStoreProvider.getStore(NessieCommitKVStoreBuilder.class);
    final KVStore<NamedRef, Hash> namedRefStore = localKVStoreProvider.getStore(NessieRefKVStoreBuilder.class);

    NessieKVVersionStore.builder()
      .commits(commitStore)
      .namedReferences(namedRefStore)
      .valueSerializer(storeWorker.getValueSerializer())
      .metadataSerializer(storeWorker.getMetadataSerializer())
      .build();
  }
  @Test(expected = ReferenceNotFoundException.class)
  public void verifyDefaultBranchNameFromConfig() throws Exception {
    final BranchName validBranch = BranchName.of(NessieConfig.NESSIE_DEFAULT_BRANCH);
    final Hash validHash = store().toHash(validBranch);
    assertNotNull(validHash);

    final BranchName invalidBranch = BranchName.of(NessieConfig.NESSIE_DEFAULT_BRANCH + "_invalid");
    store().toHash(invalidBranch);
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - check that a hash is returned by toHash
   * - check the branch is returned by getNamedRefs
   * - check that no commits are returned using getCommits
   * - check the branch cannot be created
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());
    final Hash hash = store().toHash(branch);
    assertNotNull(hash);

    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.of(hash));
    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    store().create(anotherAnotherBranch, Optional.of(commitHash));

    assertThat(store().getCommits(branch).count(), is(0L));
    assertThat(store().getCommits(anotherBranch).count(), is(1L));
    assertThat(store().getCommits(anotherAnotherBranch).count(), is(1L));
    assertThat(store().getCommits(hash).count(), is(0L)); // empty commit should not be listed
    assertThat(store().getCommits(commitHash).count(), is(1L)); // empty commit should not be listed

    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.of(hash)));

    store().delete(branch, Optional.of(hash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));

    // TODO - getNamedRefs()
    /*try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      assertThat(str.count(), is(2L)); // bar + baz
    }*/

    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(hash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to it
   * - Check that another commit with no operations can be added with the initial hash
   * - Check the commit can be listed
   * - Check that the commit can be deleted
   */
  @Test
  public void commitToBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());
    final Hash initialHash = store().toHash(branch);

    CommitMeta someCommit = ImmutableCommitMeta.builder().commiter("a").message("Some commit").build();
    store().commit(branch, Optional.of(initialHash), someCommit, Collections.emptyList());
    final Hash commitHash = store().toHash(branch);

    assertThat(commitHash, is(Matchers.not(initialHash)));

    CommitMeta anotherCommit = ImmutableCommitMeta.builder().commiter("a").message("Another commit").build();
    store().commit(branch, Optional.of(initialHash), anotherCommit, Collections.emptyList());
    final Hash anotherCommitHash = store().toHash(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
      WithHash.of(anotherCommitHash, ImmutableCommitMeta.builder().commiter("a").message("Another commit").build()),
      WithHash.of(commitHash, ImmutableCommitMeta.builder().commiter("a").message("Some commit").build())
    ));
    assertThat(store().getCommits(commitHash).collect(Collectors.toList()), contains(WithHash.of(commitHash,
      ImmutableCommitMeta.builder().commiter("a").message("Some commit").build())));

    assertThrows(ReferenceConflictException.class, () -> store().delete(branch, Optional.of(initialHash)));
    store().delete(branch, Optional.of(anotherCommitHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));

    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("foo"));

    // TODO - getNamedRefs()
    /*try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      assertThat(str.count(), is(0L));
    }*/

    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(commitHash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add 3 commits in succession with no conflicts to it with put and delete operations
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitSomeOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
      .put("t1", IcebergTable.of("v1_1"))
      .put("t2", IcebergTable.of("v2_1"))
      .put("t3", IcebergTable.of("v3_1"))
      .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
      .put("t1", IcebergTable.of("v1_2"))
      .delete("t2")
      .delete("t3")
      .put("t4", IcebergTable.of("v4_1"))
      .toBranch(branch);

    final Hash thirdCommit = commit("Third Commit")
      .put("t2", IcebergTable.of("v2_2"))
      .unchanged("t4")
      .toBranch(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
      WithHash.of(thirdCommit, ImmutableCommitMeta.builder().commiter("a").message("Third Commit").build()),
      WithHash.of(secondCommit, ImmutableCommitMeta.builder().commiter("a").message("Second Commit").build()),
      WithHash.of(initialCommit, ImmutableCommitMeta.builder().commiter("a").message("Initial Commit").build())
    ));

    assertThat(store().getKeys(branch).collect(Collectors.toList()), containsInAnyOrder(
      Key.of("t1"),
      Key.of("t2"),
      Key.of("t4")
    ));

    assertThat(store().getKeys(secondCommit).collect(Collectors.toList()), containsInAnyOrder(
      Key.of("t1"),
      Key.of("t4")
    ));

    assertThat(store().getKeys(initialCommit).collect(Collectors.toList()), containsInAnyOrder(
      Key.of("t1"),
      Key.of("t2"),
      Key.of("t3")
    ));

    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
      contains(
        Optional.of(IcebergTable.of("v1_2")),
        Optional.of(IcebergTable.of("v2_2")),
        Optional.empty(),
        Optional.of(IcebergTable.of("v4_1"))
      ));

    assertThat(store().getValues(secondCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
      contains(
        Optional.of(IcebergTable.of("v1_2")),
        Optional.empty(),
        Optional.empty(),
        Optional.of(IcebergTable.of("v4_1"))
      ));

    assertThat(store().getValues(initialCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
      contains(
        Optional.of(IcebergTable.of("v1_1")),
        Optional.of(IcebergTable.of("v2_1")),
        Optional.of(IcebergTable.of("v3_1")),
        Optional.empty()
      ));

    assertThat(store().getValue(branch, Key.of("t1")), is(IcebergTable.of("v1_2")));
    assertThat(store().getValue(branch, Key.of("t2")), is(IcebergTable.of("v2_2")));
    assertThat(store().getValue(branch, Key.of("t3")), is(nullValue()));
    assertThat(store().getValue(branch, Key.of("t4")), is(IcebergTable.of("v4_1")));

    assertThat(store().getValue(secondCommit, Key.of("t1")), is(IcebergTable.of("v1_2")));
    assertThat(store().getValue(secondCommit, Key.of("t2")), is(nullValue()));
    assertThat(store().getValue(secondCommit, Key.of("t3")), is(nullValue()));
    assertThat(store().getValue(secondCommit, Key.of("t4")), is(IcebergTable.of("v4_1")));

    assertThat(store().getValue(initialCommit, Key.of("t1")), is(IcebergTable.of("v1_1")));
    assertThat(store().getValue(initialCommit, Key.of("t2")), is(IcebergTable.of("v2_1")));
    assertThat(store().getValue(initialCommit, Key.of("t3")), is(IcebergTable.of("v3_1")));
    assertThat(store().getValue(initialCommit, Key.of("t4")), is(nullValue()));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit for 3 keys
   * - Add a commit based on initial commit for first key
   * - Add a commit based on initial commit for second key
   * - Add a commit based on initial commit for third  key
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitNonConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
      .put("t1", IcebergTable.of("v1_1"))
      .put("t2", IcebergTable.of("v2_1"))
      .put("t3", IcebergTable.of("v3_1"))
      .toBranch(branch);

    final Hash t1Commit = commit("T1 Commit").fromReference(initialCommit).put("t1", IcebergTable.of("v1_2")).toBranch(branch);
    final Hash t2Commit = commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    final Hash t3Commit = commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    final Hash extraCommit = commit("Extra Commit").fromReference(t1Commit).put("t1", IcebergTable.of("v1_3")).put("t3", IcebergTable.of("v3_2")).toBranch(branch);
    final Hash newT2Commit = commit("New T2 Commit").fromReference(t2Commit).put("t2", IcebergTable.of("new_v2_1")).toBranch(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
      WithHash.of(newT2Commit, ImmutableCommitMeta.builder().commiter("a").message("New T2 Commit").build()),
      WithHash.of(extraCommit, ImmutableCommitMeta.builder().commiter("a").message("Extra Commit").build()),
      WithHash.of(t3Commit, ImmutableCommitMeta.builder().commiter("a").message("T3 Commit").build()),
      WithHash.of(t2Commit, ImmutableCommitMeta.builder().commiter("a").message("T2 Commit").build()),
      WithHash.of(t1Commit, ImmutableCommitMeta.builder().commiter("a").message("T1 Commit").build()),
      WithHash.of(initialCommit, ImmutableCommitMeta.builder().commiter("a").message("Initial Commit").build())
    ));

    assertThat(store().getKeys(branch).collect(Collectors.toList()), containsInAnyOrder(
      Key.of("t1"),
      Key.of("t2"),
      Key.of("t3")
    ));

    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_3")),
        Optional.of(IcebergTable.of("new_v2_1")),
        Optional.of(IcebergTable.of("v3_2"))
      ));

    assertThat(store().getValues(newT2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_3")),
        Optional.of(IcebergTable.of("new_v2_1")),
        Optional.of(IcebergTable.of("v3_2"))
      ));

    assertThat(store().getValues(extraCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_3")),
        Optional.empty(),
        Optional.of(IcebergTable.of("v3_2"))
      ));

    assertThat(store().getValues(t3Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_2")),
        Optional.empty(),
        Optional.of(IcebergTable.of("v3_1"))
      ));

    assertThat(store().getValues(t2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_2")),
        Optional.empty(),
        Optional.of(IcebergTable.of("v3_1"))
      ));

    assertThat(store().getValues(t1Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_2")),
        Optional.of(IcebergTable.of("v2_1")),
        Optional.of(IcebergTable.of("v3_1"))
      ));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void commitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
      .put("t1", IcebergTable.of("v1_1"))
      .put("t2", IcebergTable.of("v2_1"))
      .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
      .put("t1", IcebergTable.of("v1_2"))
      .delete("t2")
      .put("t3", IcebergTable.of("v3_1"))
      .toBranch(branch);

    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).put("t1", IcebergTable.of("v1_3")).toBranch(branch));
    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).put("t2", IcebergTable.of("v2_2")).toBranch(branch));
    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).put("t3", IcebergTable.of("v3_2")).toBranch(branch));

    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t1").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t2").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
      () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t3").toBranch(branch));

    // Checking the state hasn't changed
    assertThat(store().toHash(branch), is(secondCommit));
  }

  @Test(expected = ConcurrentModificationException.class)
  public void commitConcurrentConflictingOperationsAllRetriesFail() throws Exception {
    this.store = buildMockVersionStore();
    final BranchName branch = BranchName.of("foo");

    Document<NamedRef, Hash> branchEntry = new ImmutableDocument.Builder<NamedRef, Hash>()
      .setKey(branch).setValue(Hash.of("123123")).setTag("tag").build();
    CommitMeta newCommit = ImmutableCommitMeta.builder().commiter("a").message("New commit").build();

    // When creating the branch:
    // - Returns null first, to ensure the create() call does not throw ReferenceAlreadyExistsException
    // - Returns a valid KVStore entry after that
    when(mockNamedRefStore.get(branch))
      .thenReturn(null)
      .thenReturn(branchEntry);

    store().create(branch, Optional.empty());

    // Throws a ConcurrentModificationException every time someone tries to put() something in the KVStore
    when(mockNamedRefStore.put(any(), any(), any()))
      .thenThrow(new ConcurrentModificationException());

    // Should fail all retries then throw a ConcurrentModificationException
    store().commit(branch, Optional.empty(), newCommit, Collections.emptyList());
  }

  @Test
  public void commitConcurrentConflictingOperationsFailThenSucceed() throws Exception {
    this.store = buildMockVersionStore();
    final BranchName branch = BranchName.of("foo");

    Document<NamedRef, Hash> branchEntry = new ImmutableDocument.Builder<NamedRef, Hash>()
      .setKey(branch).setValue(Hash.of("123123")).setTag("tag").build();
    CommitMeta newCommit = ImmutableCommitMeta.builder().commiter("a").message("New commit").build();

    // When creating the branch:
    // - Returns null first, to ensure the create() call does not throw ReferenceAlreadyExistsException
    // - Returns a valid KVStore entry after that
    when(mockNamedRefStore.get(branch))
      .thenReturn(null)
      .thenReturn(branchEntry);

    store().create(branch, Optional.empty());

    // When committing:
    // - Throws a ConcurrentModificationException the first two times someone tries to put() something in the KVStore
    // - Does not throw an exception after that
    doThrow(new ConcurrentModificationException())
      .doThrow(new ConcurrentModificationException())
      .doReturn(null).when(mockNamedRefStore).put(any(), any(), any());

    // The first two attempts to commit will fail, then the third will succeed
    store().commit(branch, Optional.empty(), newCommit, Collections.emptyList());
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - force commit put operations
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void forceCommitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    commit("Initial Commit")
      .put("t1", IcebergTable.of("v1_1"))
      .put("t2", IcebergTable.of("v2_1"))
      .toBranch(branch);

    commit("Second Commit")
      .put("t1", IcebergTable.of("v1_2"))
      .delete("t2")
      .put("t3", IcebergTable.of("v3_1"))
      .toBranch(branch);

    final Hash putCommit = forceCommit("Conflicting Commit")
      .put("t1", IcebergTable.of("v1_3"))
      .put("t2", IcebergTable.of("v2_2"))
      .put("t3", IcebergTable.of("v3_2"))
      .toBranch(branch);

    assertThat(store().toHash(branch), is(putCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_3")),
        Optional.of(IcebergTable.of("v2_2")),
        Optional.of(IcebergTable.of("v3_2"))
      ));

    final Hash unchangedCommit = commit("Conflicting Commit")
      .unchanged("t1")
      .unchanged("t2")
      .unchanged("t3")
      .toBranch(branch);
    assertThat(store().toHash(branch), is(unchangedCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.of(IcebergTable.of("v1_3")),
        Optional.of(IcebergTable.of("v2_2")),
        Optional.of(IcebergTable.of("v3_2"))
      ));

    final Hash deleteCommit = commit("Conflicting Commit")
      .delete("t1")
      .delete("t2")
      .delete("t3")
      .toBranch(branch);
    assertThat(store().toHash(branch), is(deleteCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
      contains(
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
      ));
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    store().create(branch, Optional.empty());
    CommitMeta metadata = ImmutableCommitMeta.builder().commiter("a").message("metadata").build();
    store().commit(branch, Optional.empty(), metadata, ImmutableList.of(
      ImmutablePut.<Contents>builder().key(Key.of("keyA")).value(IcebergTable.of("foo")).build(),
      ImmutablePut.<Contents>builder().key(Key.of("keyB")).value(IcebergTable.of("foo")).build()
    ));

    assertThat(store().getValue(branch, Key.of("keyA")), is(IcebergTable.of("foo")));
    assertThat(store().getValue(branch, Key.of("keyB")), is(IcebergTable.of("foo")));
  }

  /*
   * Test:
   * - Check that store throws RNFE if branch doesn't exist
   */
  @Test
  public void commitWithInvalidBranch() {
    final BranchName branch = BranchName.of("unknown");

    CommitMeta newCommit = ImmutableCommitMeta.builder().commiter("a").message("New commit").build();
    assertThrows(ReferenceNotFoundException.class,
      () -> store().commit(branch, Optional.empty(), newCommit, Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    CommitMeta newCommit = ImmutableCommitMeta.builder().commiter("a").message("New commit").build();
    assertThrows(ReferenceNotFoundException.class,
      () -> store().commit(branch, Optional.of(Hash.of("1234567890abcdef")), newCommit, Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws IllegalArgumentException if reference hash is not in branch ancestry
   */
  @Test
  public void commitWithInvalidReference() throws ReferenceNotFoundException, ReferenceConflictException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());


    final Hash initialHash = store().toHash(branch);
    CommitMeta someCommit = ImmutableCommitMeta.builder().commiter("a").message("Some commit").build();
    store().commit(branch, Optional.of(initialHash), someCommit, Collections.emptyList());

    final Hash commitHash = store().toHash(branch);

    final BranchName branch2 = BranchName.of("bar");
    store().create(branch2, Optional.empty());

    CommitMeta anotherCommit = ImmutableCommitMeta.builder().commiter("a").message("Another commit").build();
    assertThrows(ReferenceNotFoundException.class,
      () -> store().commit(branch2, Optional.of(commitHash), anotherCommit, Collections.emptyList()));
  }

  @Test
  public void getValueForEmptyBranch() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty());
    final Hash hash = store().toHash(branch);

    assertThat(store().getValue(hash, Key.of("arbitrary")), is(nullValue()));
  }

  @Test
  public void toRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    store().toHash(branch);

    final Hash firstCommit = commit("First Commit").toBranch(branch);
    final Hash secondCommit = commit("Second Commit").toBranch(branch);
    final Hash thirdCommit = commit("Third Commit").toBranch(branch);

    store().create(BranchName.of(thirdCommit.asString()), Optional.of(firstCommit));
    store().create(TagName.of(secondCommit.asString()), Optional.of(firstCommit));

    assertThat(store().toRef(firstCommit.asString()), is(WithHash.of(firstCommit, firstCommit)));
    assertThat(store().toRef(secondCommit.asString()), is(WithHash.of(firstCommit, TagName.of(secondCommit.asString()))));
    assertThat(store().toRef(thirdCommit.asString()), is(WithHash.of(firstCommit, BranchName.of(thirdCommit.asString()))));
    // Is it correct to allow a reference with the sentinel reference?
    //assertThat(store().toRef(initialCommit.asString()), is(WithHash.of(initialCommit, initialCommit)));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("unknown-ref"));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("1234567890abcdef"));
  }

  protected CommitBuilder<Contents, CommitMeta> forceCommit(String message) {
    CommitMeta commitMeta = ImmutableCommitMeta.builder().commiter("a").message(message).build();
    return new CommitBuilder<>(store()).withMetadata(commitMeta);
  }

  protected CommitBuilder<Contents, CommitMeta> commit(String message) {
    CommitMeta commitMeta = ImmutableCommitMeta.builder().commiter("a").message(message).build();
    return new CommitBuilder<>(store()).withMetadata(commitMeta).fromLatest();
  }

  protected Put<String> put(String key, String value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete<String> delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged<String> unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }

  @FunctionalInterface
  private interface Executable {
    void execute() throws Throwable;
  }

  private static <T extends Throwable> T assertThrows(Class<T> expectedType, Executable executable) {
    try {
      executable.execute();
    } catch (Throwable th) {
      if (expectedType.isInstance(th)) {
        return (T)th;
      }

      UnrecoverableExceptions.rethrowIfUnrecoverable(th);
      throw new AssertionFailedError("Unexpected exception type thrown", th);
    }

    throw new AssertionFailedError(String.format("Expected %s to be thrown, but nothing was thrown.", expectedType.getName()));
  }
}
