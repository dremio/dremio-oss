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
package com.dremio.plugins;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.nessie.combined.CombinedNessieClientFactory;

@ExtendWith(CombinedNessieClientFactory.class)
@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestRetriableCommitWithNamespaces {

  private NessieApiV2 nessieApi;

  @BeforeEach
  void testSetup(NessieClientFactory combinedNessieClientFactory) {
    nessieApi = (NessieApiV2) combinedNessieClientFactory.make();
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(nessieApi);
  }

  private static Operation.Put buildPutNamespaceOp(String... elements) {
    Namespace namespace = Namespace.of(elements);
    return Operation.Put.of(namespace.toContentKey(), namespace);
  }

  private RetriableCommitWithNamespaces buildRetriableCommit(
      Branch branch, Operation.Put operation) {
    return new RetriableCommitWithNamespaces(
        nessieApi, branch, CommitMeta.fromMessage(operation.toString()), operation);
  }

  @Test
  public void testCreateAllParentFolders() throws Exception {
    final Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    Operation.Put leafOperation = buildPutNamespaceOp("A", "B", "C", "D");
    RetriableCommitWithNamespaces commitOperation =
        buildRetriableCommit(branchBeforeCommit, leafOperation);

    assertThat(commitOperation.getParentNamespacesToCreate()).isEmpty();
    assertThat(commitOperation.tryCommit()).isEmpty();
    assertThat(commitOperation.getLastConflictException())
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining(
            "namespace 'A.B.C' must exist, namespace 'A.B' must exist, namespace 'A' must exist.");
    assertThat(commitOperation.getParentNamespacesToCreate()).hasSize(3);

    // next try succeeds by creating all the required parents
    assertThat(commitOperation.commit()).isNotNull();
  }

  @Test
  public void testCreatesOnlyMissingParentFolders() throws Exception {
    // Arrange
    final Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    Operation.Put rootOperation = buildPutNamespaceOp("A");
    Operation.Put leafOperation = buildPutNamespaceOp("A", "B", "C");
    RetriableCommitWithNamespaces commitOperation =
        buildRetriableCommit(branchBeforeCommit, leafOperation);

    // pre-create root folder
    nessieApi
        .commitMultipleOperations()
        .branch(branchBeforeCommit)
        .commitMeta(CommitMeta.fromMessage("CREATE FOLDER " + rootOperation.getKey()))
        .operation(rootOperation)
        .commit();

    // Act + Assert
    assertThat(commitOperation.tryCommit()).isEmpty();
    assertThat(commitOperation.getParentNamespacesToCreate())
        .containsExactly(ContentKey.of("A", "B"));

    // final try commit will create folder 'A.B.C' and its immediate parent
    assertThat(commitOperation.tryCommit()).isPresent();

    assertThat(
            nessieApi
                .getContent()
                .refName(branchBeforeCommit.getName())
                .getSingle(leafOperation.getKey()))
        .isNotNull()
        .extracting(ContentResponse::getContent)
        .extracting(Content::getId)
        .isNotNull();
  }

  @Test
  public void testAppearingParentFolders() throws Exception {
    final Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    Operation.Put rootOperation = buildPutNamespaceOp("A");
    Operation.Put leafOperation = buildPutNamespaceOp("A", "B", "C");
    RetriableCommitWithNamespaces commitOperation =
        buildRetriableCommit(branchBeforeCommit, leafOperation);

    assertThat(commitOperation.tryCommit()).isEmpty();
    assertThat(commitOperation.getParentNamespacesToCreate())
        .containsExactly(ContentKey.of("A"), ContentKey.of("A", "B"));

    // let root folder appear concurrently
    nessieApi
        .commitMultipleOperations()
        .branch(branchBeforeCommit)
        .commitMeta(CommitMeta.fromMessage("CREATE FOLDER " + rootOperation.getKey()))
        .operation(rootOperation)
        .commit();

    // next try will fail again and adjust the namespace to create
    assertThat(commitOperation.tryCommit()).isEmpty();
    assertThat(commitOperation.getParentNamespacesToCreate())
        .containsExactly(ContentKey.of("A", "B"));

    // final try will create folder 'A.B.C' and its immediate parent
    assertThat(commitOperation.tryCommit()).isPresent();

    assertThat(
            nessieApi
                .getContent()
                .refName(branchBeforeCommit.getName())
                .getSingle(leafOperation.getKey()))
        .isNotNull()
        .extracting(ContentResponse::getContent)
        .extracting(Content::getId)
        .isNotNull();
  }

  private void assertOnlyFirstCommitSucceeds(Operation.Put operation, String joinedKey)
      throws Exception {
    Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    Branch branchAfterCommit = buildRetriableCommit(branchBeforeCommit, operation).commit();
    assertThatThrownBy(() -> buildRetriableCommit(branchBeforeCommit, operation).commit())
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessageContaining("Key '" + joinedKey + "' already exists");
    assertThatThrownBy(() -> buildRetriableCommit(branchAfterCommit, operation).commit())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining(
            "New value to update existing key '" + joinedKey + "' has no content ID");
  }

  @Test
  public void testUnresolvableConflictCreatingNamespace() throws Exception {
    Operation.Put createNamespaceOperation = buildPutNamespaceOp("A", "B", "C");

    assertOnlyFirstCommitSucceeds(createNamespaceOperation, "A.B.C");
  }

  @Test
  public void testUnresolvableConflictCreatingTable() throws Exception {
    Operation.Put createTableOperation =
        Operation.Put.of(
            ContentKey.of("X", "Y", "Z"), IcebergTable.of("test.me.txt", 42L, 42, 42, 42));

    assertOnlyFirstCommitSucceeds(createTableOperation, "X.Y.Z");
  }
}
