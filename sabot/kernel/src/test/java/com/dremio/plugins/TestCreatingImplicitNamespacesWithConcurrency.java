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
import static org.junit.Assert.assertEquals;

import com.dremio.common.AutoCloseables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.nessie.combined.CombinedNessieClientFactory;

@ExtendWith(CombinedNessieClientFactory.class)
@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestCreatingImplicitNamespacesWithConcurrency {

  public static final String DEFAULT_BRANCH_NAME = "main";
  private NessieApiV2 nessieApi;

  @BeforeEach
  void testSetup(NessieClientFactory combinedNessieClientFactory) {
    nessieApi = (NessieApiV2) combinedNessieClientFactory.make();
  }

  @Test
  public void createImplicitFoldersWithConcurrencyForCreateNamespace() throws Exception {
    // Arrange
    final List<String> nestedFolderPath = Arrays.asList("A", "B", "C");
    final Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    final ContentKey firstFolderPathContentKey = ContentKey.of(nestedFolderPath.subList(0, 1));
    final ContentKey secondFolderPathContentKey = ContentKey.of(nestedFolderPath.subList(0, 2));
    final ContentKey entireFolderPathContentKey = ContentKey.of(nestedFolderPath);
    final Operation rootOperation =
        Operation.Put.of(firstFolderPathContentKey, Namespace.of(firstFolderPathContentKey));
    final Operation intermediateOperation =
        Operation.Put.of(secondFolderPathContentKey, Namespace.of(secondFolderPathContentKey));
    final Operation leafOperation =
        Operation.Put.of(entireFolderPathContentKey, Namespace.of(entireFolderPathContentKey));
    final CommitMeta commitMetaForEntireFolderPath =
        CommitMeta.fromMessage("CREATE FOLDER " + entireFolderPathContentKey);
    final CommitMeta commitMetaForFirstFolder =
        CommitMeta.fromMessage("CREATE FOLDER " + ContentKey.of(nestedFolderPath.subList(0, 1)));
    List<Operation> expectedOperations = Arrays.asList(leafOperation, intermediateOperation);
    RetriableCommitWithNamespaces stateWithAllFolderCommits =
        new RetriableCommitWithNamespaces(
            nessieApi, branchBeforeCommit, commitMetaForEntireFolderPath, leafOperation);

    // Act + Assert
    // creating first folder which is meant to mimic another thread creating the first folder of the
    // entire content path.
    nessieApi
        .commitMultipleOperations()
        .branch(branchBeforeCommit)
        .commitMeta(commitMetaForFirstFolder)
        .operation(rootOperation)
        .commit();
    // asserting size of state's operations to be 1 (only put operation for A.B.C) before the commit
    // and then be 2 (A.B, A.B.C).
    // Note that 'A' is missing since it was already created.
    assertThat(stateWithAllFolderCommits.getOperations().size()).isEqualTo(1);
    assertThat(stateWithAllFolderCommits.tryCommit()).isFalse();
    assertThat(stateWithAllFolderCommits.getOperations().size()).isEqualTo(2);
    assertEquals(expectedOperations, new ArrayList<>(stateWithAllFolderCommits.getOperations()));
    // perform commit with operations ready to go. This will create namespace 'A.B.C'
    stateWithAllFolderCommits.tryCommit();
    assertThat(
            nessieApi
                .getContent()
                .refName(DEFAULT_BRANCH_NAME)
                .key(entireFolderPathContentKey)
                .get())
        .containsKey(entireFolderPathContentKey);
  }

  @Test
  public void createImplicitFoldersWithConcurrencyReachingMaxElementSize() throws Exception {
    // Arrange
    final List<String> nestedFolderPath = Arrays.asList("A", "B", "C");
    final Branch branchBeforeCommit = nessieApi.getDefaultBranch();
    final ContentKey entireFolderPathContentKey = ContentKey.of(nestedFolderPath);
    final Operation leafOperation =
        Operation.Put.of(entireFolderPathContentKey, Namespace.of(entireFolderPathContentKey));
    final CommitMeta commitMetaForEntireFolderPath =
        CommitMeta.fromMessage("CREATE FOLDER " + entireFolderPathContentKey);
    RetriableCommitWithNamespaces stateWithAllFolderCommits =
        new RetriableCommitWithNamespaces(
            nessieApi, branchBeforeCommit, commitMetaForEntireFolderPath, leafOperation);

    // Act + Assert
    assertThat(stateWithAllFolderCommits.getOperations().size()).isEqualTo(1);
    assertThat(stateWithAllFolderCommits.tryCommit()).isFalse();
    assertThat(stateWithAllFolderCommits.getLastConflictException())
        .isInstanceOf(NessieConflictException.class)
        .hasMessageContaining(
            String.format(
                "namespace '%s' must exist, namespace '%s' must exist.",
                entireFolderPathContentKey.getParent(),
                entireFolderPathContentKey.getParent().getParent()));
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(nessieApi);
  }
}
