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
package com.dremio.exec.planner.sql.handlers.query;

import static com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates.ESTIMATED_RECORDS_PER_MANIFEST;
import static com.dremio.exec.planner.sql.handlers.query.VacuumCatalogCostEstimates.DEFAULT_MANIFESTS_PER_SNAPSHOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import java.time.Instant;
import org.junit.Test;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ReferencesResponse;

/** Tests for {@link VacuumCatalogCostEstimates} */
public class TestVacuumCatalogCostEstimates {

  @Test
  public void testAllCommitsLive() {
    long cutOff = Instant.now().minusSeconds(1000).toEpochMilli();
    VacuumOptions vacuumCatalogOptions = new VacuumOptions(true, true, cutOff, 1, null, null);
    long commitsPerBranch = 100L;
    long numRefs = 1L;

    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration configuration = config(Instant.now().minusSeconds(100));
    doReturn(configuration).when(nessieApi).getConfig();
    GetAllReferencesBuilder refs = refs(numRefs, commitsPerBranch, Instant.now().minusSeconds(10));
    doReturn(refs).when(nessieApi).getAllReferences();
    doReturn(refs).when(refs).fetch(FetchOption.ALL);

    IcebergCostEstimates costEstimates =
        VacuumCatalogCostEstimates.find(nessieApi, vacuumCatalogOptions);

    long estimatedSnapshots = commitsPerBranch * numRefs;
    long estimatedManifests = estimatedSnapshots * DEFAULT_MANIFESTS_PER_SNAPSHOT;
    long estimatedDataFiles = estimatedSnapshots * ESTIMATED_RECORDS_PER_MANIFEST;
    long estimatedTotalRows =
        estimatedSnapshots + estimatedManifests + estimatedDataFiles + (estimatedSnapshots * 2);

    assertThat(costEstimates)
        .extracting(
            IcebergCostEstimates::getSnapshotsCount,
            IcebergCostEstimates::getManifestFileEstimatedCount,
            IcebergCostEstimates::getDataFileEstimatedCount,
            IcebergCostEstimates::getEstimatedRows)
        .containsExactly(
            estimatedSnapshots, estimatedManifests, estimatedDataFiles, estimatedTotalRows);
  }

  @Test
  public void testNoCommitsLive() {
    long cutOff = Instant.now().minusSeconds(5).toEpochMilli();
    int retainLast = 2;
    VacuumOptions vacuumCatalogOptions =
        new VacuumOptions(true, true, cutOff, retainLast, null, null);
    long commitsPerBranch = 100L;
    long numRefs = 1L;

    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration configuration = config(Instant.now().minusSeconds(100));
    doReturn(configuration).when(nessieApi).getConfig();
    GetAllReferencesBuilder refs = refs(numRefs, commitsPerBranch, Instant.now().minusSeconds(10));
    doReturn(refs).when(nessieApi).getAllReferences();
    doReturn(refs).when(refs).fetch(FetchOption.ALL);

    IcebergCostEstimates costEstimates =
        VacuumCatalogCostEstimates.find(nessieApi, vacuumCatalogOptions);

    long estimatedSnapshots = retainLast * numRefs; // No commit is live, just follow retain last
    long estimatedManifests = estimatedSnapshots * DEFAULT_MANIFESTS_PER_SNAPSHOT;
    long estimatedDataFiles = estimatedSnapshots * ESTIMATED_RECORDS_PER_MANIFEST;
    long estimatedTotalRows =
        estimatedSnapshots + estimatedManifests + estimatedDataFiles + (estimatedSnapshots * 2);

    assertThat(costEstimates)
        .extracting(
            IcebergCostEstimates::getSnapshotsCount,
            IcebergCostEstimates::getManifestFileEstimatedCount,
            IcebergCostEstimates::getDataFileEstimatedCount,
            IcebergCostEstimates::getEstimatedRows)
        .containsExactly(
            estimatedSnapshots, estimatedManifests, estimatedDataFiles, estimatedTotalRows);
  }

  @Test
  public void testWithCutoffBetweenCommits() {
    long cutOff = Instant.now().minusSeconds(500).toEpochMilli();
    int retainLast = 1;
    VacuumOptions vacuumCatalogOptions =
        new VacuumOptions(true, true, cutOff, retainLast, null, null);
    long commitsPerBranch = 100L;
    long numRefs = 1L;

    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration configuration = config(Instant.now().minusSeconds(1000));
    doReturn(configuration).when(nessieApi).getConfig();
    GetAllReferencesBuilder refs = refs(numRefs, commitsPerBranch, Instant.now());
    doReturn(refs).when(nessieApi).getAllReferences();
    doReturn(refs).when(refs).fetch(FetchOption.ALL);

    IcebergCostEstimates costEstimates =
        VacuumCatalogCostEstimates.find(nessieApi, vacuumCatalogOptions);

    long liveCommits = commitsPerBranch / 2; // half the commits are estimated to be live
    long estimatedSnapshots = liveCommits * numRefs; // No commit is live, just follow retain last
    long estimatedManifests = estimatedSnapshots * DEFAULT_MANIFESTS_PER_SNAPSHOT;
    long estimatedDataFiles = estimatedSnapshots * ESTIMATED_RECORDS_PER_MANIFEST;
    long estimatedTotalRows =
        estimatedSnapshots + estimatedManifests + estimatedDataFiles + (estimatedSnapshots * 2);

    assertThat(costEstimates)
        .extracting(
            IcebergCostEstimates::getSnapshotsCount,
            IcebergCostEstimates::getManifestFileEstimatedCount,
            IcebergCostEstimates::getDataFileEstimatedCount,
            IcebergCostEstimates::getEstimatedRows)
        .containsExactly(
            estimatedSnapshots, estimatedManifests, estimatedDataFiles, estimatedTotalRows);
  }

  @Test
  public void testMultiBranchEstimates() {
    long cutOff = Instant.now().minusSeconds(500).toEpochMilli();
    int retainLast = 1;
    VacuumOptions vacuumCatalogOptions =
        new VacuumOptions(true, true, cutOff, retainLast, null, null);
    long commitsPerBranch = 100L;
    long numRefs = 20L;

    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration configuration = config(Instant.now().minusSeconds(1000));
    doReturn(configuration).when(nessieApi).getConfig();
    GetAllReferencesBuilder refs = refs(numRefs, commitsPerBranch, Instant.now());
    doReturn(refs).when(nessieApi).getAllReferences();
    doReturn(refs).when(refs).fetch(FetchOption.ALL);

    IcebergCostEstimates costEstimates =
        VacuumCatalogCostEstimates.find(nessieApi, vacuumCatalogOptions);

    long liveCommits = (commitsPerBranch / 2); // half the commits are estimated to be live
    long estimatedSnapshots = liveCommits * numRefs; // No commit is live, just follow retain last
    long estimatedManifests = estimatedSnapshots * DEFAULT_MANIFESTS_PER_SNAPSHOT;
    long estimatedDataFiles = estimatedSnapshots * ESTIMATED_RECORDS_PER_MANIFEST;
    long estimatedTotalRows =
        estimatedSnapshots + estimatedManifests + estimatedDataFiles + (estimatedSnapshots * 2);

    assertThat(costEstimates)
        .extracting(
            IcebergCostEstimates::getSnapshotsCount,
            IcebergCostEstimates::getManifestFileEstimatedCount,
            IcebergCostEstimates::getDataFileEstimatedCount,
            IcebergCostEstimates::getEstimatedRows)
        .containsExactly(
            estimatedSnapshots, estimatedManifests, estimatedDataFiles, estimatedTotalRows);
  }

  @Test
  public void testErrorFallback() {
    long cutOff = Instant.now().minusSeconds(500).toEpochMilli();

    VacuumOptions vacuumCatalogOptions = new VacuumOptions(true, true, cutOff, 1, null, null);
    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration configuration = config(Instant.now().minusSeconds(1000));
    doReturn(configuration).when(nessieApi).getConfig();

    doThrow(new RuntimeException("failure test")).when(nessieApi).getAllReferences();

    IcebergCostEstimates costEstimates =
        VacuumCatalogCostEstimates.find(nessieApi, vacuumCatalogOptions);
    assertThat(costEstimates)
        .extracting(IcebergCostEstimates::getSnapshotsCount)
        .isEqualTo(1_000_000L);
  }

  private NessieConfiguration config(Instant initial) {
    return ImmutableNessieConfiguration.builder()
        .maxSupportedApiVersion(63)
        .minSupportedApiVersion(63)
        .oldestPossibleCommitTimestamp(initial)
        .build();
  }

  private GetAllReferencesBuilder refs(long numRefs, long totalCommits, Instant headTime) {
    GetAllReferencesBuilder getAllReferencesBuilder = mock(GetAllReferencesBuilder.class);
    ImmutableReferencesResponse.Builder responseBuilder = ReferencesResponse.builder();

    for (int i = 0; i < numRefs; i++) {
      responseBuilder.addReferences(
          Branch.of(
              "main",
              "3647c32b9e03ef522885baeb07b30a81dbe3a586",
              ImmutableReferenceMetadata.builder()
                  .numTotalCommits(totalCommits)
                  .commitMetaOfHEAD(
                      CommitMeta.builder().commitTime(headTime).message("Put key").build())
                  .build()));
    }
    ImmutableReferencesResponse response = responseBuilder.build();
    doReturn(response).when(getAllReferencesBuilder).get();
    return getAllReferencesBuilder;
  }
}
