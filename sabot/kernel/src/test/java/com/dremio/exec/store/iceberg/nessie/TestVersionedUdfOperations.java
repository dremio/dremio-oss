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
package com.dremio.exec.store.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.util.Retryer;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.iceberg.BaseIcebergTest;
import com.dremio.exec.store.iceberg.VersionedUdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfUtil;
import com.dremio.plugins.NessieClient;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.ImmutableReferenceConflicts;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

class TestVersionedUdfOperations extends BaseIcebergTest {
  protected static final UdfSignature SIGNATURE =
      ImmutableUdfSignature.builder()
          .signatureId(UdfUtil.generateUUID())
          .addParameters(
              required(1, "x", Types.DoubleType.get(), "X coordinate"),
              required(2, "y", Types.DoubleType.get()))
          .returnType(Types.DoubleType.get())
          .deterministic(true)
          .build();

  private static final String CREATE_UDF_SQL = "select sqrt(x * x + y * y)";
  private static final String UDF_USER = "test_user";
  private static final String TEST_BRANCH = "test_branch";
  private static final List<String> UDF_KEY = Arrays.asList("udf", "foo", "bar");

  @Test
  public void testCreate() {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(UDF_KEY));

    VersionedUdfBuilder udfBuilder =
        new VersionedUdfBuilder(UDF_KEY)
            .withVersionContext(getVersion(TEST_BRANCH))
            .withWarehouseLocation(getWarehouseLocation())
            .withNessieClient(nessieClient)
            .withFileIO(getFileIO())
            .withUserName(UDF_USER)
            .withDefaultNamespace(Namespace.of("udf", "foo"))
            .withSignature(SIGNATURE)
            .withBody(
                VersionedUdfMetadata.SupportedUdfDialects.DREMIOSQL.toString(),
                CREATE_UDF_SQL,
                "dremio comment");

    Udf udf = udfBuilder.create();
    assertThat(udf).isNotNull();

    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  @Test
  public void testCreateUnsupportedDialect() {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(UDF_KEY));

    assertThatThrownBy(
            () ->
                new VersionedUdfBuilder(UDF_KEY)
                    .withVersionContext(getVersion(TEST_BRANCH))
                    .withWarehouseLocation(getWarehouseLocation())
                    .withNessieClient(nessieClient)
                    .withFileIO(getFileIO())
                    .withUserName(UDF_USER)
                    .withDefaultNamespace(Namespace.of("udf", "foo"))
                    .withSignature(SIGNATURE)
                    .withBody("dremio", CREATE_UDF_SQL, "dremio comment"))
        .hasMessageContaining("Unsupported Udf dialect");
  }

  @Test
  public void testConcurrentCreate() {
    NessieErrorDetails details =
        ImmutableReferenceConflicts.of(
            List.of(
                Conflict.conflict(
                    Conflict.ConflictType.KEY_EXISTS, ContentKey.of(UDF_KEY), "key exists")));
    UdfVersion version = mock(UdfVersion.class);
    when(version.representations()).thenReturn(List.of(mock(SQLUdfRepresentation.class)));
    UdfMetadata metadata = mock(UdfMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn("location");
    when(metadata.currentVersion()).thenReturn(version);
    when(metadata.currentVersionId()).thenReturn("123");
    when(metadata.currentSignatureId()).thenReturn("123");
    NessieError error =
        ImmutableNessieError.builder()
            .status(409)
            .reason("key exists")
            .errorDetails(details)
            .build();
    ReferenceConflictException e =
        new ReferenceConflictException(new NessieReferenceConflictException(error));
    NessieClient mockNessieClient = mock(NessieClient.class);
    doThrow(e).when(mockNessieClient).commitUdf(any(), any(), any(), any(), any(), any(), any());
    VersionedUdfOperations versionedUdfOperations =
        new VersionedUdfOperations(
            mock(FileIO.class),
            mockNessieClient,
            UDF_KEY,
            ResolvedVersionContext.ofBranch("MAIN", "commitHash"),
            UDF_USER);
    assertThatThrownBy(() -> versionedUdfOperations.doCommit(metadata, metadata))
        .isInstanceOf(Retryer.OperationFailedAfterRetriesException.class);
    verify(mockNessieClient, times(1)).commitUdf(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testConcurrentUpdate() {
    NessieErrorDetails details =
        ImmutableReferenceConflicts.of(
            List.of(
                Conflict.conflict(
                    Conflict.ConflictType.VALUE_DIFFERS, ContentKey.of(UDF_KEY), "value differs")));
    UdfVersion version = mock(UdfVersion.class);
    when(version.representations()).thenReturn(List.of(mock(SQLUdfRepresentation.class)));
    UdfMetadata metadata = mock(UdfMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn("location");
    when(metadata.currentVersion()).thenReturn(version);
    when(metadata.currentVersionId()).thenReturn("123");
    when(metadata.currentSignatureId()).thenReturn("123");
    NessieError error =
        ImmutableNessieError.builder()
            .status(409)
            .reason("value differs")
            .errorDetails(details)
            .build();
    ReferenceConflictException e =
        new ReferenceConflictException(new NessieReferenceConflictException(error));
    NessieClient mockNessieClient = mock(NessieClient.class);
    when(mockNessieClient.resolveVersionContext(any()))
        .thenReturn(ResolvedVersionContext.ofBranch("MAIN", "123"));
    doThrow(e).when(mockNessieClient).commitUdf(any(), any(), any(), any(), any(), any(), any());
    VersionedUdfOperations versionedUdfOperations =
        new VersionedUdfOperations(
            mock(FileIO.class),
            mockNessieClient,
            UDF_KEY,
            ResolvedVersionContext.ofBranch("MAIN", "123"),
            UDF_USER);
    assertThatThrownBy(() -> versionedUdfOperations.doCommit(metadata, metadata))
        .isInstanceOf(Retryer.OperationFailedAfterRetriesException.class);
    verify(mockNessieClient, times(5)).commitUdf(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testConcurrentUpdateSecondTimeSucceed() {
    NessieErrorDetails details =
        ImmutableReferenceConflicts.of(
            List.of(
                Conflict.conflict(
                    Conflict.ConflictType.VALUE_DIFFERS, ContentKey.of(UDF_KEY), "value differs")));
    UdfVersion version = mock(UdfVersion.class);
    when(version.representations()).thenReturn(List.of(mock(SQLUdfRepresentation.class)));
    UdfMetadata metadata = mock(UdfMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn("location");
    when(metadata.currentVersion()).thenReturn(version);
    when(metadata.currentVersionId()).thenReturn("123");
    when(metadata.currentSignatureId()).thenReturn("123");
    NessieError error =
        ImmutableNessieError.builder()
            .status(409)
            .reason("value differs")
            .errorDetails(details)
            .build();
    ReferenceConflictException e =
        new ReferenceConflictException(new NessieReferenceConflictException(error));
    NessieClient mockNessieClient = mock(NessieClient.class);
    when(mockNessieClient.resolveVersionContext(any()))
        .thenReturn(ResolvedVersionContext.ofBranch("MAIN", "123"));
    doThrow(e)
        .doNothing()
        .when(mockNessieClient)
        .commitUdf(any(), any(), any(), any(), any(), any(), any());
    VersionedUdfOperations versionedUdfOperations =
        new VersionedUdfOperations(
            mock(FileIO.class),
            mockNessieClient,
            UDF_KEY,
            ResolvedVersionContext.ofBranch("MAIN", "123"),
            UDF_USER);
    versionedUdfOperations.doCommit(metadata, metadata);
    verify(mockNessieClient, times(2)).commitUdf(any(), any(), any(), any(), any(), any(), any());
  }

  private String getWarehouseLocation() {
    return warehouseLocation;
  }
}
