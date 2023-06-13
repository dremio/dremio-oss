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
package com.dremio.services.nessie.grpc;

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProtoResponse;
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProtoDiffRequest;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProtoEntriesRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.api.v1.params.GetReferenceParams;
import org.projectnessie.api.v1.params.ImmutableMerge;
import org.projectnessie.api.v1.params.ImmutableTransplant;
import org.projectnessie.api.v1.params.Merge;
import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.api.v1.params.Transplant;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.CommitResponse.AddedContent;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableGetNamespacesResponse;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableMergeKeyBehavior;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutableRefLogResponseEntry;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Operations;
import org.projectnessie.model.RefLogResponse.RefLogResponseEntry;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;

import com.dremio.services.nessie.grpc.api.CommitLogEntry;
import com.dremio.services.nessie.grpc.api.CommitLogRequest;
import com.dremio.services.nessie.grpc.api.CommitLogResponse;
import com.dremio.services.nessie.grpc.api.CommitOperation;
import com.dremio.services.nessie.grpc.api.CommitOps;
import com.dremio.services.nessie.grpc.api.ContentRequest;
import com.dremio.services.nessie.grpc.api.DiffRequest;
import com.dremio.services.nessie.grpc.api.DiffResponse;
import com.dremio.services.nessie.grpc.api.EntriesRequest;
import com.dremio.services.nessie.grpc.api.EntriesResponse;
import com.dremio.services.nessie.grpc.api.GetAllReferencesRequest;
import com.dremio.services.nessie.grpc.api.GetReferenceByNameRequest;
import com.dremio.services.nessie.grpc.api.MergeRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;
import com.dremio.services.nessie.grpc.api.RefLogParams;
import com.dremio.services.nessie.grpc.api.RefLogResponse;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;

/**
 * Tests for {@link ProtoUtil}
 */
public class ProtoUtilTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void referenceConversion() {
    assertThatThrownBy(() -> refToProto(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Reference must be non-null");

    assertThatThrownBy(() -> refFromProto(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Reference must be non-null");

    Branch b = Branch.of("main", "1234567890123456");
    Tag t = Tag.of("tag", "1234567890123456");
    Branch b2 = Branch.of("main2", null);
    Tag t2 = Tag.of("tag2", null);
    Detached d = Detached.of("1234567890123456");

    assertThat(refFromProto(refToProto(b))).isEqualTo(b);
    assertThat(refFromProto(refToProto(b2))).isEqualTo(b2);
    assertThat(refFromProto(refToProto(t))).isEqualTo(t);
    assertThat(refFromProto(refToProto(t2))).isEqualTo(t2);
    assertThat(refFromProto(refToProto(d))).isEqualTo(d);
  }

  @Test
  public void refToProtoDecomposed() {
    assertThatThrownBy(() -> refToProto(ReferenceType.BRANCH, null, "hash"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Reference name must be non-null");

    assertThatThrownBy(() -> refToProto(ReferenceType.UNRECOGNIZED, "name", "hash"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Reference type 'UNRECOGNIZED' should be Branch or Tag");

    assertThat(refToProto(ReferenceType.BRANCH, "name", "1234567890123456"))
      .extracting(r -> r.getBranch().getName(), r -> r.getBranch().getHash(), r -> r.getBranch().hasMetadata())
      .containsExactly("name", "1234567890123456", false);
    assertThat(refToProto(ReferenceType.BRANCH, "name", null))
      .extracting(r -> r.getBranch().getName(), r -> r.getBranch().hasHash())
      .containsExactly("name", false);

    assertThat(refToProto(ReferenceType.TAG, "name", "1234567890123456"))
      .extracting(r -> r.getTag().getName(), r -> r.getTag().getHash(), r -> r.getTag().hasMetadata())
      .containsExactly("name", "1234567890123456", false);
  }

  @Test
  public void refFromProtoResponseConversion() {
    assertThatThrownBy(() -> refFromProtoResponse(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Reference response must be non-null");

    assertThat(refFromProtoResponse(ReferenceResponse.getDefaultInstance())).isNull();

    assertThat(refFromProtoResponse(
      ReferenceResponse.newBuilder().setReference(
        com.dremio.services.nessie.grpc.api.Reference.newBuilder()
          .setBranch(com.dremio.services.nessie.grpc.api.Branch.newBuilder().setName("br1").build())
          .build())
        .build()))
      .extracting(r -> r.getType().name(), Reference::getName, Reference::getHash)
      .containsExactly("BRANCH", "br1", null);
  }

  @Test
  public void detachedConversion() {
    assertThatThrownBy(() -> toProto((Detached) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Detached must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Detached) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Detached must be non-null");

    Detached d = Detached.of("1234567890123456");
    assertThat(fromProto(toProto(d))).isEqualTo(d);
  }

  @Test
  public void branchConversion() {
    assertThatThrownBy(() -> toProto((Branch) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Branch must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Branch) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Branch must be non-null");

    String branchName = "main";
    Branch b = Branch.of(branchName, "1234567890123456");
    assertThat(toProto(b).hasHash()).isTrue();
    assertThat(fromProto(toProto(b))).isEqualTo(b).extracting(Reference::getHash).isNotNull();

    Branch branchWithoutHash = Branch.of(branchName, null);
    assertThat(toProto(branchWithoutHash).hasHash()).isFalse();
    assertThat(fromProto(toProto(branchWithoutHash))).isEqualTo(branchWithoutHash).extracting(Reference::getHash).isNull();
  }

  @Test
  public void branchConversionWithMetadata() {
    ReferenceMetadata metadata = ImmutableReferenceMetadata.builder().numCommitsAhead(3)
      .numCommitsBehind(4).numTotalCommits(12L).commonAncestorHash("123")
      .commitMetaOfHEAD(CommitMeta.fromMessage("commit msg")).build();

    Branch branch = ImmutableBranch.builder().name("main").metadata(metadata).build();
    assertThat(fromProto(toProto(branch))).isEqualTo(branch);
  }

  @Test
  public void tagConversion() {
    assertThatThrownBy(() -> toProto((Tag) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Tag must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Tag) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Tag must be non-null");

    String tagName = "main";
    Tag tag = Tag.of(tagName, "1234567890123456");
    assertThat(toProto(tag).hasHash()).isTrue();
    assertThat(fromProto(toProto(tag))).isEqualTo(tag).extracting(Reference::getHash).isNotNull();

    Tag tagWithoutHash = Tag.of(tagName, null);
    assertThat(toProto(tagWithoutHash).hasHash()).isFalse();
    assertThat(fromProto(toProto(tagWithoutHash))).isEqualTo(tagWithoutHash).extracting(Reference::getHash).isNull();
  }

  @Test
  public void tagConversionWithMetadata() {
    ReferenceMetadata metadata = ImmutableReferenceMetadata.builder().numCommitsAhead(3)
      .numCommitsBehind(4).numTotalCommits(12L).commonAncestorHash("123")
      .commitMetaOfHEAD(CommitMeta.fromMessage("commit msg")).build();

    Tag tag = ImmutableTag.builder().name("main").metadata(metadata).build();
    assertThat(fromProto(toProto(tag))).isEqualTo(tag);
  }

  @Test
  public void icebergTableConversion() {
    assertThatThrownBy(() -> toProto((IcebergTable) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("IcebergTable must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.IcebergTable) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("IcebergTable must be non-null");

    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);
    assertThat(fromProto(toProto(icebergTable))).isEqualTo(icebergTable);

    icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42, "test-id");
    assertThat(fromProto(toProto(icebergTable))).isEqualTo(icebergTable);
  }

  @Test
  public void icebergTableMetadataConversion() throws JsonProcessingException {
    JsonNode json = MAPPER.readValue("{\"a\":42}", JsonNode.class);
    IcebergTable icebergTable = IcebergTable.builder()
      .id("test-id")
      .schemaId(1)
      .snapshotId(2)
      .sortOrderId(3)
      .specId(4)
      .metadataLocation("file")
      .metadata(ImmutableMap.of("test", json))
      .build();
    // DX-57058: metadata should be null
    assertThat(fromProto(toProto(icebergTable)).getMetadata()).isNull();
  }

  @Test
  public void icebergViewConversion() {
    assertThatThrownBy(() -> toProto((IcebergView) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("IcebergView must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.IcebergView) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("IcebergView must be non-null");

    IcebergView icebergView = IcebergView.of("test.me.txt", 42, 42, "dialect", "SELECT foo FROM bar");
    assertThat(fromProto(toProto(icebergView))).isEqualTo(icebergView);

    icebergView = IcebergView.of("test-id", "test.me.txt", 42, 42, "dialect", "SELECT foo FROM bar");
    assertThat(fromProto(toProto(icebergView))).isEqualTo(icebergView);
  }


  @Test
  public void icebergViewMetadataConversion() throws JsonProcessingException {
    JsonNode json = MAPPER.readValue("{\"a\":42}", JsonNode.class);
    IcebergView icebergView = IcebergView.builder()
      .id("test-id")
      .schemaId(1)
      .versionId(2)
      .dialect("test-dialect")
      .sqlText("SELECT 1")
      .metadataLocation("file")
      .metadata(ImmutableMap.of("test", json))
      .build();
    // DX-57058: metadata should be null
    assertThat(fromProto(toProto(icebergView)).getMetadata()).isNull();
  }

  @Test
  public void deltaLakeTableConversion() {
    assertThatThrownBy(() -> toProto((DeltaLakeTable) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DeltaLakeTable must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.DeltaLakeTable) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DeltaLakeTable must be non-null");

    DeltaLakeTable deltaLakeTable =
      ImmutableDeltaLakeTable.builder()
        .addMetadataLocationHistory("a", "b")
        .addCheckpointLocationHistory("c", "d")
        .lastCheckpoint("c")
        .build();

    assertThat(fromProto(toProto(deltaLakeTable))).isEqualTo(deltaLakeTable);

    DeltaLakeTable deltaLakeTableWithId = ImmutableDeltaLakeTable.builder().from(deltaLakeTable).id("test-id").build();
    assertThat(fromProto(toProto(deltaLakeTableWithId))).isEqualTo(deltaLakeTableWithId);

    DeltaLakeTable deltaLakeTableWithoutLastCheckpoint =
      ImmutableDeltaLakeTable.builder().from(deltaLakeTable).lastCheckpoint(null).build();
    assertThat(fromProto(toProto(deltaLakeTableWithoutLastCheckpoint)))
      .isEqualTo(deltaLakeTableWithoutLastCheckpoint);
  }

  @Test
  public void nessieConfigurationConversion() {
    assertThatThrownBy(() -> toProto((NessieConfiguration) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("NessieConfiguration must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.NessieConfiguration) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("NessieConfiguration must be non-null");

    NessieConfiguration config =
      ImmutableNessieConfiguration.builder().maxSupportedApiVersion(42).defaultBranch("main").build();
    assertThat(fromProto(toProto(config))).isEqualTo(config);

    NessieConfiguration config2 =
      ImmutableNessieConfiguration.builder()
        .maxSupportedApiVersion(42)
        .minSupportedApiVersion(24)
        .actualApiVersion(99)
        .specVersion("spec-test")
        .defaultBranch("mymain")
        .noAncestorHash("myhash")
        .repositoryCreationTimestamp(Instant.now())
        .oldestPossibleCommitTimestamp(Instant.now())
        .additionalProperties(ImmutableMap.of("foo", "bar"))
        .build();
    assertThat(fromProto(toProto(config2))).isEqualTo(config2);
  }

  @Test
  public void contentConversion() {
    assertThatThrownBy(() -> toProto((Content) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Content must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Content) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Content must be non-null");

    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);
    assertThat(fromProto(toProto((Content) icebergTable))).isEqualTo(icebergTable);

    Namespace namespace = Namespace.of("a", "b", "c");
    assertThat(fromProto(toProto((Content) namespace))).isEqualTo(namespace);
  }

  @Test
  public void contentKeyConversion() {
    assertThatThrownBy(() -> toProto((ContentKey) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ContentKey must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.ContentKey) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ContentKey must be non-null");

    ContentKey key = ContentKey.of("a.b.c.txt");
    assertThat(fromProto(toProto(key))).isEqualTo(key);
  }

  @Test
  public void contentWithKeyConversion() {
    assertThatThrownBy(() -> toProto((ContentWithKey) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ContentWithKey must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.ContentWithKey) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ContentWithKey must be non-null");

    ContentKey key = ContentKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);
    ContentWithKey c = ContentWithKey.of(key, icebergTable);
    assertThat(fromProto(toProto(c))).isEqualTo(c);
  }

  @Test
  public void entryConversion() {
    assertThatThrownBy(() -> toProto((Entry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Entry must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Entry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Entry must be non-null");

    Entry entry =
      ImmutableEntry.builder().name(ContentKey.of("a.b.c.txt")).type(Type.ICEBERG_TABLE).build();
    assertThat(fromProto(toProto(entry))).isEqualTo(entry);

    Entry entryWithContent = ImmutableEntry.builder().from(entry)
      .contentId("id").content(IcebergTable.of("loc", 1, 2, 3, 4, "id")).build();
    assertThat(fromProto(toProto(entryWithContent))).isEqualTo(entryWithContent);
  }

  @Test
  public void commitMetaConversion() {
    assertThatThrownBy(() -> toProto((CommitMeta) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitMeta must be non-null");
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.CommitMeta) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitMeta must be non-null");

    CommitMeta commitMeta =
      CommitMeta.builder()
        .author("eduard")
        .message("commit msg")
        .commitTime(Instant.now())
        .authorTime(Instant.now())
        .properties(ImmutableMap.of("a", "b"))
        .hash("1234567890123456")
        .signedOffBy("me")
        .build();
    assertThat(fromProto(toProto(commitMeta))).isEqualTo(commitMeta);

    CommitMeta minimalCommitMeta =
      CommitMeta.builder().message("commit msg").properties(ImmutableMap.of("a", "b")).build();
    assertThat(fromProto(toProto(minimalCommitMeta))).isEqualTo(minimalCommitMeta);

    CommitMeta commitMetaWithParents = CommitMeta.builder()
        .from(minimalCommitMeta)
        .addParentCommitHashes("1122334455667700")
        .addParentCommitHashes("1122334455667701")
        .addParentCommitHashes("1122334455667702")
        .build();
    assertThat(fromProto(toProto(commitMetaWithParents))).isEqualTo(commitMetaWithParents);
  }

  @Test
  public void instantConversion() {
    assertThatThrownBy(() -> toProto((Instant) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Timestamp must be non-null");

    assertThatThrownBy(() -> fromProto((Timestamp) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Timestamp must be non-null");

    Instant instant = Instant.now();
    assertThat(fromProto(toProto(instant))).isEqualTo(instant);
  }

  @Test
  public void operationConversion() {
    assertThatThrownBy(() -> toProto((Operation) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitOperation must be non-null");

    assertThatThrownBy(() -> fromProto((CommitOperation) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitOperation must be non-null");

    ContentKey key = ContentKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);

    Put put = Put.of(key, icebergTable, icebergTable);
    Delete delete = Delete.of(key);
    Unchanged unchanged = Unchanged.of(key);

    assertThat(fromProto(toProto(put))).isEqualTo(put);
    assertThat(fromProto(toProto(delete))).isEqualTo(delete);
    assertThat(fromProto(toProto(unchanged))).isEqualTo(unchanged);
  }

  @Test
  public void operationsConversion() {
    assertThatThrownBy(() -> toProto((Operations) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitOperations must be non-null");

    assertThatThrownBy(() -> fromProto((CommitOps) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitOperations must be non-null");

    CommitMeta commitMeta =
      CommitMeta.builder()
        .author("eduard")
        .message("commit msg")
        .commitTime(Instant.now())
        .authorTime(Instant.now())
        .properties(ImmutableMap.of("a", "b"))
        .hash("1234567890123456")
        .signedOffBy("me")
        .build();

    ContentKey key = ContentKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);

    Put put = Put.of(key, icebergTable);
    Delete delete = Delete.of(key);
    Unchanged unchanged = Unchanged.of(key);
    Operations commitOps =
      ImmutableOperations.builder()
        .commitMeta(commitMeta)
        .addOperations(put, delete, unchanged)
        .build();

    assertThat(fromProto(toProto(commitOps))).isEqualTo(commitOps);
  }

  @Test
  public void entriesRequestConversion() {
    assertThatThrownBy(() -> fromProto((EntriesRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("EntriesRequest must be non-null");

    EntriesParams params =
      EntriesParams.builder()
        .filter("a > b")
        .hashOnRef("123")
        .maxRecords(23)
        .build();
    assertThat(fromProto(toProtoEntriesRequest("main", "123", 23, "a > b", null, false, null, null, null, null)))
      .isEqualTo(params);
    assertThat(fromProto(toProtoEntriesRequest(null, "123", 23, "a > b", null, false, null, null, null, null)))
      .isEqualTo(params);
    assertThat(toProtoEntriesRequest(null, "1", 1, "", null, false, null, null, null, null).getNamedRef())
      .isEqualTo(Detached.REF_NAME);
    assertThat(toProtoEntriesRequest("main", "1", 1, "", null, false, null, null, null, null).getNamedRef())
      .isEqualTo("main");
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, false, null, null, null, null).getWithContent())
      .isFalse();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null, null).getWithContent())
      .isTrue();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null, null).hasMinKey())
      .isFalse();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null, null).hasMaxKey())
      .isFalse();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null, null).hasPrefixKey())
      .isFalse();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null, null).getKeysList())
      .isEmpty();
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, ContentKey.of("min"), null, null, null)
      .getMinKey().getElementsList()).containsExactly("min");
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, ContentKey.of("max"), null, null)
      .getMaxKey().getElementsList()).containsExactly("max");
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, ContentKey.of("prefix"), null)
      .getPrefixKey().getElementsList()).containsExactly("prefix");
    assertThat(toProtoEntriesRequest(null, "1", 1, "1", null, true, null, null, null,
      ImmutableList.of(ContentKey.of("k1"), ContentKey.of("k2"))).getKeysList())
      .map(ProtoUtil::fromProto)
      .containsExactly(ContentKey.of("k1"), ContentKey.of("k2"));

    assertThat(fromProto(
      toProtoEntriesRequest("main", "123", 23, "a > b", null, false, null, null, null, null).toBuilder()
        .setPageToken("token1").build()))
      .isEqualTo(params.forNextPage("token1"));
  }

  @Test
  public void commitLogRequestConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogRequest must be non-null");

    assertThatThrownBy(() -> toProto("main", (CommitLogParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogParams must be non-null");

    CommitLogParams params =
      CommitLogParams.builder()
        .filter("a > b")
        .startHash("123")
        .endHash("456")
        .maxRecords(23)
        .pageToken("abc")
        .fetchOption(FetchOption.ALL)
        .build();
    assertThat(fromProto(toProto("main", params))).isEqualTo(params);
    assertThat(fromProto(toProto(null, params))).isEqualTo(params);
    assertThat(toProto(null, params).getNamedRef()).isEqualTo(Detached.REF_NAME);

    CommitLogParams empty = CommitLogParams.empty();
    assertThat(fromProto(toProto("main", empty))).isEqualTo(empty);
  }

  @Test
  public void entriesResponseConversion() {
    assertThatThrownBy(() -> fromProto((EntriesResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("EntriesResponse must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.model.EntriesResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("EntriesResponse must be non-null");

    List<Entry> entries =
      Arrays.asList(
        ImmutableEntry.builder()
          .name(ContentKey.of("a.b.c.txt"))
          .type(Type.ICEBERG_TABLE)
          .build(),
        ImmutableEntry.builder()
          .name(ContentKey.of("a.b.d.txt"))
          .type(Type.DELTA_LAKE_TABLE)
          .build(),
        ImmutableEntry.builder().name(ContentKey.of("a.b.e.txt")).type(Type.ICEBERG_VIEW).build());
    org.projectnessie.model.EntriesResponse response =
      org.projectnessie.model.EntriesResponse.builder().entries(entries).build();
    assertThat(fromProto(toProto(response))).isEqualTo(response);

    org.projectnessie.model.EntriesResponse responseWithToken =
      org.projectnessie.model.EntriesResponse.builder().entries(entries).token("abc").build();
    assertThat(fromProto(toProto(responseWithToken))).isEqualTo(responseWithToken);

    org.projectnessie.model.EntriesResponse responseWithRef =
      org.projectnessie.model.EntriesResponse.builder()
        .entries(entries)
        .effectiveReference(Branch.of("ref", null))
        .build();
    assertThat(fromProto(toProto(responseWithRef))).isEqualTo(responseWithRef);
  }

  @Test
  public void commitLogResponseConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogResponse must be non-null");

    assertThatThrownBy(() -> toProto((LogResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogResponse must be non-null");

    List<LogEntry> commits =
      Arrays.asList(
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .author("eduard")
            .message("commit msg")
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .properties(ImmutableMap.of("a", "b"))
            .signedOffBy("me")
            .build()).build(),
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .message("commit msg2")
            .properties(ImmutableMap.of("a", "b"))
            .build()).build());

    LogResponse logResponse = ImmutableLogResponse.builder().logEntries(commits).build();
    assertThat(fromProto(toProto(logResponse))).isEqualTo(logResponse);

    LogResponse logResponseWithToken =
      ImmutableLogResponse.builder().logEntries(commits).token("abc").build();
    assertThat(fromProto(toProto(logResponseWithToken))).isEqualTo(logResponseWithToken);
  }

  @Test
  public void logEntryConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogEntry must be non-null");

    assertThatThrownBy(() -> toProto((LogEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("LogEntry must be non-null");

    CommitMeta one = CommitMeta.fromMessage("commit msg 1");
    assertThat(toProto(LogEntry.builder().commitMeta(one).build())).isEqualTo(
      CommitLogEntry.newBuilder().setCommitMeta(toProto(one)).build());

    ContentKey key = ContentKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);

    Put put = Put.of(key, icebergTable);
    Delete delete = Delete.of(key);
    Unchanged unchanged = Unchanged.of(key);

    ImmutableLogEntry logEntry = LogEntry.builder().commitMeta(one).parentCommitHash("xyz")
      .addOperations(put, delete, unchanged).build();

    assertThat(fromProto(toProto(logEntry))).isEqualTo(logEntry);
  }

  @Test
  public void referenceMetadataConversion() {
    assertThatThrownBy(() -> toProto((ReferenceMetadata) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ReferenceMetadata must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.ReferenceMetadata) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ReferenceMetadata must be non-null");

    ReferenceMetadata emptyMetadata = ImmutableReferenceMetadata.builder().build();
    assertThat(fromProto(toProto(emptyMetadata))).isEqualTo(emptyMetadata);

    CommitMeta commitMeta = CommitMeta.fromMessage("commit msg");

    ReferenceMetadata metadata = ImmutableReferenceMetadata.builder().numCommitsAhead(3)
      .numCommitsBehind(4).numTotalCommits(12L).commonAncestorHash("123")
      .commitMetaOfHEAD(commitMeta).build();
    assertThat(fromProto(toProto(metadata))).isEqualTo(metadata);
  }

  @Test
  public void contentRequestConversion() {
    assertThatThrownBy(() -> toProto((ContentKey) null, "ref", null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ContentKey must be non-null");

    ContentKey key = ContentKey.of("test.me.txt");
    String ref = "main";
    String hashOnRef = "x";
    ContentRequest request = toProto(key, ref, null);
    assertThat(request.getContentKey()).isEqualTo(toProto(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEmpty();

    request = toProto(key, ref, hashOnRef);
    assertThat(request.getContentKey()).isEqualTo(toProto(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);

    request = toProto(key, null, hashOnRef);
    assertThat(request.getContentKey()).isEqualTo(toProto(key));
    assertThat(request.getRef()).isEqualTo(Detached.REF_NAME);
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);
  }

  @Test
  public void multipleContentsRequestConversion() {
    String ref = "main";
    String hashOnRef = "x";
    ContentKey key = ContentKey.of("test.me.txt");
    MultipleContentsRequest request = toProto(ref, null, GetMultipleContentsRequest.of(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEmpty();
    assertThat(request.getRequestedKeysList()).containsExactly(toProto(key));

    request = toProto(ref, hashOnRef, GetMultipleContentsRequest.of(key));
    assertThat(request.getRef()).isEqualTo(ref);
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);
    assertThat(request.getRequestedKeysList()).containsExactly(toProto(key));

    request = toProto(null, hashOnRef, GetMultipleContentsRequest.of(key));
    assertThat(request.getRef()).isEqualTo("");
    assertThat(request.getHashOnRef()).isEqualTo(hashOnRef);
    assertThat(request.getRequestedKeysList()).containsExactly(toProto(key));
  }

  @Test
  public void multipleContentsResponseConversion() {
    assertThatThrownBy(() -> toProto((GetMultipleContentsResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("GetMultipleContentsResponse must be non-null");

    assertThatThrownBy(() -> fromProto((MultipleContentsResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MultipleContentsResponse must be non-null");

    ContentKey key = ContentKey.of("a.b.c.txt");
    IcebergTable icebergTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);
    ContentWithKey c = ContentWithKey.of(key, icebergTable);

    GetMultipleContentsResponse response = GetMultipleContentsResponse.of(Collections.singletonList(c), null);
    assertThat(fromProto(toProto(response))).isEqualTo(response);

    response = GetMultipleContentsResponse.of(Collections.singletonList(c), Branch.of("test", null));
    assertThat(fromProto(toProto(response))).isEqualTo(response);
  }

  @Test
  public void allReferencesRequestConversion() {
    assertThatThrownBy(() -> fromProto((GetAllReferencesRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("GetAllReferencesRequest must be non-null");

    assertThatThrownBy(() -> toProto((ReferencesParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ReferencesParams must be non-null");

    ReferencesParams emptyParams = ReferencesParams.empty();
    assertThat(fromProto(toProto(emptyParams))).isEqualTo(emptyParams);

    ReferencesParams params = ReferencesParams.builder()
      .maxRecords(3)
      .pageToken("xx")
      .fetchOption(FetchOption.ALL)
      .filter("a > b")
      .build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);
  }

  @Test
  public void getReferenceByNameConversion() {
    assertThatThrownBy(() -> fromProto((GetReferenceByNameRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("GetReferenceByNameRequest must be non-null");

    assertThatThrownBy(() -> toProto((GetReferenceParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("GetReferenceParams must be non-null");

    GetReferenceParams params = GetReferenceParams.builder().refName("x").build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = GetReferenceParams.builder().refName("x").fetchOption(FetchOption.ALL).build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);
  }

  @Test
  public void diffEntryConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.DiffEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffEntry must be non-null");

    assertThatThrownBy(() -> toProto((DiffEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffEntry must be non-null");

    ContentKey key = ContentKey.of("test.txt");
    DiffEntry diffEntry = ImmutableDiffEntry.builder().key(key).build();
    assertThat(fromProto(toProto(diffEntry))).isEqualTo(diffEntry);

    IcebergTable fromTable = IcebergTable.of("test.me.txt", 42L, 42, 42, 42);
    IcebergTable toTable = IcebergTable.of("test.me.txt", 43L, 43, 43, 43);
    diffEntry = ImmutableDiffEntry.builder().key(key).from(fromTable).to(toTable).build();
    assertThat(fromProto(toProto(diffEntry))).isEqualTo(diffEntry);
  }

  @Test
  public void diffRequestConversion() {
    DiffRequest request = toProtoDiffRequest("from", "fromHash", "to", "toHash", null, null, null, null, null, null);
    assertThat(request.getFromRefName()).isEqualTo("from");
    assertThat(request.getFromHashOnRef()).isEqualTo("fromHash");
    assertThat(request.getToRefName()).isEqualTo("to");
    assertThat(request.getToHashOnRef()).isEqualTo("toHash");
    assertThat(request.hasMaxRecords()).isFalse();
    assertThat(request.hasPageToken()).isFalse();
    assertThat(request.hasMinKey()).isFalse();
    assertThat(request.hasMaxKey()).isFalse();
    assertThat(request.hasPrefixKey()).isFalse();
    assertThat(request.hasFilter()).isFalse();
    assertThat(request.getKeysList()).isEmpty();

    request = toProtoDiffRequest(null, null, null, null, 42, ContentKey.of("min"), ContentKey.of("max"),
      ContentKey.of("prefix"), ImmutableList.of(ContentKey.of("k1"), ContentKey.of("k2")), "filter");
    assertThat(request.getFromRefName()).isEqualTo(Detached.REF_NAME);
    assertThat(request.hasFromHashOnRef()).isFalse();
    assertThat(request.getToRefName()).isEqualTo(Detached.REF_NAME);
    assertThat(request.hasToHashOnRef()).isFalse();
    assertThat(request.getMaxRecords()).isEqualTo(42);
    assertThat(request.hasPageToken()).isFalse();
    assertThat(request.getMinKey().getElementsList()).containsExactly("min");
    assertThat(request.getMaxKey().getElementsList()).containsExactly("max");
    assertThat(request.getPrefixKey().getElementsList()).containsExactly("prefix");
    assertThat(request.getKeysList()).containsExactly(toProto(ContentKey.of("k1")), toProto(ContentKey.of("k2")));
    assertThat(request.getFilter()).isEqualTo("filter");
  }

  @Test
  public void diffResponseConversion() {
    assertThatThrownBy(() -> fromProto((DiffResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffResponse must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.model.DiffResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffResponse must be non-null");

    ImmutableDiffResponse empty = ImmutableDiffResponse.builder().build();
    assertThat(fromProto(toProto(empty))).isEqualTo(empty);

    List<DiffEntry> diffs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ContentKey key = ContentKey.of("test.txt" + i);
      IcebergTable fromTable = IcebergTable.of("test.me.txt", (long) i, i, i, i);
      IcebergTable toTable = IcebergTable.of("test.me.txt", i + 1, i + 1, i + 1, i + 1);
      diffs.add(ImmutableDiffEntry.builder().key(key).from(fromTable).to(toTable).build());
    }

    ImmutableDiffResponse diffResponse = ImmutableDiffResponse.builder().addAllDiffs(diffs).build();
    assertThat(fromProto(toProto(diffResponse))).isEqualTo(diffResponse);

    ImmutableDiffResponse diffResponseWithMore = ImmutableDiffResponse.builder().isHasMore(true).build();
    assertThat(fromProto(toProto(diffResponseWithMore))).isEqualTo(diffResponseWithMore);

    ImmutableDiffResponse diffResponseWithToken = ImmutableDiffResponse.builder()
      .isHasMore(true)
      .token("token123")
      .build();
    assertThat(fromProto(toProto(diffResponseWithToken))).isEqualTo(diffResponseWithToken);

    Branch from = Branch.of("from", null);
    Tag to = Tag.of("from", "1234567890123456");
    ImmutableDiffResponse diffResponseWithRefs = ImmutableDiffResponse.builder()
      .addAllDiffs(diffs)
      .effectiveFromReference(from)
      .effectiveToReference(to)
      .build();
    assertThat(fromProto(toProto(diffResponseWithRefs))).isEqualTo(diffResponseWithRefs);
  }

  @Test
  public void refLogParamsConversion() {
    assertThatThrownBy(() -> fromProto((RefLogParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogParams must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.api.v1.params.RefLogParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogParams must be non-null");

    org.projectnessie.api.v1.params.RefLogParams params =
      org.projectnessie.api.v1.params.RefLogParams.builder()
        .startHash("foo")
        .endHash("bar")
        .maxRecords(23)
        .pageToken("abc")
        .build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    org.projectnessie.api.v1.params.RefLogParams empty = org.projectnessie.api.v1.params.RefLogParams.empty();
    assertThat(fromProto(toProto(empty))).isEqualTo(empty);
  }

  @Test
  public void refLogResponseConversion() {
    assertThatThrownBy(() -> fromProto((RefLogResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogResponse must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.model.RefLogResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogResponse must be non-null");

    List<RefLogResponseEntry> entries =
      Collections.singletonList(
        ImmutableRefLogResponseEntry.builder()
          .refLogId("123")
          .refType("branch")
          .refName("ref")
          .operation("test")
          .operationTime(123L)
          .commitHash("beef")
          .parentRefLogId("cafe")
          .sourceHashes(Collections.singletonList("babe"))
          .build());
    org.projectnessie.model.RefLogResponse response =
      org.projectnessie.model.ImmutableRefLogResponse.builder().logEntries(entries)
        .isHasMore(true)
        .token("token-foo")
        .build();
    assertThat(fromProto(toProto(response))).isEqualTo(response);
  }

  @Test
  public void refLogEntryConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.RefLogResponseEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogResponseEntry must be non-null");

    assertThatThrownBy(() -> toProto((RefLogResponseEntry) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogResponseEntry must be non-null");

    RefLogResponseEntry refLogEntry = ImmutableRefLogResponseEntry.builder()
      .refLogId("123")
      .refType("branch")
      .refName("ref")
      .operation("test")
      .operationTime(123L)
      .commitHash("beef")
      .parentRefLogId("cafe")
      .sourceHashes(Collections.singletonList("babe"))
      .build();

    assertThat(fromProto(toProto(refLogEntry))).isEqualTo(refLogEntry);
  }

  @Test
  public void namespaceConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.Namespace) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Namespace must be non-null");

    assertThatThrownBy(() -> toProto((Namespace) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Namespace must be non-null");

    Namespace namespace = Namespace.of("a", "b", "c");
    assertThat(fromProto(toProto(namespace))).isEqualTo(namespace);

    Namespace namespaceWithId = ImmutableNamespace.builder().from(namespace).id("id1").build();
    assertThat(fromProto(toProto(namespaceWithId))).isEqualTo(namespaceWithId);

    assertThat(fromProto(toProto(Namespace.EMPTY))).isEqualTo(Namespace.EMPTY);
    Namespace namespaceWithProperties = Namespace.of(ImmutableMap.of("key1", "prop1"), "a", "b", "c");
    assertThat(fromProto(toProto(namespaceWithProperties))).isEqualTo(namespaceWithProperties);
  }

  @Test
  public void namespaceParamsConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.NamespaceRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("NamespaceRequest must be non-null");

    assertThatThrownBy(() -> toProto((NamespaceParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("NamespaceParams must be non-null");

    NamespaceParams params = NamespaceParams.builder()
      .refName("main")
      .namespace(Namespace.of("a", "b", "c"))
      .build();

    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = NamespaceParams.builder()
      .refName("main")
      .namespace(Namespace.of("a", "b", "c"))
      .hashOnRef("someHash")
      .build();

    assertThat(fromProto(toProto(params))).isEqualTo(params);
  }

  @Test
  public void multipleNamespaceParamsConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.MultipleNamespacesRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MultipleNamespacesRequest must be non-null");

    assertThatThrownBy(() -> toProto((MultipleNamespacesParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MultipleNamespacesParams must be non-null");

    MultipleNamespacesParams params = MultipleNamespacesParams.builder()
      .refName("main")
      .build();

    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = MultipleNamespacesParams.builder()
      .refName("main")
      .namespace(Namespace.of("a", "b", "c"))
      .hashOnRef("someHash")
      .build();

    assertThat(fromProto(toProto(params))).isEqualTo(params);
  }

  @Test
  public void multipleNamespaceResponseConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.MultipleNamespacesResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MultipleNamespacesResponse must be non-null");

    assertThatThrownBy(() -> toProto((GetNamespacesResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("GetNamespacesResponse must be non-null");

    GetNamespacesResponse empty = ImmutableGetNamespacesResponse.builder().build();

    assertThat(fromProto(toProto(empty))).isEqualTo(empty);

    GetNamespacesResponse response = ImmutableGetNamespacesResponse.builder()
      .addNamespaces(Namespace.of("a", "b", "c"), Namespace.of("a", "b", "d"))
      .build();

    assertThat(fromProto(toProto(response))).isEqualTo(response);
  }

  private CommitMeta toCommitMeta(MergeRequest request) {
    return ProtoUtil.fromProto(request::getMessage, request::hasCommitMeta, request::getCommitMeta);
  }

  @Test
  public void mergeConversion() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.MergeRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MergeRequest must be non-null");

    assertThatThrownBy(() -> toProto("main", "x", (Merge) null, null, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Merge must be non-null");

    String hash = "1234567890123456";

    Merge merge = ImmutableMerge.builder().fromRefName("main").fromHash(hash).build();
    assertThat(fromProto(toProto("y", "z", merge, null, null))).isEqualTo(merge);
    assertThat(fromProto(toProto("y", "z", merge, "msg", null))).isEqualTo(merge);
    assertThat(toCommitMeta(toProto("y", "z", merge, null, null)).getMessage()).isEmpty();
    assertThat(toCommitMeta(toProto("y", "z", merge, "m1", null)).getMessage()).isEqualTo("m1");
    assertThat(toCommitMeta(toProto("y", "z", merge, "m1", CommitMeta.fromMessage(""))).getMessage()).isEqualTo("m1");
    assertThat(toCommitMeta(toProto("y", "z", merge, "m1", CommitMeta.fromMessage("m2"))).getMessage()).isEqualTo("m2");
    assertThat(toCommitMeta(toProto("y", "z", merge, "", CommitMeta.fromMessage("m2"))).getMessage()).isEqualTo("m2");
    assertThat(toCommitMeta(toProto("y", "z", merge, "", CommitMeta.builder().author("a2").message("").build()))
      .getAuthor()).isEqualTo("a2");

    Merge mergeWithKeepingCommits = ImmutableMerge.builder()
      .keepIndividualCommits(true)
      .fromRefName("main")
      .fromHash(hash)
      .build();
    assertThat(fromProto(toProto("y", "z", mergeWithKeepingCommits, null, null))).isEqualTo(mergeWithKeepingCommits);

    Merge mergeWithExtraInfo = ImmutableMerge.builder()
      .from(mergeWithKeepingCommits)
      .isReturnConflictAsResult(true)
      .isFetchAdditionalInfo(true)
      .defaultKeyMergeMode(org.projectnessie.model.MergeBehavior.FORCE)
      .addKeyMergeModes(ImmutableMergeKeyBehavior.builder()
        .mergeBehavior(org.projectnessie.model.MergeBehavior.DROP)
        .key(ContentKey.of("test", "key"))
        .build())
      .isDryRun(true)
      .isReturnConflictAsResult(true)
      .build();
    assertThat(fromProto(toProto("y", "z", mergeWithExtraInfo, null, null))).isEqualTo(mergeWithExtraInfo);
  }

  @Test
  public void transplant() {
    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.TransplantRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("TransplantRequest must be non-null");

    assertThatThrownBy(() -> toProto("main", "x", "msg", null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Transplant must be non-null");

    String hash = "1234567890123456";

    Transplant transplant = ImmutableTransplant.builder().fromRefName("main")
      .hashesToTransplant(Collections.singletonList(hash))
      .build();

    assertThat(fromProto(toProto("y", "z", "msg", transplant))).isEqualTo(transplant);

    Transplant transplantWithKeepingCommits = ImmutableTransplant.builder().fromRefName("main")
      .hashesToTransplant(Collections.singletonList(hash))
      .keepIndividualCommits(true)
      .build();

    assertThat(fromProto(toProto("y", "z", "msg", transplantWithKeepingCommits))).isEqualTo(transplantWithKeepingCommits);

    Transplant transplantWithExtraInfo = ImmutableTransplant.builder()
      .from(transplantWithKeepingCommits)
      .isReturnConflictAsResult(true)
      .isFetchAdditionalInfo(true)
      .defaultKeyMergeMode(org.projectnessie.model.MergeBehavior.FORCE)
      .addKeyMergeModes(ImmutableMergeKeyBehavior.builder()
        .mergeBehavior(org.projectnessie.model.MergeBehavior.DROP)
        .key(ContentKey.of("test", "key"))
        .build())
      .isDryRun(true)
      .isReturnConflictAsResult(true)
      .build();
    assertThat(fromProto(toProto("y", "z", "msg", transplantWithExtraInfo))).isEqualTo(transplantWithExtraInfo);
  }

  @Test
  public void mergeResponse() {
    assertThatThrownBy(() -> toProto((MergeResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MergeResponse must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.MergeResponse) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("MergeResponse must be non-null");

    MergeResponse mergeResponse = ImmutableMergeResponse.builder()
      .wasApplied(false)
      .wasSuccessful(true)
      .targetBranch("test")
      .effectiveTargetHash("1234567890123456")
      .build();
    assertThat(fromProto(toProto(mergeResponse))).isEqualTo(mergeResponse);

    List<LogEntry> commits =
      Arrays.asList(
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .author("test")
            .message("commit msg")
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .properties(ImmutableMap.of("a", "b"))
            .signedOffBy("test1")
            .build()).build(),
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .message("commit msg2")
            .properties(ImmutableMap.of("a", "b"))
            .build()).build());

    mergeResponse = ImmutableMergeResponse.builder().from(mergeResponse)
      .targetCommits(Collections.emptyList())
      .build();
    assertThat(fromProto(toProto(mergeResponse))).isEqualTo(mergeResponse);

    mergeResponse = ImmutableMergeResponse.builder().from(mergeResponse)
      .wasApplied(true)
      .wasSuccessful(false)
      .resultantTargetHash("54321")
      .commonAncestor("c12345")
      .expectedHash("e12345")
      .sourceCommits(ImmutableList.of(
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .author("test")
            .message("commit msg")
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .properties(ImmutableMap.of("a", "b"))
            .signedOffBy("test1")
            .build())
          .build()))
      .targetCommits(ImmutableList.of(
        LogEntry.builder().commitMeta(
          CommitMeta.builder()
            .message("commit msg2")
            .properties(ImmutableMap.of("a1", "b1"))
            .build())
          .build()))
      .details(ImmutableList.of(
        ImmutableContentKeyDetails.builder()
          .key(ContentKey.of("test", "key"))
          .mergeBehavior(org.projectnessie.model.MergeBehavior.FORCE)
          .conflictType(MergeResponse.ContentKeyConflict.UNRESOLVABLE)
          .sourceCommits(ImmutableList.of("a", "b"))
          .targetCommits(ImmutableList.of("c", "d"))
          .build()))
      .build();
    assertThat(fromProto(toProto(mergeResponse))).isEqualTo(mergeResponse);
  }

  @Test
  public void legacyCommitResponse() throws IOException {
    ReferenceMetadata meta = ImmutableReferenceMetadata.builder().numCommitsAhead(1).numCommitsBehind(2).build();
    Branch branch = Branch.builder().name("name").hash("1122334455667788").metadata(meta).build();

    // Legacy servers return a Branch from commitMultipleOperations()
    com.dremio.services.nessie.grpc.api.Branch protoBranch = toProto(branch);
    ByteArrayOutputStream responsePayload = new ByteArrayOutputStream();
    protoBranch.writeTo(responsePayload);

    CommitResponse commitResponse = fromProto(
            com.dremio.services.nessie.grpc.api.CommitResponse.parseFrom(responsePayload.toByteArray()));
    assertThat(commitResponse.getTargetBranch()).isEqualTo(branch);
    assertThat(commitResponse.getAddedContents()).isNull();
  }

  @Test
  public void legacyCommitResponseReader() throws IOException {
    ReferenceMetadata meta = ImmutableReferenceMetadata.builder().numCommitsAhead(1).numCommitsBehind(2).build();
    Branch branch = Branch.builder().name("name").hash("1122334455667788").metadata(meta).build();

    // Legacy clients should be able to read responses from new commitMultipleOperations() implementations as `Branch`
    CommitResponse commitResponse = CommitResponse.builder()
      .targetBranch(branch)
      .build();
    ByteArrayOutputStream responsePayload = new ByteArrayOutputStream();
    toProto(commitResponse).writeTo(responsePayload);

    Branch branchResponse = fromProto(
      com.dremio.services.nessie.grpc.api.Branch.parseFrom(responsePayload.toByteArray()));
    assertThat(branchResponse).isEqualTo(branch);
  }

  @Test
  public void commitResponse() {
    assertThatThrownBy(() -> toProto((CommitResponse) null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("CommitResponse must be non-null");

    assertThatThrownBy(() -> fromProto((com.dremio.services.nessie.grpc.api.CommitResponse) null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("CommitResponse must be non-null");

    ReferenceMetadata meta = ImmutableReferenceMetadata.builder().numCommitsAhead(1).numCommitsBehind(2).build();
    Branch branch = Branch.builder().name("name").hash("1122334455667788").metadata(meta).build();

    CommitResponse commitResponse = CommitResponse.builder()
            .targetBranch(branch)
            .build();
    assertThat(fromProto(toProto(commitResponse)).getTargetBranch()).isEqualTo(branch);
    assertThat(fromProto(toProto(commitResponse)).getAddedContents()).isNull();

    ContentKey key1 = ContentKey.of("test1");
    ContentKey key2 = ContentKey.of("test3");
    commitResponse = CommitResponse.builder()
            .targetBranch(branch)
            .addAddedContents(AddedContent.addedContent(key1, "abc"))
            .addAddedContents(AddedContent.addedContent(key2, "def"))
            .build();
    assertThat(fromProto(toProto(commitResponse)).getTargetBranch()).isEqualTo(branch);
    assertThat(fromProto(toProto(commitResponse)).getAddedContents())
            .extracting(AddedContent::getKey, AddedContent::contentId)
            .containsExactly(tuple(key1, "abc"), tuple(key2, "def"));
  }
}
