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
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.DiffParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.GetReferenceParams;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutableRefLogResponseEntry;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
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
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;
import com.dremio.services.nessie.grpc.api.RefLogParams;
import com.dremio.services.nessie.grpc.api.RefLogResponse;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;

/**
 * Tests for {@link ProtoUtil}
 */
public class ProtoUtilTest {

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
    assertThat(fromProto(toProto(icebergTable))).isEqualTo(icebergTable);
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

    assertThatThrownBy(() -> toProto(null, EntriesParams.empty()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("refName must be non-null");

    assertThatThrownBy(() -> toProto("main", (EntriesParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("EntriesParams must be non-null");

    EntriesParams params =
      EntriesParams.builder()
        .filter("a > b")
        .hashOnRef("123")
        .maxRecords(23)
        .pageToken("abc")
        .build();
    assertThat(fromProto(toProto("main", params))).isEqualTo(params);

    EntriesParams empty = EntriesParams.empty();
    assertThat(fromProto(toProto("main", empty))).isEqualTo(empty);
  }

  @Test
  public void commitLogRequestConversion() {
    assertThatThrownBy(() -> fromProto((CommitLogRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("CommitLogRequest must be non-null");

    assertThatThrownBy(() -> toProto(null, CommitLogParams.empty()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("refName must be non-null");

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
        .fetch(FetchOption.ALL)
        .build();
    assertThat(fromProto(toProto("main", params))).isEqualTo(params);

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
    assertThatThrownBy(() -> toProto(key, null, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ref must be non-null");

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
  }

  @Test
  public void multipleContentsRequestConversion() {
    assertThatThrownBy(() -> toProto(null, null, (GetMultipleContentsRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("ref must be non-null");

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

    GetMultipleContentsResponse response = GetMultipleContentsResponse.of(Collections.singletonList(c));
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
      .fetch(FetchOption.ALL)
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

    params = GetReferenceParams.builder().refName("x").fetch(FetchOption.ALL).build();
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
    assertThatThrownBy(() -> fromProto((DiffRequest) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffRequest must be non-null");

    assertThatThrownBy(() -> toProto((DiffParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("DiffParams must be non-null");

    DiffParams params = DiffParams.builder().fromRef("x").toRef("y").build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = DiffParams.builder().fromRef("x").toRef("y")
      .fromHashOnRef("1234567890123456789012345678901234567890").build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = DiffParams.builder().fromRef("x").toRef("y")
      .toHashOnRef("1234567890123456789012345678901234567890").build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    params = DiffParams.builder().fromRef("x").toRef("y")
      .fromHashOnRef("1234567890123456789012345678901234567890")
      .toHashOnRef("aabbccddeeffaabbccddeeffaabbccddeeffaabb").build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);
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
  }

  @Test
  public void refLogParamsConversion() {
    assertThatThrownBy(() -> fromProto((RefLogParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogParams must be non-null");

    assertThatThrownBy(() -> toProto((org.projectnessie.api.params.RefLogParams) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("RefLogParams must be non-null");

    org.projectnessie.api.params.RefLogParams params =
      org.projectnessie.api.params.RefLogParams.builder()
        .startHash("foo")
        .endHash("bar")
        .maxRecords(23)
        .pageToken("abc")
        .build();
    assertThat(fromProto(toProto(params))).isEqualTo(params);

    org.projectnessie.api.params.RefLogParams empty = org.projectnessie.api.params.RefLogParams.empty();
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
}
