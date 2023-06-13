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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.CommitLogParamsBuilder;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.api.v1.params.EntriesParamsBuilder;
import org.projectnessie.api.v1.params.GetReferenceParams;
import org.projectnessie.api.v1.params.GetReferenceParamsBuilder;
import org.projectnessie.api.v1.params.ImmutableMerge;
import org.projectnessie.api.v1.params.ImmutableTransplant;
import org.projectnessie.api.v1.params.Merge;
import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.api.v1.params.MultipleNamespacesParamsBuilder;
import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.NamespaceParamsBuilder;
import org.projectnessie.api.v1.params.RefLogParams;
import org.projectnessie.api.v1.params.RefLogParamsBuilder;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.api.v1.params.ReferencesParamsBuilder;
import org.projectnessie.api.v1.params.Transplant;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableAddedContent;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableCommitResponse;
import org.projectnessie.model.ImmutableConflict;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDetached;
import org.projectnessie.model.ImmutableDiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableGetNamespacesResponse;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableMergeKeyBehavior;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableRefLogResponse;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.ImmutableUnchanged;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.model.Operations;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.RefLogResponse.RefLogResponseEntry;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;
import org.projectnessie.model.types.ContentTypes;

import com.dremio.services.nessie.grpc.api.CommitLogEntry;
import com.dremio.services.nessie.grpc.api.CommitLogRequest;
import com.dremio.services.nessie.grpc.api.CommitLogResponse;
import com.dremio.services.nessie.grpc.api.CommitOperation;
import com.dremio.services.nessie.grpc.api.CommitOps;
import com.dremio.services.nessie.grpc.api.Content;
import com.dremio.services.nessie.grpc.api.ContentKeyConflict;
import com.dremio.services.nessie.grpc.api.ContentKeyConflictDetails;
import com.dremio.services.nessie.grpc.api.ContentKeyDetails;
import com.dremio.services.nessie.grpc.api.ContentRequest;
import com.dremio.services.nessie.grpc.api.ContentType;
import com.dremio.services.nessie.grpc.api.DeltaLakeTable.Builder;
import com.dremio.services.nessie.grpc.api.DiffRequest;
import com.dremio.services.nessie.grpc.api.EntriesRequest;
import com.dremio.services.nessie.grpc.api.FetchOption;
import com.dremio.services.nessie.grpc.api.GetAllReferencesRequest;
import com.dremio.services.nessie.grpc.api.GetReferenceByNameRequest;
import com.dremio.services.nessie.grpc.api.MergeBehavior;
import com.dremio.services.nessie.grpc.api.MergeRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesRequest;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesResponse;
import com.dremio.services.nessie.grpc.api.NamespaceRequest;
import com.dremio.services.nessie.grpc.api.NessieConfiguration;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.TransplantRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;

/**
 * A simple utility class that translates between Protobuf classes and Nessie model classes.
 */
public final class ProtoUtil {

  private ProtoUtil() {
  }

  public static Reference refFromProto(com.dremio.services.nessie.grpc.api.Reference ref) {
    Preconditions.checkArgument(null != ref, "Reference must be non-null");
    if (ref.hasBranch()) {
      return fromProto(ref.getBranch());
    }
    if (ref.hasTag()) {
      return fromProto(ref.getTag());
    }
    if (ref.hasDetached()) {
      return fromProto(ref.getDetached());
    }
    throw new IllegalArgumentException(String.format("'%s' should be a Branch/Tag/Detached", ref));
  }

  public static com.dremio.services.nessie.grpc.api.Reference refToProto(Reference ref) {
    Preconditions.checkArgument(null != ref, "Reference must be non-null");
    if (ref instanceof Branch) {
      return com.dremio.services.nessie.grpc.api.Reference.newBuilder()
        .setBranch(toProto((Branch) ref))
        .build();
    }
    if (ref instanceof Tag) {
      return com.dremio.services.nessie.grpc.api.Reference.newBuilder().setTag(toProto((Tag) ref)).build();
    }
    if (ref instanceof Detached) {
      return com.dremio.services.nessie.grpc.api.Reference.newBuilder().setDetached(toProto((Detached) ref)).build();
    }
    throw new IllegalArgumentException(String.format("'%s' should be a Branch/Tag/Detached", ref));
  }

  public static com.dremio.services.nessie.grpc.api.Reference refToProto(
    com.dremio.services.nessie.grpc.api.ReferenceType type, String name, String hash) {
    Preconditions.checkArgument(null != name, "Reference name must be non-null");
    switch (type) {
      case BRANCH:
        return com.dremio.services.nessie.grpc.api.Reference.newBuilder()
          .setBranch(toProto(Branch.of(name, hash)))
          .build();

      case TAG:
        return com.dremio.services.nessie.grpc.api.Reference.newBuilder()
          .setTag(toProto(Tag.of(name, hash)))
          .build();

      default:
        throw new IllegalArgumentException(String.format("Reference type '%s' should be Branch or Tag", type));
    }
  }

  @Nullable
  public static Reference refFromProtoResponse(ReferenceResponse response) {
    Preconditions.checkArgument(null != response, "Reference response must be non-null");

    if (!response.hasReference()) {
      return null;
    }

    return refFromProto(response.getReference());
  }

  public static Branch fromProto(com.dremio.services.nessie.grpc.api.Branch branch) {
    Preconditions.checkArgument(null != branch, "Branch must be non-null");
    ImmutableBranch.Builder builder = ImmutableBranch.builder().name(branch.getName());

    if (branch.hasHash()) {
      builder.hash(branch.getHash());
    }
    if (branch.hasMetadata()) {
      builder.metadata(fromProto(branch.getMetadata()));
    }

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.Branch toProto(Branch branch) {
    Preconditions.checkArgument(null != branch, "Branch must be non-null");
    com.dremio.services.nessie.grpc.api.Branch.Builder builder =
      com.dremio.services.nessie.grpc.api.Branch.newBuilder().setName(branch.getName());
    if (null != branch.getHash()) {
      builder.setHash(branch.getHash());
    }
    if (null != branch.getMetadata()) {
      builder.setMetadata(toProto(branch.getMetadata()));
    }
    return builder.build();
  }

  public static CommitResponse fromProto(com.dremio.services.nessie.grpc.api.CommitResponse commitResponse) {
    Preconditions.checkArgument(null != commitResponse, "CommitResponse must be non-null");

    if (!commitResponse.hasBranch()) { // legacy response
      ImmutableBranch.Builder builder = ImmutableBranch.builder().name(commitResponse.getName());

      if (commitResponse.hasHash()) {
        builder.hash(commitResponse.getHash());
      }
      if (commitResponse.hasMetadata()) {
        builder.metadata(fromProto(commitResponse.getMetadata()));
      }

      return CommitResponse.builder().targetBranch(builder.build()).build();
    }

    ImmutableCommitResponse.Builder builder = CommitResponse.builder();
    builder.targetBranch(fromProto(commitResponse.getBranch()));

    commitResponse.getAddedContentList().forEach(ac ->
            builder.addAddedContents(ImmutableAddedContent.builder()
                    .key(fromProto(ac.getKey()))
                    .contentId(ac.getContentId())
                    .build()));

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.CommitResponse toProto(CommitResponse commitResponse) {
    Preconditions.checkArgument(null != commitResponse, "CommitResponse must be non-null");
    com.dremio.services.nessie.grpc.api.CommitResponse.Builder builder =
            com.dremio.services.nessie.grpc.api.CommitResponse.newBuilder();

    com.dremio.services.nessie.grpc.api.Branch branch = toProto(commitResponse.getTargetBranch());
    builder.setBranch(branch);

    if (commitResponse.getAddedContents() != null) {
      commitResponse.getAddedContents().forEach(ac ->
              builder.addAddedContentBuilder().setKey(toProto(ac.getKey())).setContentId(ac.contentId()));
    }

    // Allow older clients to read the response as `Branch` - remove these fields with DX-61406
    builder.setName(branch.getName());
    if (branch.hasHash()) {
      builder.setHash(branch.getHash());
    }
    if (branch.hasMetadata()) {
      builder.setMetadata(branch.getMetadata());
    }

    return builder.build();
  }

  public static Tag fromProto(com.dremio.services.nessie.grpc.api.Tag tag) {
    Preconditions.checkArgument(null != tag, "Tag must be non-null");
    ImmutableTag.Builder builder = ImmutableTag.builder().name(tag.getName());

    if (tag.hasHash()) {
      builder.hash(tag.getHash());
    }
    if (tag.hasMetadata()) {
      builder.metadata(fromProto(tag.getMetadata()));
    }

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.Tag toProto(Tag tag) {
    Preconditions.checkArgument(null != tag, "Tag must be non-null");
    com.dremio.services.nessie.grpc.api.Tag.Builder builder =
      com.dremio.services.nessie.grpc.api.Tag.newBuilder().setName(tag.getName());
    if (null != tag.getHash()) {
      builder.setHash(tag.getHash());
    }
    if (null != tag.getMetadata()) {
      builder.setMetadata(toProto(tag.getMetadata()));
    }
    return builder.build();
  }

  public static Detached fromProto(com.dremio.services.nessie.grpc.api.Detached detached) {
    Preconditions.checkArgument(null != detached, "Detached must be non-null");
    ImmutableDetached.Builder builder = ImmutableDetached.builder().hash(detached.getHash());

    if (detached.hasMetadata()) {
      builder.metadata(fromProto(detached.getMetadata()));
    }

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.Detached toProto(Detached detached) {
    Preconditions.checkArgument(null != detached, "Detached must be non-null");
    com.dremio.services.nessie.grpc.api.Detached.Builder builder =
      com.dremio.services.nessie.grpc.api.Detached.newBuilder().setHash(detached.getHash());
    if (null != detached.getMetadata()) {
      builder.setMetadata(toProto(detached.getMetadata()));
    }
    return builder.build();
  }

  public static ReferenceType fromProto(com.dremio.services.nessie.grpc.api.ReferenceType refType) {
    Preconditions.checkArgument(null != refType, "ReferenceType must be non-null");
    return ReferenceType.valueOf(refType.name());
  }

  public static com.dremio.services.nessie.grpc.api.ReferenceType toProto(ReferenceType refType) {
    Preconditions.checkArgument(null != refType, "ReferenceType must be non-null");
    return com.dremio.services.nessie.grpc.api.ReferenceType.valueOf(refType.name());
  }

  public static ReferenceMetadata fromProto(com.dremio.services.nessie.grpc.api.ReferenceMetadata metadata) {
    Preconditions.checkArgument(null != metadata, "ReferenceMetadata must be non-null");
    ImmutableReferenceMetadata.Builder builder = ImmutableReferenceMetadata.builder();
    if (metadata.hasNumCommitsAhead()) {
      builder.numCommitsAhead(metadata.getNumCommitsAhead());
    }
    if (metadata.hasNumCommitsBehind()) {
      builder.numCommitsBehind(metadata.getNumCommitsBehind());
    }
    if (metadata.hasCommitMetaOfHEAD()) {
      builder.commitMetaOfHEAD(fromProto(metadata.getCommitMetaOfHEAD()));
    }
    if (metadata.hasCommonAncestorHash()) {
      builder.commonAncestorHash(metadata.getCommonAncestorHash());
    }
    if (metadata.hasNumTotalCommits()) {
      builder.numTotalCommits(metadata.getNumTotalCommits());
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.ReferenceMetadata toProto(ReferenceMetadata metadata) {
    Preconditions.checkArgument(null != metadata, "ReferenceMetadata must be non-null");
    com.dremio.services.nessie.grpc.api.ReferenceMetadata.Builder builder = com.dremio.services.nessie.grpc.api.ReferenceMetadata.newBuilder();
    if (null != metadata.getNumCommitsAhead()) {
      builder.setNumCommitsAhead(metadata.getNumCommitsAhead());
    }
    if (null != metadata.getNumCommitsBehind()) {
      builder.setNumCommitsBehind(metadata.getNumCommitsBehind());
    }
    if (null != metadata.getCommitMetaOfHEAD()) {
      builder.setCommitMetaOfHEAD(toProto(metadata.getCommitMetaOfHEAD()));
    }
    if (null != metadata.getCommonAncestorHash()) {
      builder.setCommonAncestorHash(metadata.getCommonAncestorHash());
    }
    if (null != metadata.getNumTotalCommits()) {
      builder.setNumTotalCommits(metadata.getNumTotalCommits());
    }
    return builder.build();
  }

  public static Content toProto(org.projectnessie.model.Content obj) {
    Preconditions.checkArgument(null != obj, "Content must be non-null");
    if (obj instanceof IcebergTable) {
      IcebergTable iceberg = (IcebergTable) obj;
      return Content.newBuilder().setIceberg(toProto(iceberg)).build();
    }

    if (obj instanceof DeltaLakeTable) {
      return Content.newBuilder()
        .setDeltaLake(toProto((DeltaLakeTable) obj))
        .build();
    }

    if (obj instanceof IcebergView) {
      return Content.newBuilder().setIcebergView(toProto((IcebergView) obj)).build();
    }

    if (obj instanceof Namespace) {
      return Content.newBuilder().setNamespace(toProto((Namespace) obj)).build();
    }
    throw new IllegalArgumentException(
      String.format("'%s' must be an IcebergTable/DeltaLakeTable/IcebergView/Namespace", obj));
  }

  public static org.projectnessie.model.Content fromProto(Content obj) {
    Preconditions.checkArgument(null != obj, "Content must be non-null");
    if (obj.hasIceberg()) {
      return fromProto(obj.getIceberg());
    }
    if (obj.hasDeltaLake()) {
      return fromProto(obj.getDeltaLake());
    }
    if (obj.hasIcebergView()) {
      return fromProto(obj.getIcebergView());
    }
    if (obj.hasNamespace()) {
      return fromProto(obj.getNamespace());
    }
    throw new IllegalArgumentException(
      String.format("'%s' must be an IcebergTable/DeltaLakeTable/IcebergView/Namespace", obj));
  }

  private static String asId(String idFromProto) {
    if (idFromProto != null && idFromProto.isEmpty()) {
      return null;
    }

    return idFromProto;
  }

  public static DeltaLakeTable fromProto(com.dremio.services.nessie.grpc.api.DeltaLakeTable deltaLakeTable) {
    Preconditions.checkArgument(null != deltaLakeTable, "DeltaLakeTable must be non-null");
    ImmutableDeltaLakeTable.Builder builder =
      ImmutableDeltaLakeTable.builder()
        .id(asId(deltaLakeTable.getId()))
        .checkpointLocationHistory(deltaLakeTable.getCheckpointLocationHistoryList())
        .metadataLocationHistory(deltaLakeTable.getMetadataLocationHistoryList());
    if (deltaLakeTable.hasLastCheckpoint()) {
      builder.lastCheckpoint(deltaLakeTable.getLastCheckpoint());
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.DeltaLakeTable toProto(DeltaLakeTable deltaLakeTable) {
    Preconditions.checkArgument(null != deltaLakeTable, "DeltaLakeTable must be non-null");
    Builder builder = com.dremio.services.nessie.grpc.api.DeltaLakeTable.newBuilder();

    // the ID is optional when a new table is created - will be assigned on the server side
    if (null != deltaLakeTable.getId()) {
      builder.setId(deltaLakeTable.getId());
    }

    if (null != deltaLakeTable.getLastCheckpoint()) {
      builder.setLastCheckpoint(deltaLakeTable.getLastCheckpoint());
    }
    deltaLakeTable.getCheckpointLocationHistory().forEach(builder::addCheckpointLocationHistory);
    deltaLakeTable.getMetadataLocationHistory().forEach(builder::addMetadataLocationHistory);
    return builder.build();
  }

  public static IcebergTable fromProto(com.dremio.services.nessie.grpc.api.IcebergTable icebergTable) {
    Preconditions.checkArgument(null != icebergTable, "IcebergTable must be non-null");
    ImmutableIcebergTable.Builder builder = ImmutableIcebergTable.builder()
      .id(asId(icebergTable.getId()))
      .metadataLocation(icebergTable.getMetadataLocation())
      .snapshotId(icebergTable.getSnapshotId())
      .schemaId(icebergTable.getSchemaId())
      .specId(icebergTable.getSpecId())
      .sortOrderId(icebergTable.getSortOrderId());

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.IcebergTable toProto(IcebergTable icebergTable) {
    Preconditions.checkArgument(null != icebergTable, "IcebergTable must be non-null");
    com.dremio.services.nessie.grpc.api.IcebergTable.Builder builder =
      com.dremio.services.nessie.grpc.api.IcebergTable.newBuilder()
      .setMetadataLocation(icebergTable.getMetadataLocation())
      .setSnapshotId(icebergTable.getSnapshotId())
      .setSchemaId(icebergTable.getSchemaId())
      .setSpecId(icebergTable.getSpecId())
      .setSortOrderId(icebergTable.getSortOrderId());
    // the ID is optional when a new table is created - will be assigned on the server side
    if (null != icebergTable.getId()) {
      builder.setId(icebergTable.getId());
    }

    return builder.build();
  }

  public static IcebergView fromProto(com.dremio.services.nessie.grpc.api.IcebergView view) {
    Preconditions.checkArgument(null != view, "IcebergView must be non-null");
    ImmutableIcebergView.Builder builder = ImmutableIcebergView.builder()
      .id(asId(view.getId()))
      .metadataLocation(view.getMetadataLocation())
      .versionId(view.getVersionId())
      .schemaId(view.getSchemaId())
      .dialect(view.getDialect())
      .sqlText(view.getSqlText());

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.IcebergView toProto(IcebergView view) {
    Preconditions.checkArgument(null != view, "IcebergView must be non-null");
    com.dremio.services.nessie.grpc.api.IcebergView.Builder builder =
      com.dremio.services.nessie.grpc.api.IcebergView.newBuilder()
      .setMetadataLocation(view.getMetadataLocation())
      .setVersionId(view.getVersionId())
      .setSchemaId(view.getSchemaId())
      .setDialect(view.getDialect())
      .setSqlText(view.getSqlText());
    // the ID is optional when a new table is created - will be assigned on the server side
    if (null != view.getId()) {
      builder.setId(view.getId());
    }

    return builder.build();
  }

  public static NessieConfiguration toProto(org.projectnessie.model.NessieConfiguration config) {
    Preconditions.checkArgument(null != config, "NessieConfiguration must be non-null");
    NessieConfiguration.Builder builder = NessieConfiguration.newBuilder()
      .setMaxSupportedApiVersion(config.getMaxSupportedApiVersion())
      .setMinSupportedApiVersion(config.getMinSupportedApiVersion())
      .setActualApiVersion(config.getActualApiVersion());
    if (null != config.getDefaultBranch()) {
      builder.setDefaultBranch(config.getDefaultBranch());
    }
    if (null != config.getSpecVersion()) {
      builder.setSpecVersion(config.getSpecVersion());
    }
    if (null != config.getNoAncestorHash()) {
      builder.setNoAncestorHash(config.getNoAncestorHash());
    }
    if (null != config.getRepositoryCreationTimestamp()) {
      builder.setRepositoryCreationTimestamp(toProto(config.getRepositoryCreationTimestamp()));
    }
    if (null != config.getOldestPossibleCommitTimestamp()) {
      builder.setOldestPossibleCommitTimestamp(toProto(config.getOldestPossibleCommitTimestamp()));
    }
    builder.putAllAdditionalProperties(config.getAdditionalProperties());
    return builder.build();
  }

  public static org.projectnessie.model.NessieConfiguration fromProto(NessieConfiguration config) {
    Preconditions.checkArgument(null != config, "NessieConfiguration must be non-null");
    ImmutableNessieConfiguration.Builder builder = ImmutableNessieConfiguration.builder()
      .maxSupportedApiVersion(config.getMaxSupportedApiVersion());
    if (config.hasMinSupportedApiVersion()) {
      builder.minSupportedApiVersion(config.getMinSupportedApiVersion());
    }
    if (config.hasSpecVersion()) {
      builder.specVersion(config.getSpecVersion());
    }
    if (config.hasDefaultBranch()) {
      builder.defaultBranch(config.getDefaultBranch());
    }
    if (config.hasNoAncestorHash()) {
      builder.noAncestorHash(config.getNoAncestorHash());
    }
    if (config.hasRepositoryCreationTimestamp()) {
      builder.repositoryCreationTimestamp(fromProto(config.getRepositoryCreationTimestamp()));
    }
    if (config.hasOldestPossibleCommitTimestamp()) {
      builder.oldestPossibleCommitTimestamp(fromProto(config.getOldestPossibleCommitTimestamp()));
    }
    if (config.hasActualApiVersion()) {
      builder.actualApiVersion(config.getActualApiVersion());
    }
    builder.additionalProperties(config.getAdditionalPropertiesMap());
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.ContentKey toProto(ContentKey key) {
    Preconditions.checkArgument(null != key, "ContentKey must be non-null");
    return com.dremio.services.nessie.grpc.api.ContentKey.newBuilder()
      .addAllElements(key.getElements())
      .build();
  }

  public static ContentKey fromProto(com.dremio.services.nessie.grpc.api.ContentKey key) {
    Preconditions.checkArgument(null != key, "ContentKey must be non-null");
    return ContentKey.of(key.getElementsList());
  }

  public static List<ContentKey> fromProto(List<com.dremio.services.nessie.grpc.api.ContentKey> keys) {
    Preconditions.checkArgument(null != keys, "ContentKey list must be non-null");
    ImmutableList.Builder<ContentKey> list = ImmutableList.builder();
    keys.forEach(k -> list.add(fromProto(k)));
    return list.build();
  }

  public static ContentWithKey fromProto(com.dremio.services.nessie.grpc.api.ContentWithKey c) {
    Preconditions.checkArgument(null != c, "ContentWithKey must be non-null");
    return ContentWithKey.of(fromProto(c.getContentKey()), fromProto(c.getContent()));
  }

  public static com.dremio.services.nessie.grpc.api.ContentWithKey toProto(ContentWithKey c) {
    Preconditions.checkArgument(null != c, "ContentWithKey must be non-null");
    return com.dremio.services.nessie.grpc.api.ContentWithKey.newBuilder()
      .setContentKey(toProto(c.getKey()))
      .setContent(toProto(c.getContent()))
      .build();
  }

  public static com.dremio.services.nessie.grpc.api.Entry toProto(Entry entry) {
    Preconditions.checkArgument(null != entry, "Entry must be non-null");
    com.dremio.services.nessie.grpc.api.Entry.Builder builder = com.dremio.services.nessie.grpc.api.Entry.newBuilder()
      .setContentKey(toProto(entry.getName()))
      .setType(ContentType.valueOf(entry.getType().name()));
    if (null != entry.getContentId()) {
      builder.setContentId(entry.getContentId());
    }
    if (null != entry.getContent()) {
      builder.setContent(toProto(entry.getContent()));
    }
    return builder.build();
  }

  public static Entry fromProto(com.dremio.services.nessie.grpc.api.Entry entry) {
    Preconditions.checkArgument(null != entry, "Entry must be non-null");
    ImmutableEntry.Builder builder = Entry.builder()
      .type(ContentTypes.forName(entry.getType().name()))
      .name(fromProto(entry.getContentKey()));
    if (entry.hasContentId()) {
      builder.contentId(entry.getContentId());
    }
    if (entry.hasContent()) {
      builder.content(fromProto(entry.getContent()));
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.CommitMeta toProto(CommitMeta commitMeta) {
    Preconditions.checkArgument(null != commitMeta, "CommitMeta must be non-null");
    // note that if the committer/author/commitmsg is an empty string, then it won't be included in
    // the GRPC commit meta
    com.dremio.services.nessie.grpc.api.CommitMeta.Builder builder =
      com.dremio.services.nessie.grpc.api.CommitMeta.newBuilder();
    if (null != commitMeta.getHash()) {
      builder.setHash(commitMeta.getHash());
    }
    if (null != commitMeta.getSignedOffBy()) {
      builder.setSignedOffBy(commitMeta.getSignedOffBy());
    }
    if (null != commitMeta.getAuthor()) {
      builder.setAuthor(commitMeta.getAuthor());
    }
    if (null != commitMeta.getAuthorTime()) {
      builder.setAuthorTime(toProto(commitMeta.getAuthorTime()));
    }
    if (null != commitMeta.getCommitter()) {
      builder.setCommitter(commitMeta.getCommitter());
    }
    if (null != commitMeta.getCommitTime()) {
      builder.setCommitTime(toProto(commitMeta.getCommitTime()));
    }
    return builder
      .addAllParentHashes(commitMeta.getParentCommitHashes())
      .setMessage(commitMeta.getMessage())
      .putAllProperties(commitMeta.getProperties())
      .build();
  }

  public static CommitMeta fromProto(com.dremio.services.nessie.grpc.api.CommitMeta commitMeta) {
    Preconditions.checkArgument(null != commitMeta, "CommitMeta must be non-null");
    // we can't set the committer here as it's set on the server-side and there's a check that
    // prevents this field from being set
    ImmutableCommitMeta.Builder builder = CommitMeta.builder();
    if (commitMeta.hasHash()) {
      builder.hash(commitMeta.getHash());
    }
    if (commitMeta.hasSignedOffBy()) {
      builder.signedOffBy(commitMeta.getSignedOffBy());
    }
    if (commitMeta.hasAuthor()) {
      builder.author(commitMeta.getAuthor());
    }
    if (commitMeta.hasAuthorTime()) {
      builder.authorTime(fromProto(commitMeta.getAuthorTime()));
    }
    if (commitMeta.hasCommitter()) {
      builder.committer(commitMeta.getCommitter());
    }
    if (commitMeta.hasCommitTime()) {
      builder.commitTime(fromProto(commitMeta.getCommitTime()));
    }
    return builder
      .addAllParentCommitHashes(commitMeta.getParentHashesList())
      .message(commitMeta.getMessage())
      .properties(commitMeta.getPropertiesMap())
      .build();
  }

  public static Timestamp toProto(Instant timestamp) {
    Preconditions.checkArgument(null != timestamp, "Timestamp must be non-null");
    return Timestamp.newBuilder()
      .setSeconds(timestamp.getEpochSecond())
      .setNanos(timestamp.getNano())
      .build();
  }

  public static Instant fromProto(Timestamp timestamp) {
    Preconditions.checkArgument(null != timestamp, "Timestamp must be non-null");
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static CommitOperation toProto(Operation op) {
    Preconditions.checkArgument(null != op, "CommitOperation must be non-null");
    if (op instanceof Put) {
      com.dremio.services.nessie.grpc.api.Put.Builder builder = com.dremio.services.nessie.grpc.api.Put.newBuilder()
        .setKey(toProto(op.getKey()))
        .setContent(toProto(((Put) op).getContent()));
      if (null != ((Put) op).getExpectedContent()) {
        builder.setExpectedContent(toProto(((Put) op).getExpectedContent()));
      }
      return CommitOperation.newBuilder()
        .setPut(builder.build())
        .build();
    }

    if (op instanceof Delete) {
      return CommitOperation.newBuilder()
        .setDelete(
          com.dremio.services.nessie.grpc.api.Delete.newBuilder().setKey(toProto(op.getKey())).build())
        .build();
    }

    if (op instanceof Unchanged) {
      return CommitOperation.newBuilder()
        .setUnchanged(
          com.dremio.services.nessie.grpc.api.Unchanged.newBuilder()
            .setKey(toProto(op.getKey()))
            .build())
        .build();
    }
    throw new IllegalArgumentException("CommitOperation should be Put/Delete/Unchanged");
  }

  public static Operation fromProto(CommitOperation op) {
    Preconditions.checkArgument(null != op, "CommitOperation must be non-null");
    if (op.hasPut()) {
      ImmutablePut.Builder builder = ImmutablePut.builder()
        .key(fromProto(op.getPut().getKey()))
        .content(fromProto(op.getPut().getContent()));
      if (op.getPut().hasExpectedContent()) {
        builder.expectedContent(fromProto(op.getPut().getExpectedContent()));
      }
      return builder.build();
    }

    if (op.hasDelete()) {
      return ImmutableDelete.builder().key(fromProto(op.getDelete().getKey())).build();
    }

    if (op.hasUnchanged()) {
      return ImmutableUnchanged.builder().key(fromProto(op.getUnchanged().getKey())).build();
    }
    throw new IllegalArgumentException("CommitOperation should be Put/Delete/Unchanged");
  }

  public static Operations fromProto(CommitOps ops) {
    Preconditions.checkArgument(null != ops, "CommitOperations must be non-null");
    return ImmutableOperations.builder()
      .commitMeta(fromProto(ops.getCommitMeta()))
      .operations(
        ops.getOperationsList().stream().map(ProtoUtil::fromProto).collect(Collectors.toList()))
      .build();
  }

  public static CommitOps toProto(Operations operations) {
    Preconditions.checkArgument(null != operations, "CommitOperations must be non-null");
    CommitOps.Builder builder =
      CommitOps.newBuilder().setCommitMeta(toProto(operations.getCommitMeta()));
    operations.getOperations().stream().map(ProtoUtil::toProto).forEach(builder::addOperations);
    return builder.build();
  }

  public static EntriesParams fromProto(EntriesRequest request) {
    Preconditions.checkArgument(null != request, "EntriesRequest must be non-null");
    EntriesParamsBuilder builder = EntriesParams.builder();
    if (request.hasHashOnRef()) {
      builder.hashOnRef(request.getHashOnRef());
    }
    if (request.hasFilter()) {
      builder.filter(request.getFilter());
    }
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    if (request.hasMaxRecords()) {
      builder.maxRecords(request.getMaxRecords());
    }
    if (request.hasNamespaceDepth()) {
      builder.namespaceDepth(request.getNamespaceDepth());
    }
    return builder.build();
  }

  public static EntriesRequest toProtoEntriesRequest(@Nullable String refName,
                                                     @Nullable String hashOnRef,
                                                     @Nullable Integer maxRecords,
                                                     @Nullable String filter,
                                                     @Nullable Integer namespaceDepth,
                                                     boolean withContent,
                                                     @Nullable ContentKey minKey,
                                                     @Nullable ContentKey maxKey,
                                                     @Nullable ContentKey prefixKey,
                                                     List<ContentKey> keys) {
    refName = refNameOrDetached(refName);
    EntriesRequest.Builder builder = EntriesRequest.newBuilder().setNamedRef(refName);
    builder.setWithContent(withContent);
    if (null != hashOnRef) {
      builder.setHashOnRef(hashOnRef);
    }
    if (null != maxRecords) {
      builder.setMaxRecords(maxRecords);
    }
    if (null != filter) {
      builder.setFilter(filter);
    }
    if (null != namespaceDepth) {
      builder.setNamespaceDepth(namespaceDepth);
    }
    if (null != minKey) {
      builder.setMinKey(toProto(minKey));
    }
    if (null != maxKey) {
      builder.setMaxKey(toProto(maxKey));
    }
    if (null != prefixKey) {
      builder.setPrefixKey(toProto(prefixKey));
    }
    if (null != keys) {
      keys.forEach(k -> builder.addKeys(toProto(k)));
    }
    return builder.build();
  }

  public static CommitLogParams fromProto(CommitLogRequest request) {
    Preconditions.checkArgument(null != request, "CommitLogRequest must be non-null");
    CommitLogParamsBuilder builder = CommitLogParams.builder();
    if (request.hasStartHash()) {
      builder.startHash(request.getStartHash());
    }
    if (request.hasEndHash()) {
      builder.endHash(request.getEndHash());
    }
    if (request.hasFilter()) {
      builder.filter(request.getFilter());
    }
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    if (request.hasMaxRecords()) {
      builder.maxRecords(request.getMaxRecords());
    }
    if (request.hasFetchOption()) {
      builder.fetchOption(org.projectnessie.model.FetchOption.valueOf(request.getFetchOption().name()));
    }
    return builder.build();
  }

  private static String refNameOrDetached(@Nullable String name) {
    return name == null ? Detached.REF_NAME : name;
  }

  public static CommitLogRequest toProto(@Nullable String refName, CommitLogParams params) {
    refName = refNameOrDetached(refName);
    Preconditions.checkArgument(null != params, "CommitLogParams must be non-null");
    CommitLogRequest.Builder builder = CommitLogRequest.newBuilder().setNamedRef(refName);
    if (null != params.startHash()) {
      builder.setStartHash(params.startHash());
    }
    if (null != params.endHash()) {
      builder.setEndHash(params.endHash());
    }
    if (null != params.maxRecords()) {
      builder.setMaxRecords(params.maxRecords());
    }
    if (null != params.pageToken()) {
      builder.setPageToken(params.pageToken());
    }
    if (null != params.filter()) {
      builder.setFilter(params.filter());
    }
    if (null != params.fetchOption()) {
      builder.setFetchOption(FetchOption.valueOf(params.fetchOption().name()));
    }
    return builder.build();
  }

  public static LogResponse fromProto(CommitLogResponse commitLog) {
    Preconditions.checkArgument(null != commitLog, "CommitLogResponse must be non-null");
    ImmutableLogResponse.Builder builder =
      ImmutableLogResponse.builder()
        .addAllLogEntries(
          commitLog.getLogEntriesList().stream()
            .map(ProtoUtil::fromProto)
            .collect(Collectors.toList()))
        .isHasMore(commitLog.getHasMore());
    if (commitLog.hasToken()) {
      builder.token(commitLog.getToken());
    }
    return builder.build();
  }

  public static CommitLogResponse toProto(LogResponse commitLog) {
    Preconditions.checkArgument(null != commitLog, "CommitLogResponse must be non-null");
    CommitLogResponse.Builder builder =
      CommitLogResponse.newBuilder().setHasMore(commitLog.isHasMore());
    builder.addAllLogEntries(
      commitLog.getLogEntries().stream().map(ProtoUtil::toProto).collect(Collectors.toList()));
    if (null != commitLog.getToken()) {
      builder.setToken(commitLog.getToken());
    }
    return builder.build();
  }

  public static CommitLogEntry toProto(LogEntry entry) {
    Preconditions.checkArgument(null != entry, "LogEntry must be non-null");
    CommitLogEntry.Builder builder = CommitLogEntry.newBuilder();
    builder.setCommitMeta(toProto(entry.getCommitMeta()));
    if (null != entry.getParentCommitHash()) {
      builder.setParentCommitHash(entry.getParentCommitHash());
    }
    if (null != entry.getOperations()) {
      builder.addAllOperations(entry.getOperations().stream().map(ProtoUtil::toProto).collect(
        Collectors.toList()));
    }
    return builder.build();
  }

  public static LogEntry fromProto(CommitLogEntry entry) {
    Preconditions.checkArgument(null != entry, "CommitLogEntry must be non-null");
    ImmutableLogEntry.Builder builder = LogEntry.builder()
      .commitMeta(fromProto(entry.getCommitMeta()));
    if (entry.hasParentCommitHash()) {
      builder.parentCommitHash(entry.getParentCommitHash());
    }
    if (entry.getOperationsCount() > 0) {
      builder.addAllOperations(entry.getOperationsList().stream().map(ProtoUtil::fromProto).collect(
        Collectors.toList()));
    }
    return builder.build();
  }

  public static EntriesResponse fromProto(com.dremio.services.nessie.grpc.api.EntriesResponse entries) {
    Preconditions.checkArgument(null != entries, "EntriesResponse must be non-null");
    ImmutableEntriesResponse.Builder builder =
      EntriesResponse.builder()
        .entries(
          entries.getEntriesList().stream()
            .map(ProtoUtil::fromProto)
            .collect(Collectors.toList()))
        .isHasMore(entries.getHasMore());
    if (entries.hasToken()) {
      builder.token(entries.getToken());
    }
    if (entries.hasEffectiveReference()) {
      builder.effectiveReference(refFromProto(entries.getEffectiveReference()));
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.EntriesResponse toProto(
    EntriesResponse entries) {
    Preconditions.checkArgument(null != entries, "EntriesResponse must be non-null");
    com.dremio.services.nessie.grpc.api.EntriesResponse.Builder builder =
      com.dremio.services.nessie.grpc.api.EntriesResponse.newBuilder().setHasMore(entries.isHasMore());
    entries.getEntries().forEach(e -> builder.addEntries(toProto(e)));
    if (null != entries.getToken()) {
      builder.setToken(entries.getToken());
    }
    if (null != entries.getEffectiveReference()) {
      builder.setEffectiveReference(refToProto(entries.getEffectiveReference()));
    }
    return builder.build();
  }

  public static ContentRequest toProto(ContentKey key, @Nullable String ref, String hashOnRef) {
    ref = refNameOrDetached(ref);
    ContentRequest.Builder builder =
      ContentRequest.newBuilder().setContentKey(toProto(key)).setRef(ref);
    builder = null != hashOnRef ? builder.setHashOnRef(hashOnRef) : builder;
    return builder.build();
  }

  public static MultipleContentsRequest toProto(
    @Nullable String ref, String hashOnRef, GetMultipleContentsRequest request) {
    final MultipleContentsRequest.Builder builder =
      MultipleContentsRequest.newBuilder();
    if (null != ref) {
      builder.setRef(ref);
    }
    if (null != hashOnRef) {
      builder.setHashOnRef(hashOnRef);
    }
    if (null != request) {
      request.getRequestedKeys().forEach(k -> builder.addRequestedKeys(toProto(k)));
    }

    return builder.build();
  }

  public static MultipleContentsResponse toProto(GetMultipleContentsResponse response) {
    Preconditions.checkArgument(null != response, "GetMultipleContentsResponse must be non-null");
    MultipleContentsResponse.Builder builder = MultipleContentsResponse.newBuilder();
    response.getContents().forEach(c -> builder.addContentWithKey(toProto(c)));
    if (response.getEffectiveReference() != null) {
      builder.setEffectiveReference(refToProto(response.getEffectiveReference()));
    }
    return builder.build();
  }

  public static GetMultipleContentsResponse fromProto(MultipleContentsResponse response) {
    Preconditions.checkArgument(null != response, "MultipleContentsResponse must be non-null");
    Reference effectiveRef = null;
    if (response.hasEffectiveReference()) {
      effectiveRef = refFromProto(response.getEffectiveReference());
    }
    return GetMultipleContentsResponse.of(
      response.getContentWithKeyList().stream()
        .map(ProtoUtil::fromProto)
        .collect(Collectors.toList()),
      effectiveRef);
  }

  public static ReferencesParams fromProto(GetAllReferencesRequest request) {
    Preconditions.checkArgument(null != request, "GetAllReferencesRequest must be non-null");
    ReferencesParamsBuilder builder = ReferencesParams.builder();
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    if (request.hasMaxRecords()) {
      builder.maxRecords(request.getMaxRecords());
    }
    if (request.hasFilter()) {
      builder.filter(request.getFilter());
    }
    if (request.hasFetchOption()) {
      builder.fetchOption(org.projectnessie.model.FetchOption.valueOf(request.getFetchOption().name()));
    }
    return builder.build();
  }

  public static GetAllReferencesRequest toProto(ReferencesParams params) {
    Preconditions.checkArgument(null != params, "ReferencesParams must be non-null");
    GetAllReferencesRequest.Builder builder = GetAllReferencesRequest.newBuilder();
    if (null != params.pageToken()) {
      builder.setPageToken(params.pageToken());
    }
    if (null != params.maxRecords()) {
      builder.setMaxRecords(params.maxRecords());
    }
    if (null != params.filter()) {
      builder.setFilter(params.filter());
    }
    if (null != params.fetchOption()) {
      builder.setFetchOption(FetchOption.valueOf(params.fetchOption().name()));
    }
    return builder.build();
  }

  public static GetReferenceParams fromProto(GetReferenceByNameRequest request) {
    Preconditions.checkArgument(null != request, "GetReferenceByNameRequest must be non-null");
    GetReferenceParamsBuilder builder = GetReferenceParams.builder()
      .refName(request.getNamedRef());
    if (request.hasFetchOption()) {
      builder.fetchOption(org.projectnessie.model.FetchOption.valueOf(request.getFetchOption().name()));
    }
    return builder.build();
  }

  public static GetReferenceByNameRequest toProto(GetReferenceParams params) {
    Preconditions.checkArgument(null != params, "GetReferenceParams must be non-null");
    GetReferenceByNameRequest.Builder builder = GetReferenceByNameRequest.newBuilder()
      .setNamedRef(params.getRefName());
    if (null != params.fetchOption()) {
      builder.setFetchOption(FetchOption.valueOf(params.fetchOption().name()));
    }
    return builder.build();
  }

  public static DiffRequest toProtoDiffRequest(@Nullable String fromRef, String fromHashOnRef, String toRef,
                                               String toHashOnRef, Integer maxRecords, ContentKey minKey,
                                               ContentKey maxKey, ContentKey prefixKey, List<ContentKey> keys,
                                               String filter) {
    fromRef = refNameOrDetached(fromRef);
    toRef = refNameOrDetached(toRef);
    DiffRequest.Builder builder = DiffRequest.newBuilder()
      .setFromRefName(fromRef)
      .setToRefName(toRef);
    if (null != fromHashOnRef) {
      builder.setFromHashOnRef(fromHashOnRef);
    }
    if (null != toHashOnRef) {
      builder.setToHashOnRef(toHashOnRef);
    }
    if (null != maxRecords) {
      builder.setMaxRecords(maxRecords);
    }
    if (null != minKey) {
      builder.setMinKey(toProto(minKey));
    }
    if (null != maxKey) {
      builder.setMaxKey(toProto(maxKey));
    }
    if (null != prefixKey) {
      builder.setPrefixKey(toProto(prefixKey));
    }
    if (null != filter) {
      builder.setFilter(filter);
    }
    if (null != keys) {
      keys.forEach(k -> builder.addKeys(toProto(k)));
    }
    return builder.build();
  }

  public static DiffResponse fromProto(com.dremio.services.nessie.grpc.api.DiffResponse response) {
    Preconditions.checkArgument(null != response, "DiffResponse must be non-null");
    ImmutableDiffResponse.Builder builder = ImmutableDiffResponse.builder()
      .addAllDiffs(response.getDiffsList().stream()
        .map(ProtoUtil::fromProto)
        .collect(Collectors.toList()))
      .isHasMore(response.getHasMore());
    if (response.hasEffectiveFromRef()) {
      builder.effectiveFromReference(refFromProto(response.getEffectiveFromRef()));
    }
    if (response.hasEffectiveToRef()) {
      builder.effectiveToReference(refFromProto(response.getEffectiveToRef()));
    }
    if (response.hasPageToken()) {
      builder.token(response.getPageToken());
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.DiffResponse toProto(DiffResponse response) {
    Preconditions.checkArgument(null != response, "DiffResponse must be non-null");
    com.dremio.services.nessie.grpc.api.DiffResponse.Builder builder =
      com.dremio.services.nessie.grpc.api.DiffResponse.newBuilder()
        .addAllDiffs(response.getDiffs().stream()
          .map(ProtoUtil::toProto)
          .collect(Collectors.toList()))
        .setHasMore(response.isHasMore());
    if (null != response.getEffectiveFromReference()) {
      builder.setEffectiveFromRef(refToProto(response.getEffectiveFromReference()));
    }
    if (null != response.getEffectiveToReference()) {
      builder.setEffectiveToRef(refToProto(response.getEffectiveToReference()));
    }
    if (null != response.getToken()) {
      builder.setPageToken(response.getToken());
    }
    return builder.build();
  }

  public static DiffEntry fromProto(com.dremio.services.nessie.grpc.api.DiffEntry diffEntry) {
    Preconditions.checkArgument(null != diffEntry, "DiffEntry must be non-null");
    ImmutableDiffEntry.Builder builder = ImmutableDiffEntry.builder()
      .key(fromProto(diffEntry.getKey()));
    if (diffEntry.hasFrom()) {
      builder.from(fromProto(diffEntry.getFrom()));
    }
    if (diffEntry.hasTo()) {
      builder.to(fromProto(diffEntry.getTo()));
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.DiffEntry toProto(DiffEntry diffEntry) {
    Preconditions.checkArgument(null != diffEntry, "DiffEntry must be non-null");
    com.dremio.services.nessie.grpc.api.DiffEntry.Builder builder = com.dremio.services.nessie.grpc.api.DiffEntry.newBuilder()
      .setKey(toProto(diffEntry.getKey()));
    if (null != diffEntry.getFrom()) {
      builder.setFrom(toProto(diffEntry.getFrom()));
    }
    if (null != diffEntry.getTo()) {
      builder.setTo(toProto(diffEntry.getTo()));
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.RefLogParams toProto(RefLogParams params) {
    Preconditions.checkArgument(null != params, "RefLogParams must be non-null");
    com.dremio.services.nessie.grpc.api.RefLogParams.Builder builder = com.dremio.services.nessie.grpc.api.RefLogParams.newBuilder();
    if (null != params.startHash()) {
      builder.setStartHash(params.startHash());
    }
    if (null != params.endHash()) {
      builder.setEndHash(params.endHash());
    }
    if (null != params.maxRecords()) {
      builder.setMaxRecords(params.maxRecords());
    }
    if (null != params.pageToken()) {
      builder.setPageToken(params.pageToken());
    }
    if (null != params.filter()) {
      builder.setFilter(params.filter());
    }
    return builder.build();
  }

  public static RefLogParams fromProto(com.dremio.services.nessie.grpc.api.RefLogParams request) {
    Preconditions.checkArgument(null != request, "RefLogParams must be non-null");
    RefLogParamsBuilder builder = RefLogParams.builder();
    if (request.hasStartHash()) {
      builder.startHash(request.getStartHash());
    }
    if (request.hasEndHash()) {
      builder.endHash(request.getEndHash());
    }
    if (request.hasPageToken()) {
      builder.pageToken(request.getPageToken());
    }
    if (request.hasMaxRecords()) {
      builder.maxRecords(request.getMaxRecords());
    }
    if (request.hasFilter()) {
      builder.filter(request.getFilter());
    }
    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.RefLogResponse toProto(RefLogResponse response) {
    Preconditions.checkArgument(null != response, "RefLogResponse must be non-null");
    com.dremio.services.nessie.grpc.api.RefLogResponse.Builder b = com.dremio.services.nessie.grpc.api.RefLogResponse.newBuilder()
      .setHasMore(response.isHasMore());
    if (response.getToken() != null) {
      b.setToken(response.getToken());
    }
    response.getLogEntries().stream().map(ProtoUtil::toProto).forEach(b::addLogEntries);
    return b.build();
  }

  public static RefLogResponse fromProto(com.dremio.services.nessie.grpc.api.RefLogResponse response) {
    Preconditions.checkArgument(null != response, "RefLogResponse must be non-null");
    ImmutableRefLogResponse.Builder b = ImmutableRefLogResponse.builder()
      .isHasMore(response.getHasMore())
      .token(response.getToken());
    response.getLogEntriesList().stream().map(ProtoUtil::fromProto).forEach(b::addLogEntries);
    return b.build();
  }

  public static com.dremio.services.nessie.grpc.api.RefLogResponseEntry toProto(RefLogResponseEntry entry) {
    Preconditions.checkArgument(null != entry, "RefLogResponseEntry must be non-null");
    return com.dremio.services.nessie.grpc.api.RefLogResponseEntry.newBuilder()
      .setRefLogId(entry.getRefLogId())
      .setRefName(entry.getRefName())
      .setRefType(entry.getRefType())
      .setCommitHash(entry.getCommitHash())
      .setParentRefLogId(entry.getParentRefLogId())
      .setOperationTime(entry.getOperationTime())
      .setOperation(entry.getOperation())
      .addAllSourceHashes(entry.getSourceHashes())
      .build();
  }

  public static RefLogResponseEntry fromProto(com.dremio.services.nessie.grpc.api.RefLogResponseEntry entry) {
    Preconditions.checkArgument(null != entry, "RefLogResponseEntry must be non-null");
    return RefLogResponseEntry.builder()
      .refLogId(entry.getRefLogId())
      .refName(entry.getRefName())
      .refType(entry.getRefType())
      .commitHash(entry.getCommitHash())
      .parentRefLogId(entry.getParentRefLogId())
      .operationTime(entry.getOperationTime())
      .operation(entry.getOperation())
      .sourceHashes(entry.getSourceHashesList())
      .build();
  }

  public static com.dremio.services.nessie.grpc.api.Namespace toProto(Namespace namespace) {
    Preconditions.checkArgument(null != namespace, "Namespace must be non-null");
    com.dremio.services.nessie.grpc.api.Namespace.Builder builder =
      com.dremio.services.nessie.grpc.api.Namespace.newBuilder()
        .addAllElements(namespace.getElements())
        .putAllProperties(namespace.getProperties());

    // the ID is optional when a new table is created - will be assigned on the server side
    if (null != namespace.getId()) {
      builder.setId(namespace.getId());
    }

    return builder.build();
  }

  public static Namespace fromProto(com.dremio.services.nessie.grpc.api.Namespace namespace) {
    Preconditions.checkArgument(null != namespace, "Namespace must be non-null");
    return ImmutableNamespace.builder()
      .id(asId(namespace.getId()))
      .elements(namespace.getElementsList())
      .properties(namespace.getPropertiesMap())
      .build();
  }

  public static NamespaceRequest toProto(NamespaceParams params) {
    Preconditions.checkArgument(null != params, "NamespaceParams must be non-null");
    NamespaceRequest.Builder builder = NamespaceRequest.newBuilder()
      .setNamedRef(params.getRefName())
      .setNamespace(toProto(params.getNamespace()));
    if (null != params.getHashOnRef()) {
      builder.setHashOnRef(params.getHashOnRef());
    }
    return builder.build();
  }

  public static NamespaceParams fromProto(NamespaceRequest request) {
    Preconditions.checkArgument(null != request, "NamespaceRequest must be non-null");
    NamespaceParamsBuilder builder = NamespaceParams.builder()
      .refName(request.getNamedRef())
      .namespace(fromProto(request.getNamespace()));
    if (request.hasHashOnRef()) {
      builder.hashOnRef(request.getHashOnRef());
    }
    return builder.build();
  }

  public static MultipleNamespacesRequest toProto(MultipleNamespacesParams params) {
    Preconditions.checkArgument(null != params, "MultipleNamespacesParams must be non-null");
    MultipleNamespacesRequest.Builder builder = MultipleNamespacesRequest.newBuilder()
      .setNamedRef(params.getRefName());
    if (null != params.getNamespace()) {
      builder.setNamespace(toProto(params.getNamespace()));
    }
    if (null != params.getHashOnRef()) {
      builder.setHashOnRef(params.getHashOnRef());
    }
    return builder.build();
  }

  public static MultipleNamespacesParams fromProto(MultipleNamespacesRequest request) {
    Preconditions.checkArgument(null != request, "MultipleNamespacesRequest must be non-null");
    MultipleNamespacesParamsBuilder builder = MultipleNamespacesParams.builder()
      .refName(request.getNamedRef());
    if (request.hasNamespace()) {
      builder.namespace(fromProto(request.getNamespace()));
    }
    if (request.hasHashOnRef()) {
      builder.hashOnRef(request.getHashOnRef());
    }
    return builder.build();
  }

  public static MultipleNamespacesResponse toProto(GetNamespacesResponse response) {
    Preconditions.checkArgument(null != response, "GetNamespacesResponse must be non-null");
    return MultipleNamespacesResponse.newBuilder()
      .addAllNamespaces(response.getNamespaces()
        .stream().map(ProtoUtil::toProto)
        .collect(Collectors.toList()))
      .build();
  }

  public static GetNamespacesResponse fromProto(MultipleNamespacesResponse response) {
    Preconditions.checkArgument(null != response, "MultipleNamespacesResponse must be non-null");
    return ImmutableGetNamespacesResponse.builder()
      .addAllNamespaces(response.getNamespacesList()
        .stream().map(ProtoUtil::fromProto)
        .collect(Collectors.toList()))
      .build();
  }

  private static org.projectnessie.model.MergeBehavior fromProto(MergeBehavior mergeBehavior) {
    return org.projectnessie.model.MergeBehavior.valueOf(mergeBehavior.name());
  }

  private static MergeBehavior toProto(org.projectnessie.model.MergeBehavior mergeBehavior) {
    return MergeBehavior.valueOf(mergeBehavior.name());
  }

  private static MergeResponse.ContentKeyConflict fromProto(ContentKeyConflict conflict) {
    return MergeResponse.ContentKeyConflict.valueOf(conflict.name());
  }

  private static ContentKeyConflict toProto(MergeResponse.ContentKeyConflict conflict) {
    return ContentKeyConflict.valueOf(conflict.name());
  }

  private static Conflict fromProto(ContentKeyConflictDetails conflict) {
    return ImmutableConflict.builder()
      .conflictType(Conflict.ConflictType.parse(conflict.getConflictType()))
      .key(fromProto(conflict.getKey()))
      .message(conflict.getMessage())
      .build();
  }

  private static ContentKeyConflictDetails toProto(Conflict conflict) {
    ContentKeyConflictDetails.Builder builder = ContentKeyConflictDetails.newBuilder();
    builder.setMessage(conflict.message());
    if (null != conflict.conflictType()) {
      builder.setConflictType(conflict.conflictType().name());
    }
    if (null != conflict.key()) {
      builder.setKey(toProto(conflict.key()));
    }
    return builder.build();
  }

  public static CommitMeta fromProto(Supplier<String> message, Supplier<Boolean> hasCommitMeta,
                                     Supplier<com.dremio.services.nessie.grpc.api.CommitMeta> commitMeta) {
    String msg = Strings.emptyToNull(message.get());

    ImmutableCommitMeta.Builder meta = CommitMeta.builder();
    if (hasCommitMeta.get()) {
      com.dremio.services.nessie.grpc.api.CommitMeta requestMeta = commitMeta.get();
      String metaMsg = Strings.emptyToNull(requestMeta.getMessage());
      meta.from(fromProto(requestMeta));
      if (metaMsg == null && msg != null) {
        meta.message(msg);
      }
    } else {
      meta.message(msg == null ? "" : msg);
    }

    return meta.build();
  }

  public static Merge fromProto(MergeRequest request) {
    Preconditions.checkArgument(null != request, "MergeRequest must be non-null");
    ImmutableMerge.Builder builder = ImmutableMerge.builder()
      .fromHash(request.getFromHash())
      .fromRefName(request.getFromRefName());
    if (request.hasKeepIndividualCommits()) {
      builder.keepIndividualCommits(request.getKeepIndividualCommits());
    }
    if (request.hasDryRun()) {
      builder.isDryRun(request.getDryRun());
    }
    if (request.hasReturnConflictAsResult()) {
      builder.isReturnConflictAsResult(request.getReturnConflictAsResult());
    }
    if (request.hasFetchAdditionalInfo()) {
      builder.isFetchAdditionalInfo(request.getFetchAdditionalInfo());
    }
    if (request.hasDefaultKeyMergeMode()) {
      builder.defaultKeyMergeMode(fromProto(request.getDefaultKeyMergeMode()));
    }
    request.getMergeModesList().forEach(m -> builder.addKeyMergeModes(
      ImmutableMergeKeyBehavior.builder()
        .key(fromProto(m.getKey()))
        .mergeBehavior(fromProto(m.getMergeBehavior()))
        .build()));
    return builder.build();
  }

  public static MergeRequest toProto(String branchName, String hash, Merge merge, String message,
                                     CommitMeta commitMeta) {
    Preconditions.checkArgument(null != merge, "Merge must be non-null");
    MergeRequest.Builder builder =
      MergeRequest.newBuilder().setToBranch(branchName).setExpectedHash(hash);
    if (null != merge.getFromHash()) {
      builder.setFromHash(merge.getFromHash());
    }
    if (null != merge.getFromRefName()) {
      builder.setFromRefName(merge.getFromRefName());
    }
    if (null != merge.keepIndividualCommits()) {
      builder.setKeepIndividualCommits(merge.keepIndividualCommits());
    }
    if (null != merge.isDryRun()) {
      builder.setDryRun(merge.isDryRun());
    }
    if (null != merge.isReturnConflictAsResult()) {
      builder.setReturnConflictAsResult(merge.isReturnConflictAsResult());
    }
    if (null != merge.isFetchAdditionalInfo()) {
      builder.setFetchAdditionalInfo(merge.isFetchAdditionalInfo());
    }
    if (null != merge.getDefaultKeyMergeMode()) {
      builder.setDefaultKeyMergeMode(toProto(merge.getDefaultKeyMergeMode()));
    }
    if (null != merge.getKeyMergeModes()) {
      merge.getKeyMergeModes().forEach(m ->
        builder.addMergeModesBuilder()
          .setKey(toProto(m.getKey()))
          .setMergeBehavior(toProto(m.getMergeBehavior())));
    }
    if (null != message) {
      builder.setMessage(message);
    }
    if (null != commitMeta) {
      builder.setCommitMeta(toProto(commitMeta));
    }
    return builder.build();
  }

  public static Transplant fromProto(TransplantRequest request) {
    Preconditions.checkArgument(null != request, "TransplantRequest must be non-null");
    ImmutableTransplant.Builder builder = ImmutableTransplant.builder()
      .hashesToTransplant(request.getHashesToTransplantList())
      .fromRefName(request.getFromRefName());
    if (request.hasKeepIndividualCommits()) {
      builder.keepIndividualCommits(request.getKeepIndividualCommits());
    }
    if (request.hasDryRun()) {
      builder.isDryRun(request.getDryRun());
    }
    if (request.hasReturnConflictAsResult()) {
      builder.isReturnConflictAsResult(request.getReturnConflictAsResult());
    }
    if (request.hasFetchAdditionalInfo()) {
      builder.isFetchAdditionalInfo(request.getFetchAdditionalInfo());
    }
    if (request.hasDefaultKeyMergeMode()) {
      builder.defaultKeyMergeMode(fromProto(request.getDefaultKeyMergeMode()));
    }
    request.getMergeModesList().forEach(m -> builder.addKeyMergeModes(
      ImmutableMergeKeyBehavior.builder()
        .key(fromProto(m.getKey()))
        .mergeBehavior(fromProto(m.getMergeBehavior()))
        .build()));
    return builder.build();
  }

  public static String fromProtoMessage(TransplantRequest request) {
    return Strings.emptyToNull(request.getMessage());
  }

  public static TransplantRequest toProto(String branchName, String hash, String message, Transplant transplant) {
    Preconditions.checkArgument(null != transplant, "Transplant must be non-null");
    TransplantRequest.Builder builder = TransplantRequest.newBuilder()
      .setBranchName(branchName)
      .setHash(hash)
      .addAllHashesToTransplant(transplant.getHashesToTransplant());
    if (null != message) {
      builder.setMessage(message);
    }
    if (null != transplant.getFromRefName()) {
      builder.setFromRefName(transplant.getFromRefName());
    }
    if (null != transplant.keepIndividualCommits()) {
      builder.setKeepIndividualCommits(transplant.keepIndividualCommits());
    }
    if (null != transplant.isDryRun()) {
      builder.setDryRun(transplant.isDryRun());
    }
    if (null != transplant.isReturnConflictAsResult()) {
      builder.setReturnConflictAsResult(transplant.isReturnConflictAsResult());
    }
    if (null != transplant.isFetchAdditionalInfo()) {
      builder.setFetchAdditionalInfo(transplant.isFetchAdditionalInfo());
    }
    if (null != transplant.getDefaultKeyMergeMode()) {
      builder.setDefaultKeyMergeMode(toProto(transplant.getDefaultKeyMergeMode()));
    }
    if (null != transplant.getKeyMergeModes()) {
      transplant.getKeyMergeModes().forEach(m ->
        builder.addMergeModesBuilder()
          .setKey(toProto(m.getKey()))
          .setMergeBehavior(toProto(m.getMergeBehavior())));
    }
    return builder.build();
  }

  public static MergeResponse fromProto(com.dremio.services.nessie.grpc.api.MergeResponse mergeResponse) {
    Preconditions.checkArgument(null != mergeResponse, "MergeResponse must be non-null");
    ImmutableMergeResponse.Builder builder = ImmutableMergeResponse.builder();
    builder.wasApplied(mergeResponse.getWasApplied());
    builder.wasSuccessful(mergeResponse.getWasSuccessful());
    builder.targetBranch(mergeResponse.getTargetBranchName());
    builder.effectiveTargetHash(mergeResponse.getEffectiveTargetHash());
    if (mergeResponse.hasResultantTargetHash()) {
      builder.resultantTargetHash(mergeResponse.getResultantTargetHash());
    }
    if (mergeResponse.hasCommonAncestorHash()) {
      builder.commonAncestor(mergeResponse.getCommonAncestorHash());
    }
    if (mergeResponse.hasExpectedHash()) {
      builder.expectedHash(mergeResponse.getExpectedHash());
    }
    mergeResponse.getSourceCommitsList().forEach(logEntry -> builder.addSourceCommits(fromProto(logEntry)));
    if (mergeResponse.hasTargetCommits()) {
      builder.addAllTargetCommits(Collections.emptyList()); // make sure it is not null
      mergeResponse.getTargetCommits().getEntriesList().forEach(e -> builder.addTargetCommits(fromProto(e)));
    }
    mergeResponse.getDetailsList().forEach(d -> {
      ImmutableContentKeyDetails.Builder details = ImmutableContentKeyDetails.builder()
        .key(fromProto(d.getContentKey()))
        .conflictType(fromProto(d.getConflictType()))
        .mergeBehavior(fromProto(d.getMergeBehavior()))
        .sourceCommits(d.getSourceCommitHashesList())
        .targetCommits(d.getTargetCommitHashesList());

      if (d.hasConflict()) {
        details.conflict(fromProto(d.getConflict()));
      }

      builder.addDetails(details.build());
    });

    return builder.build();
  }

  public static com.dremio.services.nessie.grpc.api.MergeResponse toProto(MergeResponse mergeResponse) {
    Preconditions.checkArgument(null != mergeResponse, "MergeResponse must be non-null");
    com.dremio.services.nessie.grpc.api.MergeResponse.Builder builder =
      com.dremio.services.nessie.grpc.api.MergeResponse.newBuilder();
    builder.setWasApplied(mergeResponse.wasApplied());
    builder.setWasSuccessful(mergeResponse.wasSuccessful());
    builder.setTargetBranchName(mergeResponse.getTargetBranch());
    builder.setEffectiveTargetHash(mergeResponse.getEffectiveTargetHash());
    if (null != mergeResponse.getResultantTargetHash()) {
      builder.setResultantTargetHash(mergeResponse.getResultantTargetHash());
    }
    if (null != mergeResponse.getCommonAncestor()) {
      builder.setCommonAncestorHash(mergeResponse.getCommonAncestor());
    }
    if (null != mergeResponse.getExpectedHash()) {
      builder.setExpectedHash(mergeResponse.getExpectedHash());
    }
    mergeResponse.getSourceCommits().forEach(logEntry -> builder.addSourceCommits(toProto(logEntry)));
    if (null != mergeResponse.getTargetCommits()) {
      com.dremio.services.nessie.grpc.api.CommitLogEntries.Builder fieldBuilder =
        builder.getTargetCommitsBuilder(); // this call ensures the field is set even if the list is empty
      mergeResponse.getTargetCommits().forEach(e -> fieldBuilder.addEntries(toProto(e)));
    }
    mergeResponse.getDetails().forEach(d -> {
      ContentKeyDetails.Builder details = builder
        .addDetailsBuilder();
      details.setContentKey(toProto(d.getKey()));
      details.setConflictType(toProto(d.getConflictType()));
      details.addAllSourceCommitHashes(d.getSourceCommits());
      details.addAllTargetCommitHashes(d.getTargetCommits());
      details.setMergeBehavior(toProto(d.getMergeBehavior()));
      if (null != d.getConflict()) {
        details.setConflict(toProto(d.getConflict()));
      }
    });
    return builder.build();
  }

  public static <T> T fromProto(Supplier<Boolean> isPresent, Supplier<T> value) {
    if (!isPresent.get()) {
      return null;
    }

    return value.get();
  }
}
