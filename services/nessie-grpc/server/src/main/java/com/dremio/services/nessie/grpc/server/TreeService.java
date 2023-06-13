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
package com.dremio.services.nessie.grpc.server;

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProtoMessage;
import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.services.spi.TreeService.MAX_COMMIT_LOG_ENTRIES;

import java.util.function.Supplier;

import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.api.v1.params.GetReferenceParams;
import org.projectnessie.api.v1.params.Merge;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.api.v1.params.Transplant;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Tag;
import org.projectnessie.services.spi.PagedCountingResponseHandler;

import com.dremio.services.nessie.grpc.api.AssignReferenceRequest;
import com.dremio.services.nessie.grpc.api.CommitLogRequest;
import com.dremio.services.nessie.grpc.api.CommitLogResponse;
import com.dremio.services.nessie.grpc.api.CommitRequest;
import com.dremio.services.nessie.grpc.api.CommitResponse;
import com.dremio.services.nessie.grpc.api.CreateReferenceRequest;
import com.dremio.services.nessie.grpc.api.DeleteReferenceRequest;
import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.EntriesRequest;
import com.dremio.services.nessie.grpc.api.EntriesResponse;
import com.dremio.services.nessie.grpc.api.GetAllReferencesRequest;
import com.dremio.services.nessie.grpc.api.GetAllReferencesResponse;
import com.dremio.services.nessie.grpc.api.GetReferenceByNameRequest;
import com.dremio.services.nessie.grpc.api.MergeRequest;
import com.dremio.services.nessie.grpc.api.MergeResponse;
import com.dremio.services.nessie.grpc.api.Reference;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.TransplantRequest;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc;
import com.google.common.base.Strings;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the Tree-API.
 */
public class TreeService extends TreeServiceGrpc.TreeServiceImplBase {

  private final Supplier<? extends org.projectnessie.services.spi.TreeService> bridge;

  public TreeService(Supplier<? extends org.projectnessie.services.spi.TreeService> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getAllReferences(GetAllReferencesRequest request,
    StreamObserver<GetAllReferencesResponse> observer) {
    handle(
      () ->
      {
        ReferencesParams params = fromProto(request);
        return bridge.get().getAllReferences(
            params.fetchOption(),
            params.filter(),
            params.pageToken(),
            new PagedCountingResponseHandler<GetAllReferencesResponse, org.projectnessie.model.Reference>(
              params.maxRecords()) {

              private final GetAllReferencesResponse.Builder response = GetAllReferencesResponse.newBuilder();

              @Override
              protected boolean doAddEntry(org.projectnessie.model.Reference entry) {
                response.addReference(refToProto(entry));
                return true;
              }

              @Override
              public GetAllReferencesResponse build() {
                return response.build();
              }

              @Override
              public void hasMore(String pagingToken) {
                response.setHasMore(true).setPageToken(pagingToken);
              }
            }
          );
      },
      observer);
  }

  @Override
  public void getReferenceByName(
    GetReferenceByNameRequest request, StreamObserver<Reference> observer) {
    handle(() -> {
      GetReferenceParams params = fromProto(request);
      return refToProto(bridge.get().getReferenceByName(params.getRefName(), params.fetchOption()));
    }, observer);
  }

  @Override
  public void createReference(CreateReferenceRequest request, StreamObserver<Reference> observer) {
    handle(
      () ->
      {
        org.projectnessie.model.Reference ref = refFromProto(request.getReference());
        return refToProto(
          bridge.get().createReference(
            ref.getName(),
            ref.getType(),
            ref.getHash(),
            "".equals(request.getSourceRefName()) ? null : request.getSourceRefName()));
      },
      observer);
  }

  @Override
  public void getDefaultBranch(Empty request, StreamObserver<Reference> observer) {
    handle(() -> refToProto(bridge.get().getDefaultBranch()), observer);
  }

  @Override
  public void assignReference(AssignReferenceRequest request, StreamObserver<ReferenceResponse> observer) {
    handle(
      () -> {
        org.projectnessie.model.Reference ref;
        if (request.hasBranch()) {
          ref = Branch.of(request.getBranch().getName(), request.getBranch().hasHash() ? request.getBranch().getHash() : null);
        } else if (request.hasTag()) {
          ref = Tag.of(request.getTag().getName(), request.getTag().hasHash() ? request.getTag().getHash() : null);
        } else if (request.hasDetached()) {
          ref = Detached.of(request.getDetached().getHash());
        } else {
          throw new IllegalArgumentException("assignTo must be either a Branch or Tag or Detached");
        }
        org.projectnessie.model.Reference assigned = bridge.get().assignReference(
          fromProto(request.getReferenceType()),
          request.getNamedRef(),
          request.getOldHash(),
          ref);
        return ReferenceResponse.newBuilder().setReference(refToProto(assigned)).build();
      },
      observer);
  }

  @Override
  public void deleteReference(DeleteReferenceRequest request, StreamObserver<ReferenceResponse> observer) {
    handle(
      () -> {
        String refName = request.getNamedRef();
        String refHash = request.getHash();
        bridge.get().deleteReference(fromProto(request.getReferenceType()), refName, refHash);
        // The backend service allows deleting a reference only when the expected hash is equal
        // to the current HEAD of the reference. Therefore, we can construct the response object
        // using input parameters in the successful case.
        return ReferenceResponse.newBuilder()
          .setReference(refToProto(request.getReferenceType(), refName, refHash))
          .build();
      },
      observer);
  }

  @Override
  public void getCommitLog(CommitLogRequest request, StreamObserver<CommitLogResponse> observer) {
    handle(
      () -> {
        CommitLogParams params = fromProto(request);
        return bridge.get().getCommitLog(
          request.getNamedRef(),
          params.fetchOption(),
          params.startHash(),
          params.endHash(),
          params.filter(),
          params.pageToken(),
          new PagedCountingResponseHandler<CommitLogResponse, LogResponse.LogEntry>(
            params.maxRecords(), MAX_COMMIT_LOG_ENTRIES) {

            private final CommitLogResponse.Builder response = CommitLogResponse.newBuilder();

            @Override
            protected boolean doAddEntry(LogResponse.LogEntry entry) {
              response.addLogEntries(toProto(entry));
              return true;
            }

            @Override
            public CommitLogResponse build() {
              return response.build();
            }

            @Override
            public void hasMore(String pagingToken) {
              response.setHasMore(true).setToken(pagingToken);
            }
          }
        );
      },
      observer);
  }

  @Override
  public void getEntries(EntriesRequest request, StreamObserver<EntriesResponse> observer) {
    handle(
      () -> {
        EntriesParams params = fromProto(request);
        EntriesResponse.Builder response = EntriesResponse.newBuilder();
        return bridge.get().getEntries(
          request.getNamedRef(),
          params.hashOnRef(),
          params.namespaceDepth(),
          params.filter(),
          params.pageToken(),
          request.getWithContent(),
          new PagedCountingResponseHandler<EntriesResponse, org.projectnessie.model.EntriesResponse.Entry>(
            params.maxRecords()) {

            @Override
            protected boolean doAddEntry(org.projectnessie.model.EntriesResponse.Entry entry) {
              response.addEntries(toProto(entry));
              return true;
            }

            @Override
            public EntriesResponse build() {
              return response.build();
            }

            @Override
            public void hasMore(String pagingToken) {
              response.setHasMore(true).setToken(pagingToken);
            }
          },
          effectiveRef -> response.setEffectiveReference(refToProto(toReference(effectiveRef))),
          fromProto(request::hasMinKey, () -> fromProto(request.getMinKey())),
          fromProto(request::hasMaxKey, () -> fromProto(request.getMaxKey())),
          fromProto(request::hasPrefixKey, () -> fromProto(request.getPrefixKey())),
          fromProto(request.getKeysList())
        );
      },
      observer);
  }

  @Override
  public void transplantCommitsIntoBranch(TransplantRequest request, StreamObserver<MergeResponse> observer) {
    handle(
      () -> {
        String msg = fromProtoMessage(request);
        CommitMeta meta = CommitMeta.fromMessage(msg == null ? "" : msg);
        Transplant transplant = fromProto(request);
        return toProto(
          bridge.get().transplantCommitsIntoBranch(
            request.getBranchName(),
            request.getHash(),
            meta,
            transplant.getHashesToTransplant(),
            transplant.getFromRefName(),
            transplant.keepIndividualCommits(),
            transplant.getKeyMergeModes(),
            transplant.getDefaultKeyMergeMode(),
            transplant.isDryRun(),
            transplant.isFetchAdditionalInfo(),
            transplant.isReturnConflictAsResult()));
      },
      observer);
  }

  @Override
  public void mergeRefIntoBranch(MergeRequest request, StreamObserver<MergeResponse> observer) {
    handle(
      () -> {
        Merge merge = fromProto(request);
        return toProto(
          bridge.get().mergeRefIntoBranch(
            request.getToBranch(),
            request.getExpectedHash(),
            merge.getFromRefName(),
            merge.getFromHash(),
            merge.keepIndividualCommits(),
            fromProto(request::getMessage, request::hasCommitMeta, request::getCommitMeta),
            merge.getKeyMergeModes(),
            merge.getDefaultKeyMergeMode(),
            merge.isDryRun(),
            merge.isFetchAdditionalInfo(),
            merge.isReturnConflictAsResult()));
      },
      observer);
  }

  @Override
  public void commitMultipleOperations(CommitRequest request, StreamObserver<CommitResponse> observer) {
    handle(
      () ->
        toProto(
          bridge.get().commitMultipleOperations(
            request.getBranch(),
            Strings.emptyToNull(request.getHash()),
            fromProto(request.getCommitOperations()))),
      observer);
  }
}
