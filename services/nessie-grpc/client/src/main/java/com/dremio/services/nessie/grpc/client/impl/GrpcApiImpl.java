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
package com.dremio.services.nessie.grpc.client.impl;

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handleNessieNotFoundEx;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;

import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc;
import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc.ConfigServiceBlockingStub;
import com.dremio.services.nessie.grpc.api.ContentServiceGrpc;
import com.dremio.services.nessie.grpc.api.ContentServiceGrpc.ContentServiceBlockingStub;
import com.dremio.services.nessie.grpc.api.DiffServiceGrpc;
import com.dremio.services.nessie.grpc.api.DiffServiceGrpc.DiffServiceBlockingStub;
import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.RefLogServiceGrpc;
import com.dremio.services.nessie.grpc.api.RefLogServiceGrpc.RefLogServiceBlockingStub;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.client.api.AssignReferenceBuilder;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.DeleteNamespaceBuilder;
import org.projectnessie.client.api.DeleteReferenceBuilder;
import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.client.api.GetNamespaceBuilder;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.ReferenceHistoryBuilder;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.client.api.UpdateRepositoryConfigBuilder;
import org.projectnessie.client.api.ns.ClientSideCreateNamespace;
import org.projectnessie.client.api.ns.ClientSideDeleteNamespace;
import org.projectnessie.client.api.ns.ClientSideGetMultipleNamespaces;
import org.projectnessie.client.api.ns.ClientSideGetNamespace;
import org.projectnessie.client.api.ns.ClientSideUpdateNamespace;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;

/** gRPC client implementation for {@link NessieApiV1}. */
public class GrpcApiImpl implements NessieApiV1, NessieApiV2 {

  private final ManagedChannel channel;
  private final boolean shutdownChannel;
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final TreeServiceBlockingStub treeServiceBlockingStub;
  private final ContentServiceBlockingStub contentServiceBlockingStub;
  private final DiffServiceBlockingStub diffServiceBlockingStub;
  private final RefLogServiceBlockingStub refLogServiceBlockingStub;

  public GrpcApiImpl(ManagedChannel channel, boolean shutdownChannel) {
    this(channel, shutdownChannel, new ClientInterceptor[0]);
  }

  public GrpcApiImpl(
      ManagedChannel channel, boolean shutdownChannel, ClientInterceptor... clientInterceptors) {
    this.channel = channel;
    this.shutdownChannel = shutdownChannel;
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel).withInterceptors(clientInterceptors);
    this.contentServiceBlockingStub =
        ContentServiceGrpc.newBlockingStub(channel).withInterceptors(clientInterceptors);
    this.treeServiceBlockingStub =
        TreeServiceGrpc.newBlockingStub(channel).withInterceptors(clientInterceptors);
    this.diffServiceBlockingStub =
        DiffServiceGrpc.newBlockingStub(channel).withInterceptors(clientInterceptors);
    this.refLogServiceBlockingStub =
        RefLogServiceGrpc.newBlockingStub(channel).withInterceptors(clientInterceptors);
  }

  @Override
  public void close() {
    if (null != channel && shutdownChannel) {
      channel.shutdown();
    }
  }

  @Override
  public NessieConfiguration getConfig() {
    return fromProto(configServiceBlockingStub.getConfig(Empty.newBuilder().build()));
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () ->
            fromProto(
                treeServiceBlockingStub.getDefaultBranch(Empty.newBuilder().build()).getBranch()));
  }

  @Override
  public GetContentBuilder getContent() {
    return new GrpcGetContent(contentServiceBlockingStub);
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    return new GrpcGetAllReferences(treeServiceBlockingStub);
  }

  @Override
  public GetReferenceBuilder getReference() {
    return new GrpcGetReference(treeServiceBlockingStub);
  }

  @Override
  public CreateReferenceBuilder createReference() {
    return new GrpcCreateReference(treeServiceBlockingStub);
  }

  @Override
  public GetEntriesBuilder getEntries() {
    return new GrpcGetEntries(treeServiceBlockingStub);
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    return new GrpcGetCommitLog(treeServiceBlockingStub);
  }

  @Override
  public AssignTagBuilder assignTag() {
    return new GrpcAssignTag(treeServiceBlockingStub);
  }

  @Override
  public DeleteTagBuilder deleteTag() {
    return new GrpcDeleteTag(treeServiceBlockingStub);
  }

  @Override
  public AssignBranchBuilder assignBranch() {
    return new GrpcAssignBranch(treeServiceBlockingStub);
  }

  @Override
  public DeleteBranchBuilder deleteBranch() {
    return new GrpcDeleteBranch(treeServiceBlockingStub);
  }

  @Override
  public DeleteReferenceBuilder deleteReference() {
    return new GrpcDeleteReference(treeServiceBlockingStub);
  }

  @Override
  public AssignReferenceBuilder assignReference() {
    return new GrpcAssignReference(treeServiceBlockingStub);
  }

  @Override
  public TransplantCommitsBuilder transplantCommitsIntoBranch() {
    return new GrpcTransplantCommits(treeServiceBlockingStub);
  }

  @Override
  public MergeReferenceBuilder mergeRefIntoBranch() {
    return new GrpcMergeReference(treeServiceBlockingStub);
  }

  @Override
  public CommitMultipleOperationsBuilder commitMultipleOperations() {
    return new GrpcCommitMultipleOperations(treeServiceBlockingStub);
  }

  @Override
  public GetDiffBuilder getDiff() {
    return new GrpcGetDiff(diffServiceBlockingStub);
  }

  @Override
  public GetRefLogBuilder getRefLog() {
    return new GrpcGetRefLog(refLogServiceBlockingStub);
  }

  @Override
  public GetNamespaceBuilder getNamespace() {
    return new ClientSideGetNamespace(this);
  }

  @Override
  public GetMultipleNamespacesBuilder getMultipleNamespaces() {
    return new ClientSideGetMultipleNamespaces(this);
  }

  @Override
  public CreateNamespaceBuilder createNamespace() {
    return new ClientSideCreateNamespace(this);
  }

  @Override
  public DeleteNamespaceBuilder deleteNamespace() {
    return new ClientSideDeleteNamespace(this);
  }

  @Override
  public UpdateNamespaceBuilder updateProperties() {
    return new ClientSideUpdateNamespace(this);
  }

  @Override
  public GetRepositoryConfigBuilder getRepositoryConfig() {
    return new GrpcGetRepositoryConfig(configServiceBlockingStub);
  }

  @Override
  public UpdateRepositoryConfigBuilder updateRepositoryConfig() {
    return new GrpcUpdateRepositoryConfig(configServiceBlockingStub);
  }

  @Override
  public ReferenceHistoryBuilder referenceHistory() {
    return new GrpcReferenceHistoryBuilder(treeServiceBlockingStub);
  }
}
