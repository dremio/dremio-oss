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
package com.dremio.services.nessie.proxy;

import static com.dremio.services.nessie.proxy.ProxyUtil.paging;

import com.fasterxml.jackson.annotation.JsonView;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Path;
import org.projectnessie.api.v1.http.HttpTreeApi;
import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.api.v1.params.GetReferenceParams;
import org.projectnessie.api.v1.params.Merge;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.api.v1.params.Transplant;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.OnBranchBuilder;
import org.projectnessie.client.api.OnReferenceBuilder;
import org.projectnessie.client.api.OnTagBuilder;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.ser.Views;

/** Nessie tree-API REST endpoint that forwards via gRPC. */
@RequestScoped
@Path("api/v1/trees")
public class ProxyTreeResource implements HttpTreeApi {

  @SuppressWarnings("checkstyle:visibilityModifier")
  @Inject
  NessieApiV1 api;

  public ProxyTreeResource() {}

  public ProxyTreeResource(NessieApiV1 api) {
    this.api = api;
  }

  @JsonView(Views.V1.class)
  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    GetAllReferencesBuilder allReferences = api.getAllReferences();
    if (null != params.pageToken()) {
      allReferences.pageToken(params.pageToken());
    }
    if (null != params.maxRecords()) {
      allReferences.maxRecords(params.maxRecords());
    }
    if (null != params.filter()) {
      allReferences.filter(params.filter());
    }
    if (null != params.fetchOption()) {
      allReferences.fetch(params.fetchOption());
    }
    return allReferences.get();
  }

  @JsonView(Views.V1.class)
  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return api.getDefaultBranch();
  }

  @JsonView(Views.V1.class)
  @Override
  public Reference createReference(String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    return api.createReference().sourceRefName(sourceRefName).reference(reference).create();
  }

  @JsonView(Views.V1.class)
  @Override
  public Reference getReferenceByName(GetReferenceParams params) throws NessieNotFoundException {
    GetReferenceBuilder builder = api.getReference().refName(params.getRefName());
    if (null != params.fetchOption()) {
      builder.fetch(params.fetchOption());
    }
    return builder.get();
  }

  @JsonView(Views.V1.class)
  @Override
  public EntriesResponse getEntries(String refName, EntriesParams params)
      throws NessieNotFoundException {
    GetEntriesBuilder req = onReference(api.getEntries(), refName, params.hashOnRef());
    paging(req, params.pageToken(), params.maxRecords());
    if (params.namespaceDepth() != null) {
      req.namespaceDepth(params.namespaceDepth());
    }
    if (params.filter() != null) {
      req.filter(params.filter());
    }
    return req.get();
  }

  @JsonView(Views.V1.class)
  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    GetCommitLogBuilder req = api.getCommitLog();
    if (ref != null) {
      req.refName(ref);
    }
    if (params.endHash() != null) {
      req.hashOnRef(params.endHash());
    }
    if (params.startHash() != null) {
      req.untilHash(params.startHash());
    }
    if (params.filter() != null) {
      req.filter(params.filter());
    }
    if (null != params.fetchOption()) {
      req.fetch(params.fetchOption());
    }
    return paging(req, params.pageToken(), params.maxRecords()).get();
  }

  @JsonView(Views.V1.class)
  @Override
  public void assignReference(
      Reference.ReferenceType referenceType,
      String referenceName,
      String oldHash,
      Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    switch (referenceType) {
      case BRANCH:
        onBranch(api.assignBranch(), referenceName, oldHash).assignTo(assignTo).assign();
        break;
      case TAG:
        onTag(api.assignTag(), referenceName, oldHash).assignTo(assignTo).assign();
        break;
      default:
        throw new IllegalArgumentException("Invalid reference type " + referenceType);
    }
  }

  @JsonView(Views.V1.class)
  @Override
  public void deleteReference(
      Reference.ReferenceType referenceType, String referenceName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    switch (referenceType) {
      case BRANCH:
        onBranch(api.deleteBranch(), referenceName, hash).delete();
        break;
      case TAG:
        onTag(api.deleteTag(), referenceName, hash).delete();
        break;
      default:
        throw new IllegalArgumentException("Invalid reference type " + referenceType);
    }
  }

  @JsonView(Views.V1.class)
  @Override
  public MergeResponse transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    TransplantCommitsBuilder req = onBranch(api.transplantCommitsIntoBranch(), branchName, hash);
    ProxyUtil.applyBaseMergeTransplant(req, transplant);
    if (message != null) {
      req.message(message);
    }
    return req.fromRefName(transplant.getFromRefName())
        .hashesToTransplant(transplant.getHashesToTransplant())
        .transplant();
  }

  @JsonView(Views.V1.class)
  @Override
  public MergeResponse mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    MergeReferenceBuilder req =
        onBranch(api.mergeRefIntoBranch(), branchName, hash).fromRefName(merge.getFromRefName());
    ProxyUtil.applyBaseMergeTransplant(req, merge);
    if (merge.getFromHash() != null) {
      req.fromHash(merge.getFromHash());
    }
    if (merge.keepIndividualCommits() != null) {
      req.keepIndividualCommits(merge.keepIndividualCommits());
    }
    return req.merge();
  }

  @JsonView(Views.V1.class)
  @Override
  public Branch commitMultipleOperations(String branchName, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    return onBranch(api.commitMultipleOperations(), branchName, hash)
        .commitMeta(operations.getCommitMeta())
        .operations(operations.getOperations())
        .commit();
  }

  private static <B extends OnReferenceBuilder<B>> B onReference(
      B builder, String refName, String hashOnRef) {
    if (refName != null) {
      builder.refName(refName);
    }
    if (hashOnRef != null) {
      builder.hashOnRef(hashOnRef);
    }
    return builder;
  }

  private static <B extends OnTagBuilder<B>> B onTag(B builder, String tagName, String hash) {
    if (tagName != null) {
      builder.tagName(tagName);
    }
    if (hash != null) {
      builder.hash(hash);
    }
    return builder;
  }

  private static <B extends OnBranchBuilder<B>> B onBranch(
      B builder, String branchName, String hash) {
    if (branchName != null) {
      builder.branchName(branchName);
    }
    if (hash != null) {
      builder.hash(hash);
    }
    return builder;
  }
}
