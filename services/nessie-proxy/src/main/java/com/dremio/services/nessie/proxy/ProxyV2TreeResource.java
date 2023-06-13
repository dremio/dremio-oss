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

import static com.dremio.services.nessie.proxy.ProxyUtil.toReference;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElementWithDefaultBranch;

import java.util.List;

import javax.inject.Inject;

import org.projectnessie.api.v2.http.HttpTreeApi;
import org.projectnessie.api.v2.params.BaseMergeTransplant;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.MergeTransplantBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.ser.Views;

import com.fasterxml.jackson.annotation.JsonView;

public class ProxyV2TreeResource implements HttpTreeApi {
  private final NessieApiV2 api;

  @Inject
  public ProxyV2TreeResource(NessieApiV2 api) {
    this.api = api;
  }
  private ParsedReference resolveRef(String refPathString) {
    return resolveReferencePathElementWithDefaultBranch(refPathString,
      () -> api.getConfig().getDefaultBranch());
  }

  @Override
  @JsonView(Views.V2.class)
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    GetAllReferencesBuilder request = api.getAllReferences()
      .fetch(params.fetchOption())
      .filter(params.filter())
      .pageToken(params.pageToken());

    Integer maxRecords = params.maxRecords();
    if (maxRecords != null) {
      request.maxRecords(maxRecords);
    }

    return request.get();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse createReference(String name, Reference.ReferenceType type, Reference reference)
    throws NessieNotFoundException, NessieConflictException {

    String fromRefName = null;
    String fromHash = null;
    if (reference != null) {
      fromRefName = reference.getName();
      fromHash = reference.getHash();
    }

    Reference toCreate = toReference(name, type, fromHash);
    Reference created = api.createReference().sourceRefName(fromRefName).reference(toCreate).create();
    return SingleReferenceResponse.builder().reference(created).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse getReferenceByName(GetReferenceParams params) throws NessieNotFoundException {
    ParsedReference ref = resolveRef(params.getRef());
    Reference result = api.getReference().refName(ref.name()).fetch(params.fetchOption()).get();
    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public EntriesResponse getEntries(String ref, EntriesParams params) throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    GetEntriesBuilder request = api.getEntries()
      .refName(reference.name())
      .hashOnRef(reference.hash())
      .withContent(params.withContent())
      .filter(params.filter())
      .minKey(params.minKey())
      .maxKey(params.maxKey())
      .prefixKey(params.prefixKey())
      .pageToken(params.pageToken());

    Integer maxRecords = params.maxRecords();
    if (maxRecords != null) {
      request.maxRecords(maxRecords);
    }

    List<ContentKey> requestedKeys = params.getRequestedKeys();
    if (requestedKeys != null) {
      requestedKeys.forEach(request::key);
    }

    return request.get();
  }

  @Override
  @JsonView(Views.V2.class)
  public LogResponse getCommitLog(String ref, CommitLogParams params) throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    GetCommitLogBuilder request = api.getCommitLog()
      .refName(reference.name())
      .hashOnRef(reference.hash())
      .untilHash(params.startHash())
      .filter(params.filter())
      .fetch(params.fetchOption())
      .pageToken(params.pageToken());

    Integer maxRecords = params.maxRecords();
    if (maxRecords != null) {
      request.maxRecords(maxRecords);
    }

    return request.get();
  }

  @Override
  @JsonView(Views.V2.class)
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    ParsedReference from = resolveRef(params.getFromRef());
    ParsedReference to = resolveRef(params.getToRef());

    GetDiffBuilder request = api.getDiff()
      .fromRefName(from.name())
      .fromHashOnRef(from.hash())
      .toRefName(to.name())
      .toHashOnRef(to.hash())
      .minKey(params.minKey())
      .maxKey(params.maxKey())
      .prefixKey(params.prefixKey())
      .filter(params.getFilter())
      .pageToken(params.pageToken());

    Integer maxRecords = params.maxRecords();
    if (maxRecords != null) {
      request.maxRecords(maxRecords);
    }

    List<ContentKey> requestedKeys = params.getRequestedKeys();
    if (requestedKeys != null) {
      requestedKeys.forEach(request::key);
    }

    return request.get();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse assignReference(Reference.ReferenceType type, String ref, Reference assignTo)
    throws NessieNotFoundException, NessieConflictException {

    ParsedReference reference = resolveReferencePathElement(ref, type);
    Reference result;
    switch (type) {
      case BRANCH:
        result = api.assignBranch()
          .branchName(reference.name())
          .hash(reference.hash())
          .assignTo(assignTo)
          .assignAndGet();
        break;
      case TAG:
        result = api.assignTag()
          .tagName(reference.name())
          .hash(reference.hash())
          .assignTo(assignTo)
          .assignAndGet();
        break;
      default:
        throw new IllegalArgumentException("Unsupported reference type: " + type);
    }

    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse deleteReference(Reference.ReferenceType type, String ref)
    throws NessieConflictException, NessieNotFoundException {

    ParsedReference reference = resolveReferencePathElement(ref, type);
    Reference result;
    switch (type) {
      case BRANCH:
        result = api.deleteBranch().branchName(reference.name()).hash(reference.hash()).getAndDelete();
        break;
      case TAG:
        result = api.deleteTag().tagName(reference.name()).hash(reference.hash()).getAndDelete();
        break;
      default:
        throw new IllegalArgumentException("Unsupported reference type: " + type);
    }
    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public ContentResponse getContent(ContentKey key, String ref, boolean withDocumentation) throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    return api.getContent().refName(reference.name()).hashOnRef(reference.hash()).getSingle(key);
  }

  @Override
  @JsonView(Views.V2.class)
  public GetMultipleContentsResponse getSeveralContents(String ref, List<String> keys, boolean withDocumentation) throws NessieNotFoundException {
    ImmutableGetMultipleContentsRequest.Builder request = GetMultipleContentsRequest.builder();
    keys.forEach(k -> request.addRequestedKeys(ContentKey.fromPathString(k)));
    return getMultipleContents(ref, request.build(), withDocumentation);
  }

  @Override
  @JsonView(Views.V2.class)
  public GetMultipleContentsResponse getMultipleContents(String ref, GetMultipleContentsRequest request, boolean withDocumentation)
    throws NessieNotFoundException {

    ParsedReference reference = resolveRef(ref);
    return api.getContent()
      .refName(reference.name())
      .hashOnRef(reference.hash())
      .keys(request.getRequestedKeys())
      .getWithResponse();
  }

  @Override
  @JsonView(Views.V2.class)
  public MergeResponse transplantCommitsIntoBranch(String branch, Transplant transplant)
    throws NessieNotFoundException, NessieConflictException {

    ParsedReference targetRef = resolveRef(branch);
    TransplantCommitsBuilder request = api.transplantCommitsIntoBranch()
      .message(transplant.getMessage())
      .fromRefName(transplant.getFromRefName())
      .hashesToTransplant(transplant.getHashesToTransplant());

    mergeTransplantAttr(request, targetRef, transplant);

    return request.transplant();
  }

  @Override
  @JsonView(Views.V2.class)
  public MergeResponse mergeRefIntoBranch(String branch, Merge merge)
    throws NessieNotFoundException, NessieConflictException {

    ParsedReference targetRef = resolveRef(branch);
    MergeReferenceBuilder request = api.mergeRefIntoBranch()
      .commitMeta(merge.getCommitMeta())
      .message(merge.getMessage())
      .fromRefName(merge.getFromRefName())
      .fromHash(merge.getFromHash());

    mergeTransplantAttr(request, targetRef, merge);

    return request.merge();
  }

  private <T extends MergeTransplantBuilder<T>> void mergeTransplantAttr(MergeTransplantBuilder<T> request,
                                                                         ParsedReference targetRef,
                                                                         BaseMergeTransplant base) {
    request.branchName(targetRef.name());
    request.hash(targetRef.hash());

    request.defaultMergeMode(base.getDefaultKeyMergeMode());
    if (base.getKeyMergeModes() != null) {
      base.getKeyMergeModes().forEach(kmt -> request.mergeMode(kmt.getKey(), kmt.getMergeBehavior()));
    }

    Boolean dryRun = base.isDryRun();
    if (dryRun != null) {
      request.dryRun(dryRun);
    }

    Boolean fetchAdditionalInfo = base.isFetchAdditionalInfo();
    if (fetchAdditionalInfo != null) {
      request.fetchAdditionalInfo(fetchAdditionalInfo);
    }

    Boolean returnConflictAsResult = base.isReturnConflictAsResult();
    if (returnConflictAsResult != null) {
      request.returnConflictAsResult(returnConflictAsResult);
    }
  }


  @Override
  @JsonView(Views.V2.class)
  public CommitResponse commitMultipleOperations(String branch, Operations operations)
    throws NessieNotFoundException, NessieConflictException {

    ParsedReference reference = resolveRef(branch);
    return api.commitMultipleOperations()
      .branchName(reference.name())
      .hash(reference.hash())
      .commitMeta(operations.getCommitMeta())
      .operations(operations.getOperations())
      .commitWithResponse();
  }
}
