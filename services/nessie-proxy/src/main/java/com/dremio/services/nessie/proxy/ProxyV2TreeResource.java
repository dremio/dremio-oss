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
import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonView;
import java.util.List;
import java.util.Locale;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Path;
import org.projectnessie.api.v2.http.HttpTreeApi;
import org.projectnessie.api.v2.params.BaseMergeTransplant;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.api.v2.params.ReferenceHistoryParams;
import org.projectnessie.api.v2.params.ReferenceResolver;
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
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.ser.Views;

@RequestScoped
@Path("api/v2/trees")
public class ProxyV2TreeResource implements HttpTreeApi {
  private final NessieApiV2 api;

  @Inject
  public ProxyV2TreeResource(NessieApiV2 api) {
    this.api = api;
  }

  private ParsedReference resolveRef(String refPathString) {
    return resolveReferencePathElement(refPathString, Reference.ReferenceType.BRANCH);
  }

  private ParsedReference resolveReferencePathElement(
      String refPathString, Reference.ReferenceType namedRefType) {
    return ReferenceResolver.resolveReferencePathElement(
        refPathString, namedRefType, () -> api.getConfig().getDefaultBranch());
  }

  private static Reference.ReferenceType parseReferenceType(String type) {
    if (type == null) {
      return null;
    }
    return Reference.ReferenceType.valueOf(type.toUpperCase(Locale.ROOT));
  }

  @Override
  @JsonView(Views.V2.class)
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    GetAllReferencesBuilder request =
        api.getAllReferences()
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
  public SingleReferenceResponse createReference(String name, String typeName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {

    Reference.ReferenceType type = parseReferenceType(typeName);

    String fromRefName = null;
    String fromHash = null;
    if (reference != null) {
      fromRefName = reference.getName();
      fromHash = reference.getHash();
    }

    Reference toCreate = toReference(name, type, fromHash);
    Reference created =
        api.createReference().sourceRefName(fromRefName).reference(toCreate).create();
    return SingleReferenceResponse.builder().reference(created).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse getReferenceByName(GetReferenceParams params)
      throws NessieNotFoundException {
    ParsedReference ref = resolveRef(params.getRef());
    checkArgument(
        ref.hashWithRelativeSpec() == null,
        "Hashes are not allowed when fetching a reference by name");
    Reference result = api.getReference().refName(ref.name()).fetch(params.fetchOption()).get();
    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public EntriesResponse getEntries(String ref, EntriesParams params)
      throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    GetEntriesBuilder request =
        api.getEntries()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
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
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    GetCommitLogBuilder request =
        api.getCommitLog()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
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

    GetDiffBuilder request =
        api.getDiff()
            .fromRefName(from.name())
            .fromHashOnRef(from.hashWithRelativeSpec())
            .toRefName(to.name())
            .toHashOnRef(to.hashWithRelativeSpec())
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
  public SingleReferenceResponse assignReference(String typeName, String ref, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {

    Reference.ReferenceType type = parseReferenceType(typeName);
    ParsedReference reference = resolveReferencePathElement(ref, type);

    Reference result =
        api.assignReference()
            .refName(reference.name())
            .refType(type)
            .hash(reference.hashWithRelativeSpec())
            .assignTo(assignTo)
            .assignAndGet();

    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public SingleReferenceResponse deleteReference(String typeName, String ref)
      throws NessieConflictException, NessieNotFoundException {

    Reference.ReferenceType type = parseReferenceType(typeName);
    ParsedReference reference = resolveReferencePathElement(ref, type);

    Reference result =
        api.deleteReference()
            .refName(reference.name())
            .refType(type)
            .hash(reference.hashWithRelativeSpec())
            .getAndDelete();

    return SingleReferenceResponse.builder().reference(result).build();
  }

  @Override
  @JsonView(Views.V2.class)
  public ContentResponse getContent(
      ContentKey key, String ref, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {
    ParsedReference reference = resolveRef(ref);
    return api.getContent()
        .refName(reference.name())
        .hashOnRef(reference.hashWithRelativeSpec())
        .forWrite(forWrite)
        .getSingle(key);
  }

  @Override
  @JsonView(Views.V2.class)
  public GetMultipleContentsResponse getSeveralContents(
      String ref, List<String> keys, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {
    ImmutableGetMultipleContentsRequest.Builder request = GetMultipleContentsRequest.builder();
    keys.forEach(k -> request.addRequestedKeys(ContentKey.fromPathString(k)));
    return getMultipleContents(ref, request.build(), withDocumentation, forWrite);
  }

  @Override
  @JsonView(Views.V2.class)
  public GetMultipleContentsResponse getMultipleContents(
      String ref, GetMultipleContentsRequest request, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {

    ParsedReference reference = resolveRef(ref);
    return api.getContent()
        .refName(reference.name())
        .hashOnRef(reference.hashWithRelativeSpec())
        .keys(request.getRequestedKeys())
        .forWrite(forWrite)
        .getWithResponse();
  }

  @Override
  @JsonView(Views.V2.class)
  public MergeResponse transplantCommitsIntoBranch(String branch, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {

    ParsedReference targetRef = resolveRef(branch);
    TransplantCommitsBuilder request =
        api.transplantCommitsIntoBranch()
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
    MergeReferenceBuilder request =
        api.mergeRefIntoBranch()
            .commitMeta(merge.getCommitMeta())
            .message(merge.getMessage())
            .fromRefName(merge.getFromRefName())
            .fromHash(merge.getFromHash());

    mergeTransplantAttr(request, targetRef, merge);

    return request.merge();
  }

  private <T extends MergeTransplantBuilder<T>> void mergeTransplantAttr(
      MergeTransplantBuilder<T> request, ParsedReference targetRef, BaseMergeTransplant base) {
    request.branchName(targetRef.name());
    request.hash(targetRef.hashWithRelativeSpec());

    request.defaultMergeMode(base.getDefaultKeyMergeMode());
    if (base.getKeyMergeModes() != null) {
      base.getKeyMergeModes()
          .forEach(kmt -> request.mergeMode(kmt.getKey(), kmt.getMergeBehavior()));
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
        .hash(reference.hashWithRelativeSpec())
        .commitMeta(operations.getCommitMeta())
        .operations(operations.getOperations())
        .commitWithResponse();
  }

  @Override
  public ReferenceHistoryResponse getReferenceHistory(ReferenceHistoryParams params)
      throws NessieNotFoundException {
    return api.referenceHistory()
        .refName(params.getRef())
        .headCommitsToScan(params.headCommitsToScan())
        .get();
  }

  private ParsedReference parseRefPathString(String refPathString) {
    return parseRefPathString(refPathString, Reference.ReferenceType.BRANCH);
  }

  private ParsedReference parseRefPathString(
      String refPathString, Reference.ReferenceType referenceType) {
    return resolveReferencePathElement(refPathString, referenceType);
  }
}
