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
package com.dremio.services.nessie.grpc.client.v1api;

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;

import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.api.params.EntriesParamsBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.EntriesResponse;

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcGetEntries implements GetEntriesBuilder {

  private final TreeServiceBlockingStub stub;
  private final EntriesParamsBuilder params = EntriesParams.builder();
  private String refName;

  public GrpcGetEntries(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetEntriesBuilder namespaceDepth(Integer namespaceDepth) {
    params.namespaceDepth(namespaceDepth);
    return this;
  }

  @Override
  public GetEntriesBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public GetEntriesBuilder hashOnRef(@Nullable String hashOnRef) {
    params.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public GetEntriesBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetEntriesBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  @Override
  public GetEntriesBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  @Override
  public EntriesResponse get() throws NessieNotFoundException {
    return get(params.build());
  }

  private EntriesResponse get(EntriesParams p) throws NessieNotFoundException {
    return handleNessieNotFoundEx(
      () -> fromProto(stub.getEntries(toProto(refName, p))));
  }

  @Override
  public Stream<EntriesResponse.Entry> stream() throws NessieNotFoundException {
    EntriesParams p = params.build();
    return StreamingUtil.generateStream(
      EntriesResponse::getEntries, pageToken -> get(p.forNextPage(pageToken)));
  }
}
