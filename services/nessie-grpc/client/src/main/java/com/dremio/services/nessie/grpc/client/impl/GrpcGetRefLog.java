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

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;

import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.projectnessie.api.v1.params.RefLogParams;
import org.projectnessie.api.v1.params.RefLogParamsBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;

import com.dremio.services.nessie.grpc.api.RefLogServiceGrpc.RefLogServiceBlockingStub;

final class GrpcGetRefLog implements GetRefLogBuilder {

  private final RefLogServiceBlockingStub stub;
  private final RefLogParamsBuilder params = RefLogParams.builder();

  public GrpcGetRefLog(RefLogServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetRefLogBuilder fromHash(@Nullable String fromHash) {
    params.endHash(fromHash);
    return this;
  }

  @Override
  public GetRefLogBuilder untilHash(@Nullable String untilHash) {
    params.startHash(untilHash);
    return this;
  }

  @Override
  public GetRefLogBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetRefLogBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  @Override
  public GetRefLogBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  @Override
  public RefLogResponse get() throws NessieNotFoundException {
    return get(params.build());
  }

  private RefLogResponse get(RefLogParams p) throws NessieNotFoundException {
    return handleNessieNotFoundEx(
      () -> fromProto(stub.getRefLog(toProto(p))));
  }

  @Override
  public Stream<RefLogResponse.RefLogResponseEntry> stream() throws NessieNotFoundException {
    RefLogParams p = params.build();
    return StreamingUtil.generateStream(
      RefLogResponse::getLogEntries, pageToken -> get(p.forNextPage(pageToken)));
  }
}
