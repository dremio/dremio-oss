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

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handle;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static org.projectnessie.services.impl.RefUtil.toReference;

import com.dremio.services.nessie.grpc.api.DiffRequest;
import com.dremio.services.nessie.grpc.api.DiffResponse;
import com.dremio.services.nessie.grpc.api.DiffServiceGrpc.DiffServiceImplBase;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.services.spi.PagedCountingResponseHandler;

/** The gRPC service implementation for the Diff-API. */
public class DiffService extends DiffServiceImplBase {

  private final Supplier<? extends org.projectnessie.services.spi.DiffService> bridge;
  private final int maxEntriesPerPage;

  public DiffService(
      Supplier<? extends org.projectnessie.services.spi.DiffService> bridge,
      int maxEntriesPerPage) {
    this.bridge = bridge;
    this.maxEntriesPerPage = maxEntriesPerPage;
  }

  @Override
  public void getDiff(DiffRequest request, StreamObserver<DiffResponse> observer) {

    handle(
        () -> {
          DiffResponse.Builder response = DiffResponse.newBuilder();
          return bridge
              .get()
              .getDiff(
                  request.getFromRefName(),
                  fromProto(request::hasFromHashOnRef, request::getFromHashOnRef),
                  request.getToRefName(),
                  fromProto(request::hasToHashOnRef, request::getToHashOnRef),
                  fromProto(request::hasPageToken, request::getPageToken),
                  new PagedCountingResponseHandler<DiffResponse, DiffEntry>(
                      fromProto(request::hasMaxRecords, request::getMaxRecords),
                      maxEntriesPerPage) {

                    @Override
                    protected boolean doAddEntry(DiffEntry entry) {
                      response.addDiffs(toProto(entry));
                      return true;
                    }

                    @Override
                    public DiffResponse build() {
                      return response.build();
                    }

                    @Override
                    public void hasMore(String pagingToken) {
                      response.setHasMore(true).setPageToken(pagingToken);
                    }
                  },
                  fromReference ->
                      response.setEffectiveFromRef(refToProto(toReference(fromReference))),
                  toReference -> response.setEffectiveToRef(refToProto(toReference(toReference))),
                  fromProto(request::hasMinKey, () -> fromProto(request.getMinKey())),
                  fromProto(request::hasMaxKey, () -> fromProto(request.getMaxKey())),
                  fromProto(request::hasPrefixKey, () -> fromProto(request.getPrefixKey())),
                  fromProto(request.getKeysList()),
                  fromProto(request::hasFilter, request::getFilter));
        },
        observer);
  }
}
