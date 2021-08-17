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

package com.dremio.service.flight.impl;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;

/**
 * {@link UserResponseHandler} implementation for {@link FlightWorkManager#getCatalogs}.
 */
class GetCatalogsResponseHandler implements UserResponseHandler {
  private final BufferAllocator allocator;
  private final FlightProducer.ServerStreamListener listener;

  public GetCatalogsResponseHandler(BufferAllocator allocator, FlightProducer.ServerStreamListener listener) {
    this.allocator = allocator;
    this.listener = listener;
  }

  @Override
  public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
                       QueryWritableBatch result) {
  }

  @Override
  public void completed(UserResult result) {
    UserProtos.GetCatalogsResp catalogsResp = result.unwrap(UserProtos.GetCatalogsResp.class);

    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA,
      allocator)) {
      listener.start(vectorSchemaRoot);

      vectorSchemaRoot.allocateNew();
      VarCharVector catalogNameVector = (VarCharVector) vectorSchemaRoot.getVector("catalog_name");

      int i = 0;
      for (UserProtos.CatalogMetadata catalogMetadata : catalogsResp.getCatalogsList()) {
        catalogNameVector.setSafe(i, new Text(catalogMetadata.getCatalogName()));
        i++;
      }

      vectorSchemaRoot.setRowCount(catalogsResp.getCatalogsCount());
      listener.putNext();
      listener.completed();
    }
  }
}
