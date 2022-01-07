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
package com.dremio.service.flight;

import java.util.Optional;

import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.google.rpc.Status;

import io.grpc.Metadata;
import io.grpc.protobuf.lite.ProtoLiteUtils;

/**
 * Utilities related to Flight RPC
 */

public final class FlightRpcUtils{

  private static final String GRPC_STATUS_METADATA = "grpc-status-details-bin";
  private static final Metadata.BinaryMarshaller<Status> marshaller =
    ProtoLiteUtils.metadataMarshaller(Status.getDefaultInstance());

  /**
   * Converts the given {@link FlightRuntimeException} to a {@link UserException}, if possible.
   *
   * @param fre status runtime exception
   * @return user exception if one is passed as part of the details
   */
  public static Optional<UserException> fromFlightRuntimeException(FlightRuntimeException fre) {
    if (fre.status().metadata() != null) {
      ErrorFlightMetadata metadata = fre.status().metadata();
      if (metadata.containsKey(GRPC_STATUS_METADATA)) {
        return GrpcExceptionUtil.fromStatus(marshaller.parseBytes(metadata.getByte(GRPC_STATUS_METADATA)));
      }
    }
    return Optional.empty();
  }
}

