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
package com.dremio.service.sysflight;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.dremio.exec.record.BatchSchema;

/**
 * Interface to get schema & data for the SysFlight tables
 */
public interface SysFlightDataProvider {

  void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator, int recordBatchSize)
    throws Exception;

  BatchSchema getSchema();


  /**
   * No op implementation
   */
  SysFlightDataProvider NO_OP = new SysFlightDataProvider() {
    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener,
      BufferAllocator allocator, int recordBatchSize) { }

    @Override
    public BatchSchema getSchema() {
      return BatchSchema.EMPTY;
    }
  };
}
