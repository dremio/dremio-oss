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

import java.util.ArrayList;
import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages the system tables.
 */
public class SystemTableManagerImpl implements SystemTableManager {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SystemTableManagerImpl.class);

  private final BufferAllocator allocator;
  private final Provider<Map<TABLES, SysFlightDataProvider>> tablesProvider;
  private int recordBatchSize;

  public SystemTableManagerImpl(BufferAllocator allocator,
                                Provider<Map<TABLES, SysFlightDataProvider>> tablesProvider) {
    this.allocator = allocator;
    this.recordBatchSize = 4000;
    this.tablesProvider = tablesProvider;
  }

  @Override
  public void streamData(SysFlightTicket ticket, ServerStreamListener listener) throws Exception {
    if (tablesProvider.get().containsKey(TABLES.fromString(ticket.getDatasetName()))) {
      tablesProvider.get().get(TABLES.fromString(ticket.getDatasetName()))
        .streamData(ticket, listener, allocator, recordBatchSize);
    } else {
      SystemTableManager.throwUnsupportedException(ticket.getDatasetName());
    }
  }

  @Override
  public void listSchemas(StreamListener<FlightInfo> listener) {
    for(TABLES t : TABLES.values()) {
      try {
        FlightInfo info = new FlightInfo(getSchema(t.getName()), FlightDescriptor.path(t.getName()), new ArrayList<>(), -1, -1);
        listener.onNext(info);
      } catch (Exception e) {
        if (tablesProvider.get().containsKey(t)) {
          throw e;
        }
        // else ignore as the table doesn't have a data/schema provider registered, Enterprise only tables are not registered in OSS.
      }
    }
    listener.onCompleted();
  }

  @Override
  public Schema getSchema(String datasetName) {
    if (tablesProvider.get().containsKey(TABLES.fromString(datasetName))) {
      return tablesProvider.get().get(TABLES.fromString(datasetName)).getSchema();
    } else {
      SystemTableManager.throwUnsupportedException(datasetName);
    }
    return null;
  }

  @Override
  @VisibleForTesting
  public void setRecordBatchSize(int recordBatchSize) {
    this.recordBatchSize = recordBatchSize;
  }

  @VisibleForTesting
  public int getRecordBatchSize() {
    return recordBatchSize;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }
}
