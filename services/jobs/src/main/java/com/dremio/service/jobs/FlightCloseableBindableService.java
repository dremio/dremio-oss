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
package com.dremio.service.jobs;

import java.util.concurrent.ExecutorService;

import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.service.grpc.CloseableBindableService;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;



/**
 * A wrapper class around FlightBindingService
 */
public class FlightCloseableBindableService implements CloseableBindableService {
  private final JobsFlightProducer producer;
  private final BufferAllocator allocator;
  private final BindableService jobsFlightService;

  public FlightCloseableBindableService(BufferAllocator allocator, JobsFlightProducer producer,
                                        ServerAuthHandler authHandler, ExecutorService executor) {
    this.producer = producer;
    this.allocator = allocator;
    this.jobsFlightService = FlightGrpcUtils.createFlightService(allocator, producer,
      authHandler, executor);
  }

  @Override
  public ServerServiceDefinition bindService() {
    return jobsFlightService.bindService();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(producer, allocator);
  }
}
