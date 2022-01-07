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

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.FlightProtos.CoordinatorFlightTicket;
import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.google.common.annotations.VisibleForTesting;

import io.grpc.Status;

/**
 * Flight Producer for System tables
 */
public class SysFlightProducer implements FlightProducer, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SysFlightProducer.class);

  private final Provider<SystemTableManager> managerProvider;

  public SysFlightProducer(Provider<SystemTableManager> managerProvider) {
    this.managerProvider = managerProvider;
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener listener) {
    LOGGER.debug("Got getStream request for ticket: {}", ticket);
    try {
      final SysFlightTicket sysTicket = CoordinatorFlightTicket.parseFrom(ticket.getBytes()).getSyFlightTicket();
      // todo: set username in RequestContext from SysFlightTicket
      managerProvider.get().streamData(sysTicket, listener);
    } catch (UserException ue) {
      LOGGER.error("UserException while getStream for ticket {}: ", ticket, ue);
      listener.error(GrpcExceptionUtil.toStatusRuntimeException(ue));
    } catch (Throwable e) {
      LOGGER.error("Exception while getStream for ticket {}: ", ticket, e);
      listener.error(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> listener) {
    LOGGER.debug("Got listFlights request");
    try {
      managerProvider.get().listSchemas(listener);
    } catch (UserException ue) {
      LOGGER.error("Exception while listFlights: ", ue);
      listener.onError(GrpcExceptionUtil.toStatusRuntimeException(ue));
    } catch (Throwable e) {
      LOGGER.error("Exception while listFlights: ", e);
      listener.onError(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor desc) {
    LOGGER.debug("Got getFlightInfo request for descriptor: {}", desc);
    try {
      Schema schema =  managerProvider.get().getSchema(String.join(".", desc.getPath()));
      return new FlightInfo(schema, desc, new ArrayList<>(), -1, -1);
    } catch (Throwable e) {
      LOGGER.error("Exception while getFlightInfo: ", e);
      throw Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asRuntimeException();
    }
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(managerProvider.get());
  }

  @VisibleForTesting
  public void setRecordBatchSize(int recordBatchSize) {
    this.managerProvider.get().setRecordBatchSize(recordBatchSize);
  }
}
