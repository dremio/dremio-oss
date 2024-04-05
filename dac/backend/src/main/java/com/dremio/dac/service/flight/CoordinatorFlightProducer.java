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
package com.dremio.dac.service.flight;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.FlightProtos.CoordinatorFlightTicket;
import com.dremio.exec.proto.FlightProtos.CoordinatorFlightTicket.IdentifierCase;
import com.dremio.service.jobs.JobsFlightProducer;
import com.dremio.service.sysflight.SysFlightProducer;
import io.grpc.Status;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy Flight Producer that delegates to sys-flight or jobs producer based on the ticket, as we
 * can have only one flight producer on conduit
 */
public class CoordinatorFlightProducer implements FlightProducer, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorFlightProducer.class);

  private final JobsFlightProducer jobsFlightProducer;
  private final SysFlightProducer sysFlightProducer;

  public CoordinatorFlightProducer(
      JobsFlightProducer jobsFlightProducer, SysFlightProducer sysFlightProducer) {
    this.jobsFlightProducer = jobsFlightProducer;
    this.sysFlightProducer = sysFlightProducer;
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener listener) {
    try {
      final CoordinatorFlightTicket cticket = CoordinatorFlightTicket.parseFrom(ticket.getBytes());
      if (cticket.getIdentifierCase() == IdentifierCase.JOBS_FLIGHT_TICKET) {
        LOGGER.debug("Got getStream request for JOBS_FLIGHT_TICKET ticket: {}", ticket);
        jobsFlightProducer.getStream(callContext, ticket, listener);
      } else {
        LOGGER.debug("Got getStream request for SYS_FLIGHT_TICKET ticket: {}", ticket);
        sysFlightProducer.getStream(callContext, ticket, listener);
      }
    } catch (UserException ue) {
      LOGGER.error("Exception while getStream for ticket {}: ", ticket, ue);
      listener.error(GrpcExceptionUtil.toStatusRuntimeException(ue));
    } catch (Exception e) {
      LOGGER.error("Exception while getStream for ticket {}: ", ticket, e);
      listener.error(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void listFlights(
      CallContext callContext, Criteria criteria, StreamListener<FlightInfo> listener) {
    LOGGER.debug("Got listFlights request");
    try {
      sysFlightProducer.listFlights(callContext, criteria, listener);
    } catch (UserException ue) {
      LOGGER.error("Exception while listFlights: ", ue);
      listener.onError(GrpcExceptionUtil.toStatusRuntimeException(ue));
    } catch (Exception e) {
      LOGGER.error("Exception while listFlights: ", e);
      listener.onError(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor desc) {
    LOGGER.debug("Got getFlightInfo request for descriptor: {}", desc);
    try {
      return sysFlightProducer.getFlightInfo(callContext, desc);
    } catch (UserException e) {
      LOGGER.error("Exception while getFlightInfo: ", e);
      throw e;
    }
  }

  @Override
  public Runnable acceptPut(
      CallContext callContext, FlightStream flightStream, StreamListener<PutResult> listener) {
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
    AutoCloseables.close(jobsFlightProducer, sysFlightProducer);
  }
}
