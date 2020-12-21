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

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightProducer {
  private final FlightWorkManager flightWorkManager;
  private final Location location;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;

  public DremioFlightProducer(Location location, DremioFlightSessionsManager sessionsManager,
                              Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                              BufferAllocator allocator, RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.location = location;
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    try {
      final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
      final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);
      final TicketContent.PreparedStatementTicket preparedStatementTicket = TicketContent.PreparedStatementTicket.parseFrom(ticket.getBytes());

      flightWorkManager.runPreparedStatement(preparedStatementTicket, serverStreamListener, allocator, session);
    } catch (InvalidProtocolBufferException ex) {
      final RuntimeException error = CallStatus.INVALID_ARGUMENT.withCause(ex).withDescription("Invalid ticket used in getStream").toRuntimeException();
      serverStreamListener.error(error);
      throw error;
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listFlights is unimplemented").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
    final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);
    return flightPreparedStatement.getFlightInfo(location);
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("acceptPut is unimplemented").toRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("doAction is unimplemented").toRuntimeException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listActions is unimplemented").toRuntimeException();
  }

  /**
   * Helper method to retrieve CallHeaders from the CallContext.
   *
   * @param callContext the CallContext to retrieve headers from.
   * @return CallHeaders retrieved from provided CallContext.
   */
  private CallHeaders retrieveHeadersFromCallContext(CallContext callContext) {
    return callContext.getMiddleware(FlightConstants.HEADER_KEY).headers();
  }
}
