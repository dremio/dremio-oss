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

import java.io.IOException;

import org.apache.arrow.flight.Ticket;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Throwables;

/**
 * Arrow Flight Ticket for JobsService.
 */
@JsonSerialize
public class JobsFlightTicket {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String jobId;
  private final int offset;
  private final int limit;

  @JsonCreator
  public JobsFlightTicket(@JsonProperty("jobId") String jobId, @JsonProperty("offset") int offset, @JsonProperty("limit") int limit) {
    this.jobId = jobId;
    this.offset = offset;
    this.limit = limit;
  }

  @JsonProperty
  public String getJobId() {
    return jobId;
  }

  @JsonProperty
  public int getOffset() {
    return offset;
  }

  @JsonProperty
  public int getLimit() {
    return limit;
  }

  /**
   * Deserializes a new instance from the protocol buffer ticket.
   */
  public static JobsFlightTicket from(Ticket ticket) {
    try {
      return MAPPER.readValue(ticket.getBytes(), JobsFlightTicket.class);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   *  Creates a new protocol buffer Ticket by serializing to JSON.
   */
  public Ticket toTicket() {
    try {
      return new Ticket(MAPPER.writeValueAsBytes(this));
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

}
