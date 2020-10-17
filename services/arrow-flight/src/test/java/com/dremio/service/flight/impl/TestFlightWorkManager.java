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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for FlightWorkManager.
 */
public class TestFlightWorkManager {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetQueryPath() {
    // Arrange
    thrown.expectMessage("FlightDescriptor type Path is unimplemented.");
    thrown.expect(FlightRuntimeException.class);

    final FlightDescriptor flightDescriptor = FlightDescriptor.path("pathelement");

    // Act
    FlightWorkManager.getQuery(flightDescriptor);
  }

  @Test
  public void testGetQueryNullCommand() {
    // Arrange
    thrown.expectMessage("FlightDescriptor type Cmd must have content in the cmd member.");
    thrown.expect(FlightRuntimeException.class);

    final FlightDescriptor flightDescriptor = FlightDescriptor.command(null);

    // Act
    FlightWorkManager.getQuery(flightDescriptor);
  }

  @Test
  public void testGetQueryEmptyCommand() {
    // Arrange
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(new byte[0]);

    // Act
    String actual = FlightWorkManager.getQuery(flightDescriptor);

    // Assert
    assertEquals("", actual);
  }

  @Test
  public void testGetQueryCorrectCommand() {
    // Arrange
    final String expected = "command";
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(expected.getBytes(StandardCharsets.UTF_8));

    // Act
    final String actual = FlightWorkManager.getQuery(flightDescriptor);

    // Assert
    assertEquals(expected, actual);
  }
}
