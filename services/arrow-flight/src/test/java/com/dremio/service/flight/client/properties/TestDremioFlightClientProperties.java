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
package com.dremio.service.flight.client.properties;

import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyClientPropertiesToUserSessionBuilder;
import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyMutableClientProperties;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.junit.Test;

import com.dremio.exec.proto.UserProtos;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Test client properties processing methods in DremioFlightClientProperties.
 */
public class TestDremioFlightClientProperties {
  @Test
  public void testApplyMutableClientProperties() {
    // Arrange
    final String testSchema = "test.catalog.table";

    final UserSession userSession = UserSession.Builder.newBuilder()
      .withDefaultSchema(Arrays.asList(testSchema.split("\\.")))
      .build();

    // Update UserSession with new CallHeaders.
    final CallHeaders testCallHeaders = new ErrorFlightMetadata();
    final String newTestSchema = "new.catalog.table";

    testCallHeaders.insert(UserSession.SCHEMA, newTestSchema);

    // Act
    applyMutableClientProperties(userSession, testCallHeaders);

    // Verify
    assertEquals(newTestSchema, String.join(".", userSession.getDefaultSchemaPath().getPathComponents()));
  }

  @Test
  public void testApplyMutableClientPropertiesWithImmutableProperties() {
    // Arrange
    final String testRoutingTag = "test-tag-value";
    final String testRoutingQueue = "test.queue.value";
    final String testSchema = "test.catalog.table";

    final UserSession userSession = UserSession.Builder.newBuilder()
      .withUserProperties(
        UserProtos.UserProperties.newBuilder()
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.SCHEMA, testSchema))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_TAG, testRoutingTag))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_QUEUE, testRoutingQueue))
          .build())
      .build();

    // Update UserSession with new CallHeaders.
    final CallHeaders updatedHeaders = new ErrorFlightMetadata();
    final String newTestRoutingTag = "new-tag-value";
    final String newTestRoutingQueue = "new.queue.value";
    final String newTestSchema = "new.catalog.table";

    updatedHeaders.insert(UserSession.ROUTING_TAG, newTestRoutingTag);
    updatedHeaders.insert(UserSession.ROUTING_QUEUE, newTestRoutingQueue);
    updatedHeaders.insert(UserSession.SCHEMA, newTestSchema);

    // Act
    applyMutableClientProperties(userSession, updatedHeaders);

    // Verify
    // Routing tag should remain unchanged since it is not a mutable property.
    assertEquals(testRoutingTag, userSession.getRoutingTag());
    // Routing queue should remain unchanged since it is not a mutable property.
    assertEquals(testRoutingQueue, userSession.getRoutingQueue());
    assertEquals(newTestSchema, String.join(".", userSession.getDefaultSchemaPath().getPathComponents()));
  }

  @Test
  public void testApplyMutableClientPropertiesWithUnsupportedProperties() {
    // Arrange
    final String defaultEngineName =  "default-engine-name";
    final CallHeaders incomingHeaders = new ErrorFlightMetadata();
    incomingHeaders.insert("engine-name", "new-engine-name");

    final UserSession userSession = UserSession.Builder.newBuilder()
      .withEngineName(defaultEngineName)
      .build();

    // Act
    applyMutableClientProperties(userSession, incomingHeaders);

    // Verify
    // Unsupported client properties should not have any impact on the UserSessionObject
    assertEquals(defaultEngineName, userSession.getRoutingEngine());
  }

  @Test
  public void testApplyMutableClientPropertiesWithNullCallHeaders() {
    final String testSchema =  "test.catalog.table";
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withDefaultSchema(Arrays.asList(testSchema.split("\\.")))
      .build();
    applyMutableClientProperties(userSession, null);
  }

  @Test
  public void testApplyMutableClientPropertiesWithNullUserSession() {
    applyMutableClientProperties(null, new ErrorFlightMetadata());
  }

  @Test
  public void testBuildWithClientProperties() {
    // Arrange
    final CallHeaders incomingHeaders = new ErrorFlightMetadata();
    final String testRoutingTag = "test-tag-value";
    final String testRoutingQueue = "test.queue.value";
    final String testSchema = "test.catalog.table";

    incomingHeaders.insert(UserSession.ROUTING_TAG, testRoutingTag);
    incomingHeaders.insert(UserSession.ROUTING_QUEUE, testRoutingQueue);
    incomingHeaders.insert(UserSession.SCHEMA, testSchema);

    final UserSession.Builder builder = UserSession.Builder.newBuilder();

    // Act
    applyClientPropertiesToUserSessionBuilder(builder, incomingHeaders);
    final UserSession userSession = builder.build();

    // Verify
    assertEquals(testRoutingTag, userSession.getRoutingTag());
    assertEquals(testRoutingQueue, userSession.getRoutingQueue());
    assertEquals(testSchema, String.join(".", userSession.getDefaultSchemaPath().getPathComponents()));
  }

  @Test
  public void testBuildWithClientPropertiesWithUnsupportedProperties() {
    // Arrange
    final String defaultEngineName =  "default-engine-name";
    final CallHeaders incomingHeaders = new ErrorFlightMetadata();
    incomingHeaders.insert("engine-name", "new-engine-name");

    final UserSession.Builder builder = UserSession.Builder.newBuilder()
      .withEngineName(defaultEngineName);

    // Act
    applyClientPropertiesToUserSessionBuilder(builder, incomingHeaders);
    final UserSession userSession = builder.build();

    // Verify
    // Unsupported client properties should not have any impact on the UserSessionObject
    assertEquals(defaultEngineName, userSession.getRoutingEngine());
  }

  @Test
  public void testBuildWithClientPropertiesWithNullUserSession() {
    applyClientPropertiesToUserSessionBuilder(null, new ErrorFlightMetadata());
  }

  @Test
  public void testBuildWithClientPropertiesWithNullUserSessionAndHeaders() {
    applyClientPropertiesToUserSessionBuilder(null, null);
  }
}
