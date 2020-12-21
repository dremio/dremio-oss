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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.arrow.flight.CallHeaders;

import com.dremio.exec.proto.UserProtos;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableSet;

/**
 * Class with accepted Dremio Flight client property constants.
 */
public final class DremioFlightClientProperties {
  public static final Set<String> SUPPORTED_FLIGHT_CLIENT_USER_SESSION_PROPERTIES =
    ImmutableSet.of(
      UserSession.ROUTING_TAG,
      UserSession.ROUTING_QUEUE,
      UserSession.SCHEMA);

  /**
   * Apply supported client properties to the provided UserSession Builder.
   *
   * @param builder the UserSession Builder to apply client properties to.
   * @param callHeaders the CallHeaders to parse client properties from.
   */
  public static void applyClientPropertiesToUserSessionBuilder(UserSession.Builder builder, CallHeaders callHeaders) {
    if (callHeaders == null || builder == null) {
      return;
    }

    final List<UserProtos.Property> properties = new ArrayList<>();

    callHeaders.keys().forEach(key -> {
      if (DremioFlightClientProperties.SUPPORTED_FLIGHT_CLIENT_USER_SESSION_PROPERTIES
        .contains(key.toLowerCase(Locale.ROOT))) {
        final String value = callHeaders.get(key);
        properties.add(DremioFlightClientProperties.createUserProperty(key, value));
      }
    });

    if (!properties.isEmpty()) {
      builder.withUserProperties(
        UserProtos.UserProperties.newBuilder().addAllProperties(properties).build());
    }
  }

  /**
   * Apply mutable properties from the CallHeaders to the provided UserSession.
   *
   * @param userSession the UserSession to apply supported mutable properties to.
   * @param callHeaders CallHeaders to parse mutable client properties from.
   */
  public static void applyMutableClientProperties(UserSession userSession, CallHeaders callHeaders) {
    if (callHeaders == null || userSession == null) {
      return;
    }

    callHeaders.keys().forEach(key -> {
      if (key.toLowerCase(Locale.ROOT).equals(UserSession.SCHEMA)) {
        final String value = callHeaders.get(key);
        userSession.setDefaultSchemaPath(Arrays.asList(value.split("\\.")));
      }
    });
  }

  /**
   * Helper method to create a Property with the given key and value pair.
   *
   * @param key the name of the property.
   * @param value the value of the property.
   * @return a Property created with the given key and value.
   */
  public static UserProtos.Property createUserProperty(String key, String value) {
    return UserProtos.Property.newBuilder().setKey(key).setValue(value).build();
  }
}
