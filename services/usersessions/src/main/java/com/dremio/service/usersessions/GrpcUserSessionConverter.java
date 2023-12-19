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
package com.dremio.service.usersessions;

import java.util.Map;

import org.apache.calcite.avatica.util.Quoting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserSessionProtobuf;
import com.dremio.exec.proto.UserSessionProtobuf.UserSessionRPC;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.protobuf.ProtocolStringList;

/**
 * Contains the static methods to convert from and to protobuf
 */
public final class GrpcUserSessionConverter {
  private static final Logger logger = LoggerFactory.getLogger(GrpcUserSessionConverter.class);
  private static final ObjectMapper mapper = new ObjectMapper(); // thread safe only if the configuration doesn't change

  private GrpcUserSessionConverter() {
  }

  /**
   * Creates a protobuf of a UserSession object.
   *
   * @param session the object to convert to a protobuf
   * @return the protobuf representation of the object
   */
  public static UserSessionRPC toProtoBuf(UserSession session) throws JsonProcessingException {
    final UserSessionProtobuf.SubstitutionSettingsRPC substitutionSettingsRPC =
      UserSessionProtobuf.SubstitutionSettingsRPC.newBuilder()
        .addAllExclusions(session.getSubstitutionSettings().getExclusions())
        .addAllInclusions(session.getSubstitutionSettings().getInclusions())
        .build();

    final UserSessionProtobuf.UserSessionRPC.Builder sessionBuilder = UserSessionProtobuf.UserSessionRPC.newBuilder()
      .setSupportFullyQualifiedProjections(session.supportFullyQualifiedProjections())
      .setUseLegacyCatalogName(session.useLegacyCatalogName())
      .setSubstitutionSettings(substitutionSettingsRPC)
      .setSupportComplexTypes(session.isSupportComplexTypes())
      .setExposeInternalSources(session.exposeInternalSources())
      .setTracingEnabled(session.isTracingEnabled());

    final UserBitShared.RpcEndpointInfos clientInfos = session.getClientInfos();
    if (clientInfos != null) {
      sessionBuilder.setClientInfos(clientInfos);
    }

    final NamespaceKey defaultSchemaPath = session.getDefaultSchemaPath();
    if (defaultSchemaPath != null) {
      sessionBuilder.addAllDefaultSchemaPath(defaultSchemaPath.getPathComponents());
    }

    final UserBitShared.UserCredentials credentials = session.getCredentials();
    if (credentials != null) {
      sessionBuilder.setCredentials(credentials);
    }

    final Quoting quote = session.getInitialQuoting();
    if (quote != null) {
      sessionBuilder.setInitialQuoting(quote.name());
    }

    final UserProtos.RecordBatchFormat batchFormat = session.getRecordBatchFormat();
    if (batchFormat != null) {
      sessionBuilder.setRecordBatchFormat(batchFormat);
    }

    final UserBitShared.QueryId lastQueryId = session.getLastQueryId();
    if (lastQueryId != null) {
      sessionBuilder.setLastQueryIda(lastQueryId);
    }

    final Map<String, VersionContext> sourceVersionMapping = session.getSourceVersionMapping();
    if (sourceVersionMapping != null) {

      for (Map.Entry<String, VersionContext> entry : sourceVersionMapping.entrySet()) {
        final String value = mapper.writeValueAsString(entry.getValue());
        sessionBuilder.putSourceVersionMapping(entry.getKey(), value);
      }
    }

    if (!Strings.isNullOrEmpty(session.getRoutingEngine())) {
      sessionBuilder.setRoutingEngine(session.getRoutingEngine());
    }
    if (!Strings.isNullOrEmpty(session.getRoutingQueue())) {
      sessionBuilder.setRoutingQueue(session.getRoutingQueue());
    }
    if (!Strings.isNullOrEmpty(session.getRoutingTag())) {
      sessionBuilder.setRoutingTag(session.getRoutingTag());
    }
    if (!Strings.isNullOrEmpty(session.getQueryLabel())) {
      sessionBuilder.setQueryLabel(session.getQueryLabel());
    }
    if (!Strings.isNullOrEmpty(session.getTargetUserName())) {
      sessionBuilder.setImpersonationTarget(session.getTargetUserName());
    }

    return sessionBuilder.build();
  }

  /**
   * Creates a UserSession object from a protobuf UserSession object.
   * <p>
   * Note: the SessionOptionManager and OptionManager are not rehydrated
   *
   * @param userSessionRPC a ProtoBuf representation of the UserSession
   * @return the UserSession object
   */
  public static UserSession fromProtoBuf(UserSessionProtobuf.UserSessionRPC userSessionRPC) throws JsonProcessingException {

    final SubstitutionSettings substitutionSettings =
      new SubstitutionSettings(userSessionRPC.getSubstitutionSettings().getExclusionsList());
    if (!userSessionRPC.getSubstitutionSettings().getInclusionsList().isEmpty()) {
      substitutionSettings.setInclusions(userSessionRPC.getSubstitutionSettings().getInclusionsList());
    }

    final UserProtos.UserProperties.Builder userPropBuilder = UserProtos.UserProperties.newBuilder()
      .addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.TRACING_ENABLED).setValue(String.valueOf(userSessionRPC.getTracingEnabled())).build());
    if (userSessionRPC.hasRoutingQueue()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.ROUTING_QUEUE).setValue(userSessionRPC.getRoutingQueue()).build());
    }
    if (userSessionRPC.hasRoutingEngine()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.ROUTING_ENGINE).setValue(userSessionRPC.getRoutingEngine()).build());
    }
    if (userSessionRPC.hasRoutingTag()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.ROUTING_TAG).setValue(userSessionRPC.getRoutingTag()).build());
    }
    if (userSessionRPC.hasQueryLabel()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
              .setKey(UserSession.QUERY_LABEL).setValue(userSessionRPC.getQueryLabel()).build());
    }
    if (userSessionRPC.hasImpersonationTarget()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.IMPERSONATION_TARGET).setValue(userSessionRPC.getImpersonationTarget()).build());
    }


    final UserSession.Builder sessionBuilder = UserSession.Builder.newBuilder()
      .withUserProperties(userPropBuilder.build());

    if (userSessionRPC.getUseLegacyCatalogName()) {
      sessionBuilder.withLegacyCatalog();
    }

    final ProtocolStringList defaultSchemaPathList = userSessionRPC.getDefaultSchemaPathList();
    if (defaultSchemaPathList != null && !defaultSchemaPathList.isEmpty()) {
      sessionBuilder
        .withDefaultSchema(defaultSchemaPathList);
    }

    if (userSessionRPC.hasSubstitutionSettings()) {
      sessionBuilder
        .withSubstitutionSettings(substitutionSettings);
    }

    if (userSessionRPC.hasRecordBatchFormat()) {
      sessionBuilder.withRecordBatchFormat(userSessionRPC.getRecordBatchFormat());
    }

    if (userSessionRPC.hasSupportFullyQualifiedProjections()) {
      sessionBuilder
        .withFullyQualifiedProjectsSupport(userSessionRPC.getSupportFullyQualifiedProjections());
    }
    if (userSessionRPC.hasExposeInternalSources()) {
      sessionBuilder
        .exposeInternalSources(userSessionRPC.getExposeInternalSources());
    }

    if (userSessionRPC.hasSupportComplexTypes()) {
      sessionBuilder
        .setSupportComplexTypes(userSessionRPC.getSupportComplexTypes());
    }

    if (userSessionRPC.hasCredentials()) {
      sessionBuilder
        .withCredentials(userSessionRPC.getCredentials());
    }

    if (userSessionRPC.hasClientInfos()) {
      sessionBuilder.withClientInfos(userSessionRPC.getClientInfos());
    }

    if (userSessionRPC.hasInitialQuoting()) {
      sessionBuilder.withInitialQuoting(Quoting.valueOf(userSessionRPC.getInitialQuoting()));
    }

    final UserSession session = sessionBuilder.build();


    for (Map.Entry<String, String> entry : userSessionRPC.getSourceVersionMappingMap().entrySet()) {
      final String key = entry.getKey();
      final String value = entry.getValue();

      final VersionContext versionContext = mapper.readValue(value, VersionContext.class);
      session.setSessionVersionForSource(key, versionContext);
    }
    if (userSessionRPC.hasLastQueryIda()) {
      session.setLastQueryId(userSessionRPC.getLastQueryIda());
    }
    return session;
  }
}
