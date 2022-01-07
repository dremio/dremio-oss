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

import static java.lang.Math.toIntExact;

import java.util.ConcurrentModificationException;
import java.util.UUID;

import javax.inject.Provider;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.parquet.Strings;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.transientstore.TransientStore;
import com.dremio.datastore.transientstore.TransientStoreProvider;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserSessionProtobuf;
import com.dremio.exec.proto.UserSessionProtobuf.UserSessionRPC;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * A store of UserSessions that can be shared between coordinators.
 */
public class UserSessionServiceImpl implements UserSessionService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSessionServiceImpl.class);

  private final Provider<OptionManager> optionManagerProvider;
  private final Provider<SessionOptionManager> sessionOptionManagerProvider;
  private final Provider<TransientStoreProvider> userSessionStoreProvider;
  private TransientStore<String, UserSessionRPC> userSessionStore;

  public UserSessionServiceImpl(Provider<OptionManager> optionManagerProvider,
                                Provider<SessionOptionManager> sessionOptionManager, Provider<TransientStoreProvider> userSessionStoreProvider) {
    this.optionManagerProvider = optionManagerProvider;
    this.sessionOptionManagerProvider = sessionOptionManager;
    this.userSessionStoreProvider = userSessionStoreProvider;
  }

  /**
   * Creates a protobuf of a UserSession object.
   *
   * @param session the object to convert to a protobuf
   * @return the protobuf representation of the object
   */
  public static UserSessionRPC toProtoBuf(UserSession session) {
    final UserSessionProtobuf.SubstitutionSettingsRPC substitutionSettingsRPC =
      UserSessionProtobuf.SubstitutionSettingsRPC.newBuilder()
        .addAllExclusions(session.getSubstitutionSettings().getExclusions())
        .addAllInclusions(session.getSubstitutionSettings().getInclusions())
        .build();

    final UserSessionProtobuf.UserSessionRPC.Builder sessionBuilder = UserSessionProtobuf.UserSessionRPC.newBuilder()
      .setClientInfos(session.getClientInfos())
      .addAllDefaultSchemaPath(session.getDefaultSchemaPath().getPathComponents())
      .setCredentials(session.getCredentials())
      .setSupportFullyQualifiedProjections(session.supportFullyQualifiedProjections())
      .setInitialQuoting(session.getInitialQuoting().name())
      .setUseLegacyCatalogName(session.useLegacyCatalogName())
      .setRecordBatchFormat(session.getRecordBatchFormat())
      .setSubstitutionSettings(substitutionSettingsRPC)
      .setSupportComplexTypes(session.isSupportComplexTypes())
      .setExposeInternalSources(session.exposeInternalSources())
      .setLastQueryIda(session.getLastQueryId())
      .setTracingEnabled(session.isTracingEnabled());

    if (!Strings.isNullOrEmpty(session.getRoutingEngine())) {
      sessionBuilder.setRoutingEngine(session.getRoutingEngine());
    }
    if (!Strings.isNullOrEmpty(session.getRoutingQueue())) {
      sessionBuilder.setRoutingQueue(session.getRoutingQueue());
    }
    if (!Strings.isNullOrEmpty(session.getRoutingTag())) {
      sessionBuilder.setRoutingTag(session.getRoutingTag());
    }
    if (!Strings.isNullOrEmpty(session.getTargetUserName())) {
      sessionBuilder.setImpersonationTarget(session.getTargetUserName());
    }

    return sessionBuilder.build();
  }

  /**
   * Creates a UserSession object from a protobuf UserSession object.
   *
   * @param userSessionRPC a ProtoBuf representation of the UserSession
   * @return the UserSession object
   */
  public UserSession fromProtoBuf(UserSessionRPC userSessionRPC) {

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
    if (userSessionRPC.hasImpersonationTarget()) {
      userPropBuilder.addProperties(UserProtos.Property.newBuilder()
        .setKey(UserSession.IMPERSONATION_TARGET).setValue(userSessionRPC.getImpersonationTarget()).build());
    }

    final UserSession.Builder sessionBuilder = UserSession.Builder.newBuilder()
      .withClientInfos(userSessionRPC.getClientInfos())
      .withDefaultSchema(userSessionRPC.getDefaultSchemaPathList())
      .setSupportComplexTypes(userSessionRPC.getSupportComplexTypes())
      .exposeInternalSources(userSessionRPC.getExposeInternalSources())
      .withCredentials(userSessionRPC.getCredentials())
      .withFullyQualifiedProjectsSupport(userSessionRPC.getSupportFullyQualifiedProjections())
      .withLegacyCatalog()
      .withRecordBatchFormat(userSessionRPC.getRecordBatchFormat())
      .withSessionOptionManager(sessionOptionManagerProvider.get(), optionManagerProvider.get())
      .withSubstitutionSettings(substitutionSettings)
      .withInitialQuoting(Quoting.valueOf(userSessionRPC.getInitialQuoting()))
      .withUserProperties(userPropBuilder.build());

    final UserSession session = sessionBuilder.build();
    if (userSessionRPC.hasLastQueryIda()) {
      session.setLastQueryId(userSessionRPC.getLastQueryIda());
    }
    return session;
  }

  @Override
  public void start() throws Exception {
    final int ttlInSeconds = toIntExact(this.optionManagerProvider.get().getOption(UserSessionServiceOptions.SESSION_TTL) * 60);
    this.userSessionStore = this.userSessionStoreProvider.get().getStore(Format.ofString(), Format.ofProtobuf(UserSessionRPC.class), ttlInSeconds);
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public SessionIdAndVersion putSession(UserSession session) {
    final String key = UUID.randomUUID().toString();
    final UserSessionRPC serializedUserSession = toProtoBuf(session);
    Document<String, UserSessionRPC> doc = this.userSessionStore.put(key, serializedUserSession, KVStore.PutOption.CREATE);
    return new SessionIdAndVersion(doc.getKey(), VersionOption.from(doc));
  }

  @Override
  public VersionOption updateSession(String sessionId, VersionOption version, UserSession newSession) {
    final UserSessionRPC serializedUserSession = toProtoBuf(newSession);

    try {
      return VersionOption.from(this.userSessionStore.put(sessionId, serializedUserSession, version));
    } catch (ConcurrentModificationException concurrentModificationException) {

      // if the version we would insert and the version that is in the store are the same then no change is needed
      UserSessionData sessionResponse = this.getSession(sessionId);
      if (sessionResponse == null || !sessionResponse.getSession().equals(newSession)) {
        throw concurrentModificationException;
      }

      return sessionResponse.getVersion();
    }
  }

  @Override
  public UserSessionData getSession(String sessionId) {
    final Document<String, UserSessionRPC> doc = this.userSessionStore.get(sessionId);
    if (doc == null) {
      // no user session
      return null;
    }
    return new UserSessionData(fromProtoBuf(doc.getValue()),VersionOption.from(doc));
  }
}
