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

import static com.dremio.service.usersessions.GrpcUserSessionConverter.fromProtoBuf;
import static com.dremio.service.usersessions.GrpcUserSessionConverter.toProtoBuf;

import java.util.ConcurrentModificationException;
import java.util.UUID;

import javax.inject.Provider;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.transientstore.TransientStore;
import com.dremio.exec.proto.UserSessionProtobuf.UserSessionRPC;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.usersessions.store.UserSessionStoreProvider;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * A store of UserSessions that can be shared between coordinators.
 */
public class UserSessionServiceImpl implements UserSessionService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSessionServiceImpl.class);

  private final Provider<UserSessionStoreProvider> userSessionStoreProvider;
  private final Provider<Integer> ttlProvider;
  private TransientStore<String, UserSessionRPC> userSessionStore;

  public UserSessionServiceImpl(Provider<UserSessionStoreProvider> userSessionStoreProvider, Provider<Integer> ttlProvider) {
    this.userSessionStoreProvider = userSessionStoreProvider;
    this.ttlProvider = ttlProvider;
  }

  @Override
  public void start() throws Exception {
    final int ttlInSeconds = ttlProvider.get();
    this.userSessionStore = this.userSessionStoreProvider.get().getStore(Format.ofString(), Format.ofProtobuf(UserSessionRPC.class), ttlInSeconds);
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public SessionIdAndVersion putSession(UserSession session) {
    final String key = UUID.randomUUID().toString();
    try {
      final UserSessionRPC serializedUserSession = toProtoBuf(session);

      Document<String, UserSessionRPC> doc = this.userSessionStore.put(key, serializedUserSession, KVStore.PutOption.CREATE);
      return new SessionIdAndVersion(doc.getKey(), VersionOption.from(doc));
    } catch (JsonProcessingException je) {
      throw new RuntimeException("failed to serialize the user session", je);
    }
  }

  @Override
  public VersionOption updateSession(String sessionId, VersionOption version, UserSession newSession) {
    UserSessionRPC serializedUserSession;

    try {
      serializedUserSession = toProtoBuf(newSession);
    } catch (JsonProcessingException je) {
      throw new RuntimeException("failed to serialize the user session", je);
    }

    try {
      return VersionOption.from(this.userSessionStore.put(sessionId, serializedUserSession, version));
    } catch (ConcurrentModificationException concurrentModificationException) {

      // if the version we would insert and the version that is in the store are the same then no change is needed
      final Document<String, UserSessionRPC> doc = this.userSessionStore.get(sessionId);
      if (doc == null || !serializedUserSession.equals(doc.getValue())) {
        throw concurrentModificationException;
      }

      return VersionOption.from(doc);
    }
  }

  @Override
  public UserSessionAndVersion getSession(String sessionId) {
    final Document<String, UserSessionRPC> doc = this.userSessionStore.get(sessionId);
    if (doc == null) {
      // no user session
      return null;
    }

    try {
      UserSession userSession = fromProtoBuf(doc.getValue());
      return new UserSessionAndVersion(userSession, VersionOption.from(doc));
    } catch (JsonProcessingException je) {
      logger.debug("failed to deserialize usersession, string: {}", doc.getValue(), je);
      return null;
    }
  }

}
