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
package com.dremio.service.sqlrunner;

import com.dremio.options.OptionManager;
import com.dremio.service.script.ScriptService;
import com.dremio.service.sqlrunner.proto.SQLRunnerSessionProto;
import com.dremio.service.sqlrunner.store.SQLRunnerSessionStore;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;

/** Implementation of SQLRunnerSessionService. */
public class SQLRunnerSessionServiceImpl implements SQLRunnerSessionService {
  private static final long TTL_IN_HOURS = 30 * 24;

  private final SQLRunnerSessionStore sessionStore;
  private final Provider<OptionManager> optionManager;
  private final Provider<ScriptService> scriptService;

  @Inject
  public SQLRunnerSessionServiceImpl(
      final Provider<SQLRunnerSessionStore> sessionStoreProvider,
      final Provider<OptionManager> optionManager,
      final Provider<ScriptService> scriptService) {
    this.sessionStore = sessionStoreProvider.get();
    this.optionManager = optionManager;
    this.scriptService = scriptService;
  }

  @Override
  public SQLRunnerSession getSession(String userId) {
    SQLRunnerSessionProto.SQLRunnerSession session = getOrCreateSession(userId);

    // Extend ttl_expire_at when the user fetches SQLRunnerSession
    long expireAt = getExpireAt();
    if (session.getTtlExpireAt() < expireAt) {
      SQLRunnerSessionProto.SQLRunnerSession updatedSession =
          session.toBuilder().setTtlExpireAt(expireAt).build();
      return new SQLRunnerSession(sessionStore.update(updatedSession));
    }

    SQLRunnerSession sqlRunnerSession = new SQLRunnerSession(session);
    removeNotAccessibleScripts(sqlRunnerSession);
    if (sqlRunnerSession.getScriptIds().size() == 0) {
      sqlRunnerSession.setCurrentScriptId("");
    } else if (!sqlRunnerSession.getScriptIds().contains(sqlRunnerSession.getCurrentScriptId())) {
      sqlRunnerSession.setCurrentScriptId(sqlRunnerSession.getScriptIds().get(0));
    }

    return sqlRunnerSession;
  }

  @Override
  public SQLRunnerSession updateSession(SQLRunnerSession newSession)
      throws SQLRunnerSessionNotSupportedException {
    SQLRunnerSessionProto.SQLRunnerSession updatedSession =
        newSession.toProtobuf().toBuilder().setTtlExpireAt(getExpireAt()).build();

    SQLRunnerSession session = new SQLRunnerSession(sessionStore.update(updatedSession));

    return session;
  }

  @Override
  public void deleteSession(String userId) throws SQLRunnerSessionNotSupportedException {
    sessionStore.delete(userId);
  }

  @Override
  public SQLRunnerSession newTab(String userId, String scriptId) {
    SQLRunnerSessionProto.SQLRunnerSession existingSession = getOrCreateSession(userId);
    SQLRunnerSessionProto.SQLRunnerSession.Builder builder = existingSession.toBuilder();
    if (!existingSession.getScriptIdsList().contains(scriptId)) {
      builder.addScriptIds(scriptId);
    }
    builder.setCurrentScriptId(scriptId).setTtlExpireAt(getExpireAt());

    return new SQLRunnerSession(sessionStore.update(builder.build()));
  }

  @Override
  public void deleteTab(String userId, String scriptId) {
    SQLRunnerSessionProto.SQLRunnerSession existingSession = getOrCreateSession(userId);

    if (existingSession.getScriptIdsCount() <= 1) {
      throw new LastTabException();
    }
    if (!existingSession.getScriptIdsList().contains(scriptId)) {
      throw new TabNotFoundException();
    }

    List<String> scriptsList = new ArrayList<>(existingSession.getScriptIdsList());
    scriptsList.remove(scriptId);

    final SQLRunnerSessionProto.SQLRunnerSession updatedSession =
        existingSession.toBuilder()
            .clearScriptIds()
            .addAllScriptIds(scriptsList)
            .setCurrentScriptId(
                scriptId.equals(existingSession.getCurrentScriptId())
                    ? existingSession.getScriptIds(0)
                    : existingSession.getCurrentScriptId())
            .setTtlExpireAt(getExpireAt())
            .build();

    sessionStore.update(updatedSession);
  }

  @Override
  public int deleteExpired() {
    return sessionStore.deleteExpired();
  }

  private SQLRunnerSessionProto.SQLRunnerSession getOrCreateSession(String userId) {
    SQLRunnerSessionProto.SQLRunnerSession session = sessionStore.get(userId).orElse(null);
    if (session == null || System.currentTimeMillis() > session.getTtlExpireAt()) {
      session =
          SQLRunnerSessionProto.SQLRunnerSession.newBuilder()
              .setUserId(userId)
              .setCurrentScriptId("")
              .build();
    }

    return session;
  }

  /**
   * Get TTL round up to 1 hour ceiling
   *
   * @return expire time in milliseconds
   */
  private static long getExpireAt() {
    long ttlInHour = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis()) + TTL_IN_HOURS + 1;
    return TimeUnit.HOURS.toMillis(ttlInHour);
  }

  private void removeNotAccessibleScripts(SQLRunnerSession session) {
    List<String> scriptIds = Lists.newArrayList(session.getScriptIds());
    Set<String> accessibleScriptIds = getAccessibleScriptIds();
    if (scriptIds.removeIf(
        s -> {
          return !accessibleScriptIds.contains(s);
        })) {
      session.setScriptIds(scriptIds);
    }
  }

  protected Set<String> getAccessibleScriptIds() {
    return scriptService.get().getScripts(0, Integer.MAX_VALUE, "", "", "", null).parallelStream()
        .map(
            script -> {
              return script.getScriptId();
            })
        .collect(Collectors.toSet());
  }
}
