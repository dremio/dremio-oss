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

import javax.inject.Inject;
import javax.inject.Provider;

import com.dremio.service.users.events.UserDeletionEvent;
import com.dremio.service.users.events.UserServiceEvent;
import com.dremio.service.users.events.UserServiceEventSubscriber;

public class SQLRunnerSessionUserDeletionSubscriber implements UserServiceEventSubscriber {
  private final Provider<SQLRunnerSessionService> sqlRunnerSessionService;

  @Inject
  public SQLRunnerSessionUserDeletionSubscriber(final Provider<SQLRunnerSessionService> sqlRunnerSessionService) {
    this.sqlRunnerSessionService = sqlRunnerSessionService;
  }

  @Override
  public void onUserServiceEvent(UserServiceEvent event) {
    if (!(event instanceof UserDeletionEvent)) {
      return;
    }

    UserDeletionEvent deletionEvent = (UserDeletionEvent) event;
    try {
      sqlRunnerSessionService.get().deleteSession(deletionEvent.getUserId());
    } catch (final Exception ignore) {
    }
  }
}
