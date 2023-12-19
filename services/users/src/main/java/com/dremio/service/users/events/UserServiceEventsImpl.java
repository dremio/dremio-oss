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
package com.dremio.service.users.events;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserServiceEventsImpl implements UserServiceEvents{
  private ConcurrentHashMap<UserServiceEventTopic, ConcurrentHashMap<Integer, WeakReference<UserServiceEventSubscriber>>> subscribers;

  public UserServiceEventsImpl() {
    this.subscribers = new ConcurrentHashMap<>();
  }
  @Override
  public void subscribe(UserServiceEventTopic userServiceEventTopic, UserServiceEventSubscriber subscriber) {
    if (!subscribers.containsKey(userServiceEventTopic)) {
      subscribers.put(userServiceEventTopic, new ConcurrentHashMap<>());
    }
    subscribers.get(userServiceEventTopic).put(subscriber.hashCode(), new WeakReference<>(subscriber));

  }

  @Override
  public void publish(UserServiceEvent event) {
    ConcurrentHashMap<Integer, WeakReference<UserServiceEventSubscriber>> map = subscribers.get(event.getTopic());
    if (map == null) {
      return;
    }

    for(Map.Entry<Integer, WeakReference<UserServiceEventSubscriber>> subscribers : map.entrySet()) {
      WeakReference<UserServiceEventSubscriber> subscriberRef = subscribers.getValue();
      UserServiceEventSubscriber subscriber = subscriberRef.get();
      subscriber.onUserServiceEvent(event);
    }
  }
}
