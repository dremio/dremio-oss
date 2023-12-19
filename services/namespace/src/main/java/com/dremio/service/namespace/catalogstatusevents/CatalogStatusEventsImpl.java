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
package com.dremio.service.namespace.catalogstatusevents;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogStatusEventsImpl implements CatalogStatusEvents {
  private ConcurrentHashMap<CatalogStatusEventTopic, ConcurrentHashMap<Integer, WeakReference<CatalogStatusSubscriber>>> topics;
  public CatalogStatusEventsImpl() {
    this.topics = new ConcurrentHashMap<>();
  }

  @Override
  public void subscribe(CatalogStatusEventTopic catalogStatusEventTopic, CatalogStatusSubscriber subscriber) {
    if (!topics.containsKey(catalogStatusEventTopic)) {
      topics.put(catalogStatusEventTopic, new ConcurrentHashMap<>());
    }
    topics.get(catalogStatusEventTopic).put(subscriber.hashCode(), new WeakReference<>(subscriber));
  }

  @Override
  public void publish(CatalogStatusEvent event) {
    ConcurrentHashMap<Integer, WeakReference<CatalogStatusSubscriber>> map = topics.get(event.getTopic());
    if (map == null) {
      return;
    }

    for(Map.Entry<Integer, WeakReference<CatalogStatusSubscriber>> subscribers : map.entrySet()) {
      WeakReference<CatalogStatusSubscriber> subscriberRef = subscribers.getValue();
      CatalogStatusSubscriber subscriber = subscriberRef.get();
      subscriber.onCatalogStatusEvent(event);
    }
  }
}
