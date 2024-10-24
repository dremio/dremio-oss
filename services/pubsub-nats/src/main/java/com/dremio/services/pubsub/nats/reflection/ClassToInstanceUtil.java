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
package com.dremio.services.pubsub.nats.reflection;

import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.lang3.reflect.ConstructorUtils;

public final class ClassToInstanceUtil {

  private ClassToInstanceUtil() {}

  public static <M extends Message> Topic<M> toTopicInstance(Class<? extends Topic<M>> topicClass) {
    Topic<M> topic;
    try {
      topic = ConstructorUtils.invokeConstructor(topicClass);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }
    return topic;
  }

  public static <M extends Message> Subscription<M> toSubscriptionInstance(
      Class<? extends Subscription<M>> subscriptionClass) {
    Subscription<M> subscription;
    try {
      subscription = ConstructorUtils.invokeConstructor(subscriptionClass);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }
    return subscription;
  }
}
