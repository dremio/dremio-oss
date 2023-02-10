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
package com.dremio.exec.planner.serializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.google.protobuf.Message;

/**
 * Tools to retrieve information from Proto classes.
 */
public class MessageTools {

  public static Message getDefaultInstance(Class<? extends Message> clazz) {
    for(Method m : clazz.getDeclaredMethods()) {
      if(Modifier.isStatic(m.getModifiers()) && "getDefaultInstance".equals(m.getName())) {
        try {
          return (Message) m.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    }
    throw new IllegalStateException(String.format("unable to find getDefaultInstance() for class %s.", clazz.getName()));
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<? extends T> getClassArg(RelNodeSerde<?, ?> instance, Class<T> clazz, int index){

    for(Type c : instance.getClass().getGenericInterfaces()) {
      if(c instanceof ParameterizedType && ((ParameterizedType) c).getRawType() == RelNodeSerde.class) {
        return (Class<? extends T>) ((ParameterizedType) c).getActualTypeArguments()[index];
      }
    }
    throw new IllegalStateException(String.format("unable to find interface: %s", clazz.getName()));
  }

}
