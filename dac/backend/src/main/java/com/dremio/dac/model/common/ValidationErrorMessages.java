/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * to return validation errors to the client
 */
public class ValidationErrorMessages {

  private final Map<String, List<String>> fieldErrorMessages;
  private final List<String> globalErrorMessages;

  @JsonCreator
  public ValidationErrorMessages(
      @JsonProperty("fieldErrorMessages") Map<String, List<String>> fieldErrorMessages,
      @JsonProperty("globalErrorMessages") List<String> globalErrorMessages) {
    super();
    this.fieldErrorMessages = checkNotNull(fieldErrorMessages);
    this.globalErrorMessages = checkNotNull(globalErrorMessages);
  }

  public ValidationErrorMessages() {
    fieldErrorMessages = new HashMap<>();
    globalErrorMessages = new ArrayList<>();
  }

  public void addFieldError(String field, String message) {
    List<String> messages = fieldErrorMessages.get(field);
    if (messages == null) {
      messages = new ArrayList<>();
      fieldErrorMessages.put(field, messages);
    }
    messages.add(message);
  }

  public void addGlobaldError(String message) {
    globalErrorMessages.add(message);
  }

  public Map<String, List<String>> getFieldErrorMessages() {
    return fieldErrorMessages;
  }

  public List<String> getGlobalErrorMessages() {
    return globalErrorMessages;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return fieldErrorMessages.isEmpty() && globalErrorMessages.isEmpty();
  }

}
