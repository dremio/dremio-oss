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
package com.dremio.service.autocomplete.functionlist2;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@Value.Style(stagedBuilder = true)
@JsonSerialize(as = ImmutableSampleCode.class)
@JsonDeserialize(as = ImmutableSampleCode.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class SampleCode {
  public abstract String getCall();
  public abstract String getResult();

  public static ImmutableSampleCode.CallBuildStage builder() {
    return ImmutableSampleCode.builder();
  }

  public static SampleCode create(String sampleCall, String sampleReturn) {
    return builder()
      .call(sampleCall)
      .result(sampleReturn)
      .build();
  }
}
