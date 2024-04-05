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

package com.dremio.telemetry.api.metrics;

import io.micrometer.core.instrument.Tag;

public class CommonTags {
  public static final String TAG_RESULT_VALUE_SUCCESS = "success";
  public static final String TAG_RESULT_VALUE_FAILURE = "failure";
  public static final String TAG_RESULT_KEY = "result";

  public static final Tag TAG_RESULT_SUCCESS = Tag.of(TAG_RESULT_KEY, TAG_RESULT_VALUE_SUCCESS);
  public static final Tag TAG_RESULT_FAILURE = Tag.of(TAG_RESULT_KEY, TAG_RESULT_VALUE_FAILURE);
}
