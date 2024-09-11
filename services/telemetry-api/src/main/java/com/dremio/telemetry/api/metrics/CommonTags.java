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
import io.micrometer.core.instrument.Tags;

/** Common tags used in Dremio metrics. */
public final class CommonTags {
  /** The value for the outcome tag indicating success. */
  public static final String TAG_OUTCOME_VALUE_SUCCESS = "success";

  /** The value for the outcome tag indicating error. */
  public static final String TAG_OUTCOME_VALUE_ERROR = "error";

  /** The value for the outcome tag indicating user error. */
  public static final String TAG_OUTCOME_VALUE_USER_ERROR = "user_error";

  /** The value for the exception tag indicating no exception. */
  public static final String TAG_EXCEPTION_VALUE_NONE = "none";

  /** The key for the outcome tag. */
  public static final String TAG_OUTCOME_KEY = "outcome";

  /** The key for the exception tag. */
  public static final String TAG_EXCEPTION_KEY = "exception";

  /** The outcome tag indicating success. */
  public static final Tag TAG_OUTCOME_SUCCESS = Tag.of(TAG_OUTCOME_KEY, TAG_OUTCOME_VALUE_SUCCESS);

  /** The outcome tag indicating error. */
  public static final Tag TAG_OUTCOME_ERROR = Tag.of(TAG_OUTCOME_KEY, TAG_OUTCOME_VALUE_ERROR);

  /** The outcome tag indicating user error. */
  public static final Tag TAG_OUTCOME_USER_ERROR =
      Tag.of(TAG_OUTCOME_KEY, TAG_OUTCOME_VALUE_USER_ERROR);

  /** The exception tag indicating no exception. */
  public static final Tag TAG_EXCEPTION_NONE = Tag.of(TAG_EXCEPTION_KEY, TAG_EXCEPTION_VALUE_NONE);

  /** The 1-element collection of tags for the outcome tag indicating success. */
  public static final Tags TAGS_OUTCOME_SUCCESS = Tags.of(TAG_OUTCOME_SUCCESS);

  /** The 1-element collection of tags for the outcome tag indicating error. */
  public static final Tags TAGS_OUTCOME_ERROR = Tags.of(TAG_OUTCOME_ERROR);

  /** The 1-element collection of tags for the outcome tag indicating user error. */
  public static final Tags TAGS_OUTCOME_USER_ERROR = Tags.of(TAG_OUTCOME_USER_ERROR);

  private CommonTags() {
    // private constructor to prevent instantiation
  }

  public static Tag tagException(Throwable exception) {
    return Tag.of(TAG_EXCEPTION_KEY, exception.getClass().getSimpleName());
  }
}
