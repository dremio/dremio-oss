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
package com.dremio.resource.basic;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/**
 * Specific constants needed to manage basic resources
 */
@Options
public interface BasicResourceConstants {
  TypeValidators.BooleanValidator ENABLE_QUEUE_MEMORY_LIMIT = new TypeValidators.BooleanValidator("exec.queue.memory.enable", true);
  TypeValidators.LongValidator LARGE_QUEUE_MEMORY_LIMIT = new TypeValidators.RangeLongValidator("exec.queue.memory.large", 0, Long.MAX_VALUE, 0);
  TypeValidators.LongValidator SMALL_QUEUE_MEMORY_LIMIT = new TypeValidators.RangeLongValidator("exec.queue.memory.small", 0, Long.MAX_VALUE, 0);
  TypeValidators.LongValidator QUEUE_TIMEOUT = new TypeValidators.PositiveLongValidator("exec.queue.timeout_millis",
    Long.MAX_VALUE, 60 * 1000 * 5);
  // 24 hour timeout for reflection jobs to enter queue.
  // This will enable reflections to wait for longer running reflections to finish and enter queue.
  TypeValidators.LongValidator REFLECTION_QUEUE_TIMEOUT = new TypeValidators.PositiveLongValidator("reflection.queue.timeout_millis",
    Long.MAX_VALUE, 24 * 60 * 60 * 1000 );
  TypeValidators.BooleanValidator ENABLE_QUEUE = new TypeValidators.BooleanValidator("exec.queue.enable", true);
  TypeValidators.BooleanValidator REFLECTION_ENABLE_QUEUE = new TypeValidators.BooleanValidator("reflection.queue.enable", true);
  TypeValidators.LongValidator LARGE_QUEUE_SIZE = new TypeValidators.PositiveLongValidator("exec.queue.large", 1000, 10);
  TypeValidators.LongValidator SMALL_QUEUE_SIZE = new TypeValidators.PositiveLongValidator("exec.queue.small", 100000, 100);
  TypeValidators.LongValidator REFLECTION_LARGE_QUEUE_SIZE = new TypeValidators.RangeLongValidator("reflection.queue.large", 0, 100, 1);
  TypeValidators.LongValidator REFLECTION_SMALL_QUEUE_SIZE = new TypeValidators.RangeLongValidator("reflection.queue.small", 0, 10000, 10);
  TypeValidators.LongValidator QUEUE_THRESHOLD_SIZE = new TypeValidators.PositiveLongValidator("exec.queue.threshold", Long.MAX_VALUE, 30000000);
  TypeValidators.BooleanValidator QUEUE_WAIT_SLICING = new TypeValidators.BooleanValidator("exec.queue.sliced_wait", true);
}
