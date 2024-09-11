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

import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_KEY;

import com.google.common.base.Joiner;
import io.micrometer.core.instrument.Tag;

/**
 * A composite counter that creates a {@link SimpleCounter} and a {@link CounterWithOutcome} for the
 * same name and all other possible parameters. The {@link CounterWithOutcome}'s name is suffixed
 * with ".outcome". The {@link SimpleCounter} is used to count the total number of events, while the
 * {@link CounterWithOutcome} is used to count the number of successful and errored events.
 */
public final class TotalAndOutcomeCounters {
  private final SimpleCounter totalCounter;
  private final CounterWithOutcome outcomeCounter;

  public static TotalAndOutcomeCounters of(String name) {
    return new TotalAndOutcomeCounters(
        SimpleCounter.of(name), CounterWithOutcome.of(getNameForOutcomeCounter(name)));
  }

  public static TotalAndOutcomeCounters of(String name, Iterable<Tag> tags) {
    return new TotalAndOutcomeCounters(
        SimpleCounter.of(name, tags), CounterWithOutcome.of(getNameForOutcomeCounter(name), tags));
  }

  public static TotalAndOutcomeCounters of(String name, String description) {
    return new TotalAndOutcomeCounters(
        SimpleCounter.of(name, description),
        CounterWithOutcome.of(getNameForOutcomeCounter(name), description));
  }

  public static TotalAndOutcomeCounters of(String name, String description, Iterable<Tag> tags) {
    return new TotalAndOutcomeCounters(
        SimpleCounter.of(name, description, tags),
        CounterWithOutcome.of(getNameForOutcomeCounter(name), description, tags));
  }

  private TotalAndOutcomeCounters(SimpleCounter totalCounter, CounterWithOutcome outcomeCounter) {
    this.totalCounter = totalCounter;
    this.outcomeCounter = outcomeCounter;
  }

  public void incrementTotal() {
    totalCounter.increment();
  }

  public long countTotal() {
    return totalCounter.count();
  }

  public void succeeded() {
    outcomeCounter.succeeded();
  }

  public long countSucceeded() {
    return outcomeCounter.countSucceeded();
  }

  public void errored() {
    outcomeCounter.errored();
  }

  public long countErrored() {
    return outcomeCounter.countErrored();
  }

  public void userErrored() {
    outcomeCounter.userErrored();
  }

  public long countUserErrored() {
    return outcomeCounter.countUserErrored();
  }

  private static String getNameForOutcomeCounter(String name) {
    return Joiner.on('.').join(name, TAG_OUTCOME_KEY);
  }
}
