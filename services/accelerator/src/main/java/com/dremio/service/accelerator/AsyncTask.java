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
package com.dremio.service.accelerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.DeferredException;
import com.google.common.base.Throwables;

/**
 * An abstract async task that logs and holds on to exceptions.
 *
 */
public abstract class AsyncTask implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final DeferredException exception = new DeferredException();

  @Override
  public final void run() {
    try {
      doRun();
    } catch (final Exception ex) {
      logger.error("failed to execute task", ex);
      exception.addException(ex);
      Throwables.propagateIfPossible(ex);
    }
  }

  public DeferredException getException() {
    return exception;
  }

  protected abstract void doRun();

}
