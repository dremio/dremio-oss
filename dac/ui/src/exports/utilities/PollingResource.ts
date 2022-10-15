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

import { SmartResource, type ResourceOptions } from "smart-resource";

type Fetcher<T> = (...args: any) => T;

type Options<T> = Partial<ResourceOptions<T>> & {
  pollingInterval: number;
  stopAfterErrors?: number;
};

export class PollingResource<T> extends SmartResource<T> {
  private _errorCount = 0;
  private _getFetcherArgs: (() => any[]) | null = null;
  private _pollingInterval: number;
  private _stopAfterErrors: number;
  private _timer: number | null = null;

  constructor(fetcher: Fetcher<T>, options: Options<T>) {
    super(fetcher, options);
    this._pollingInterval = options.pollingInterval;
    this._stopAfterErrors = options.stopAfterErrors || 3;
  }

  private _startTimer() {
    this._timer = window.setTimeout(() => {
      this.fetch(...(this._getFetcherArgs?.() || []));
    }, this._pollingInterval);
  }

  private _clearTimer(): void {
    if (this._timer) {
      clearTimeout(this._timer);
    }

    this._cancelPromises(this._queued.splice(0, this._queued.length));
  }

  /**
   * Starts (or restarts) polling with a new argument fetcher function
   */
  start(getArgs: () => any[] = () => []): void {
    this._errorCount = 0;
    this._getFetcherArgs = getArgs;
    this._clearTimer();
    this._startTimer();
  }

  stop(): void {
    this._clearTimer();
    this._getFetcherArgs = null;
  }

  /**
   * Returns true when the resource has paused/stopped due to too many errors
   */
  isFailed() {
    return this._errorCount >= this._stopAfterErrors;
  }

  protected _next(value: Awaited<T>) {
    super._next(value);
    this._startTimer();
  }

  protected _error(err: any) {
    super._error(err);
    this._errorCount++;
    if (!this.isFailed()) {
      this._startTimer();
    }
  }
}
