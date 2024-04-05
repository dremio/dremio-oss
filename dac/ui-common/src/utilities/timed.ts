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

import { Logger, getLoggingContext } from "../contexts/LoggingContext";

let _LOGGER: ReturnType<Logger>;
const getLogger = () => {
  if (typeof _LOGGER === "undefined") {
    _LOGGER = getLoggingContext().createLogger("Timed");
  }
  return _LOGGER;
};

function isPromise(obj: any): boolean {
  return obj != null && typeof obj.then === "function";
}

export function timed(
  message: string,
  logLevel?: "debug" | "info" | "warn" | "error",
) {
  return function (
    _target: any,
    _name: string,
    descriptor: PropertyDescriptor,
  ) {
    const logger = getLogger();
    const func = descriptor.value;
    descriptor.value = function (...args: any[]) {
      if (!logLevel) {
        logLevel = "debug";
      }
      const startTime = Date.now();
      const result = func.apply(this, args);
      if (isPromise(result)) {
        throw new Error(
          "Must use timedAsync for functions that return promises",
        );
      }
      const endTime = Date.now();
      logger[logLevel](`${message} took ${endTime - startTime}ms to complete.`);
      return result;
    };
  };
}

export function timedAsync(
  message: string,
  logLevel?: "debug" | "info" | "warn" | "error",
) {
  return function (
    _target: any,
    _name: string,
    descriptor: PropertyDescriptor,
  ) {
    const logger = getLogger();
    const func = descriptor.value;
    descriptor.value = async function (...args: any[]) {
      if (!logLevel) {
        logLevel = "debug";
      }
      const startTime = Date.now();
      const resultPromise = func.apply(this, args);
      if (!isPromise(resultPromise)) {
        throw new Error(
          "Must use timed for functions that do not return promises",
        );
      }
      const result = await resultPromise;
      const endTime = Date.now();
      logger[logLevel](`${message} took ${endTime - startTime}ms to complete.`);
      return result;
    };
  };
}
