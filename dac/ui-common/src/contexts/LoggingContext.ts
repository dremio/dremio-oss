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

export type Logger = (prefix: string) => {
  debug: (message: string, context?: unknown) => void;
  info: (message: string, context?: unknown) => void;
  warn: (message: string, context?: unknown) => void;
  error: (message: string, context?: unknown) => void;
};

const handlers = new Set<Logger>();

const createLogger: Logger = (prefix) => {
  return {
    debug: (...args) => {
      handlers.forEach((handler) => {
        handler(prefix).debug(...args);
      });
    },
    info: (...args) => {
      handlers.forEach((handler) => {
        handler(prefix).info(...args);
      });
    },
    warn: (...args) => {
      handlers.forEach((handler) => {
        handler(prefix).warn(...args);
      });
    },
    error: (...args) => {
      handlers.forEach((handler) => {
        handler(prefix).error(...args);
      });
    },
  };
};

const loggingContext = {
  registerHandler: (handler: Logger): void => {
    handlers.add(handler);
  },
  createLogger,
} as const;

export const getLoggingContext = () => loggingContext;
