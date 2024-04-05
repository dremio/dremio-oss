/* eslint-disable no-console */
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

import moize from "moize";
import { isDev } from "./isDev";

import { type Logger } from "../contexts/LoggingContext";

const prefixColors = [
  "#f99157",
  "#ffcc66",
  "#99cc99",
  "#66cccc",
  "#6699cc",
  "cc99cc",
] as const;

const getPrefixColor = (() => {
  let next = 0;

  const incrementNext = () => {
    next++;
    if (next >= prefixColors.length) {
      next = 0;
    }
  };

  return moize(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    (_prefix: string): string => {
      const color = prefixColors[next];
      incrementNext();
      return color;
    },
    { maxSize: 50 },
  );
})();

const getPrefixArgs = (prefix: string, prefixColor: string) => [
  `%c${prefix}`,
  `color: ${prefixColor}; font-weight: bold;`,
];

export const consoleLogger: Logger = (prefix) => {
  if (!isDev) {
    return {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {},
    };
  }

  const prefixArgs = getPrefixArgs(prefix, getPrefixColor(prefix));

  return {
    debug: console.debug.bind(window.console, ...prefixArgs) as any,
    info: console.info.bind(window.console, ...prefixArgs) as any,
    warn: console.warn.bind(window.console, ...prefixArgs) as any,
    error: console.error.bind(window.console, ...prefixArgs) as any,
  };
};
