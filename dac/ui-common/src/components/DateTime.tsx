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
import type { FC } from "react";

const supportedFormats = {
  /**
   * Used when tabular alignment / consistent line length is desired,
   * for example when used in a table column.
   * '01/01/2024, 02:00 PM'
   */
  tabular: {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  },

  /**
   * Used when maximum detail is desired, for example showing a precise
   * time on job details page.
   * '01/01/2024, 2:00:00.00 PM PDT'
   */
  technical: {
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    second: "numeric",
    //@ts-ignore
    fractionalSecondDigits: 2,
    timeZoneName: "short",
  },

  /**
   * Provides simple and easy to read contextual information such
   * as the weekday, timezone, etc.
   * 'Monday, January 1, 2024 at 2:00 PM PDT'
   */
  pretty: {
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    weekday: "long",
    timeZoneName: "short",
  },
} as const satisfies Record<string, Intl.DateTimeFormatOptions>;

export const DateTimeFormat: FC<{
  date: Date;
  options: Intl.DateTimeFormatOptions;
}> = (props) => (
  <time
    dateTime={props.date.toISOString()}
    title={Intl.DateTimeFormat("default", supportedFormats["pretty"]).format(
      props.date,
    )}
  >
    {Intl.DateTimeFormat("default", props.options).format(props.date)}
  </time>
);

export const DateTime: FC<{
  date: Date;
  format: keyof typeof supportedFormats;
}> = (props) => (
  <DateTimeFormat date={props.date} options={supportedFormats[props.format]} />
);
