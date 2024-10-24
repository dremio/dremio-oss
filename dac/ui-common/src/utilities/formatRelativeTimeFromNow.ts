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

import parseMilliseconds from "parse-ms";

export const formatRelativeTimeFromNow = (date: Date): string => {
  const parsed = parseMilliseconds(Date.now() - date.getTime());
  if (parsed.days > 0) {
    return new Intl.RelativeTimeFormat("default").format(
      parsed.days * -1,
      "days",
    );
  }
  if (parsed.hours > 0) {
    return new Intl.RelativeTimeFormat("default").format(
      parsed.hours * -1,
      "hours",
    );
  }
  if (parsed.minutes > 0) {
    return new Intl.RelativeTimeFormat("default").format(
      parsed.minutes * -1,
      "minutes",
    );
  }
  if (parsed.seconds > 0) {
    return new Intl.RelativeTimeFormat("default").format(
      parsed.seconds * -1,
      "seconds",
    );
  }
  return "just now";
};
