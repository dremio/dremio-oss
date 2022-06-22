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
import dayjs from "dayjs";

import relativeTime from "dayjs/plugin/relativeTime";
import customParseFormat from "dayjs/plugin/customParseFormat";
import duration from "dayjs/plugin/duration";
import isSameOrAfter from "dayjs/plugin/isSameOrAfter";
import isSameOrBefore from "dayjs/plugin/isSameOrBefore";
import isBetween from "dayjs/plugin/isBetween";
import localeData from "dayjs/plugin/localeData";
import utc from "dayjs/plugin/utc";
import isoWeek from "dayjs/plugin/isoWeek";
import LocalizedFormat from "dayjs/plugin/localizedFormat";
import timezone from "dayjs/plugin/timezone";

//Treat "" as invalid date, as momentJS does. This can be removed upon further testing
import formatOverridePlugin from "./dayjs-plugins/dayjs-format-override";

//Bad mutable for backwards compatability with bad moment mutations, may be able to remove but requires testing
import badMutable from "dayjs/plugin/badMutable";
dayjs.extend(badMutable);

dayjs.extend(relativeTime);
dayjs.extend(customParseFormat);
dayjs.extend(duration);
dayjs.extend(isSameOrAfter);
dayjs.extend(isSameOrBefore);
dayjs.extend(isBetween);
dayjs.extend(localeData);
dayjs.extend(utc);
dayjs.extend(isoWeek);
dayjs.extend(LocalizedFormat);
dayjs.extend(timezone);
dayjs.extend(formatOverridePlugin);

export default dayjs;
