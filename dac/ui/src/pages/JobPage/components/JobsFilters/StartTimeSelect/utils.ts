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
import Immutable from "immutable";
import moment from "@app/utils/dayjs";

import * as IntervalTypes from "./IntervalTypes";

const defaultStartTime = new Date(2015, 0).getTime();
export const Intervals = () =>
  Object.freeze([
    {
      type: IntervalTypes.LAST_HOUR_INTERVAL,
      label: `Last Hour`,
      time: [moment().subtract(1, "h"), moment()],
      dataQa: "lastHours",
    },
    {
      type: IntervalTypes.LAST_6_HOURS_INTERVAL,
      label: `Last 6 Hours`,
      time: [moment().subtract(6, "h"), moment()],
    },
    {
      type: IntervalTypes.LAST_1_DAY_INTERVAL,
      label: `Last 24 Hours`,
      time: [moment().subtract(1, "d"), moment()],
    },
    {
      type: IntervalTypes.SEPARATOR,
    },
    {
      type: IntervalTypes.LAST_3_DAYS_INTERVAL,
      label: `Last 3 Days`,
      time: [moment().subtract(3, "d"), moment()],
    },
    {
      type: IntervalTypes.LAST_7_DAYS_INTERVAL,
      label: `Last 7 Days`,
      time: [moment().subtract(7, "d"), moment()],
    },
    {
      type: IntervalTypes.LAST_30_DAYS_INTERVAL,
      label: `Last 30 Days`,
      time: [moment().subtract(30, "d"), moment()],
    },
    {
      type: IntervalTypes.LAST_90_DAYS_INTERVAL,
      label: `Last 90 Days`,
      time: [moment().subtract(90, "d"), moment()],
    },
    {
      type: IntervalTypes.SEPARATOR,
    },
    {
      type: IntervalTypes.YEAR_TO_DATE_INTERVAL,
      label: "Year to Date",
      time: [moment().startOf("year"), moment()],
    },
    {
      type: IntervalTypes.ALL_TIME_INTERVAL,
      label: "All",
      time: [moment(defaultStartTime), moment()], // start from 2015
    },
    {
      type: IntervalTypes.SEPARATOR,
    },
    {
      type: IntervalTypes.CUSTOM_INTERVAL,
      label: "Custom",
      time: [moment(), moment()],
    },
  ]);

export function getIntervals() {
  return Immutable.fromJS(Intervals().filter((cur) => cur.time !== undefined));
}
