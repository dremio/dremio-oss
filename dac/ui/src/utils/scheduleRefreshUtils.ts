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

import { cronParser } from "dremio-ui-common/utilities/cron.js";
import { z } from "zod";

export const ScheduleRefreshWeekLabels: any = {
  "1": "Sunday",
  "2": "Monday",
  "3": "Tuesday",
  "4": "Wednesday",
  "5": "Thursday",
  "6": "Friday",
  "7": "Saturday",
};

export const DaysOfWeek: any = [
  ["1", "S"],
  ["2", "M"],
  ["3", "T"],
  ["4", "W"],
  ["5", "T"],
  ["6", "F"],
  ["7", "S"],
];

const dateFormat = new Intl.DateTimeFormat("en", {
  year: "numeric",
  month: "short",
  day: "numeric",
});

/*
 * DAY: Get next run when Day is selected
 */
export const getNextRunFromEveryday = () => {
  const today = new Date();
  const nextDayFromEveryday = today.setDate(today.getDate() + 1);
  return dateFormat.format(nextDayFromEveryday);
};

/*
 * WEEK: Get next run when Day is selected
 */
export const getNextRunFromWeek = (time: Date, sortedWeek: string[]) => {
  const today = new Date();
  const todayDay = today.getDay() + 1 + "";
  const dayDifference =
    Number(getNextDayFromWeek(sortedWeek, todayDay, today, time)) -
    Number(todayDay);
  const nextDayFromWeek = today.setDate(
    today.getDate() + getAdditionToDayOfWeek(today, time, dayDifference),
  );
  return dateFormat.format(nextDayFromWeek);
};

// Return closest rightmost day to the current day
const getNextDayFromWeek = (
  sortedWeek: string[],
  todayDay: string,
  today: Date,
  time: Date,
) => {
  let a = sortedWeek[0];
  for (const i in sortedWeek) {
    if (sortedWeek[i] > todayDay) {
      a = sortedWeek[i];
      break;
    } else if (sortedWeek[i] === todayDay) {
      if (today.getTime() < time.getTime()) {
        a = sortedWeek[i];
        break;
      }
    }
  }
  return a;
};

// Get the right number of days to add to current date
const getAdditionToDayOfWeek = (
  today: Date,
  time: Date,
  additionToDayOfWeek: number,
) => {
  if (additionToDayOfWeek < 0) {
    // Need to add seven days for week overflow
    return additionToDayOfWeek + 7;
  } else if (additionToDayOfWeek > 0) {
    // Use difference if there is no overflow
    return additionToDayOfWeek;
  } else {
    // If it lands on same day, need to check times
    if (today.getTime() > time.getTime()) return 7;
    else return 0;
  }
};

export const getMaxDistanceOfDays = (cron: string) => {
  const { weekInput } = cronParser(cron);
  const daysOfWeek = weekInput.map((day) => Number(day));

  if (daysOfWeek.length === 1) return 7;

  let longestGap = 0;
  for (let i = 1; i < daysOfWeek.length; i++) {
    const currentGap = daysOfWeek[i] - daysOfWeek[i - 1];
    longestGap = Math.max(longestGap, currentGap);
  }

  longestGap = Math.max(
    longestGap,
    7 - daysOfWeek[daysOfWeek.length - 1] + daysOfWeek[0],
  );
  return longestGap;
};

export const validationSchema = z.object({
  accelerationRefreshSchedule: z
    .object({
      scheduleType: z.string(),
      weekInput: z.string().array(),
    })
    .partial()
    .superRefine((data, ctx) => {
      if (
        data.scheduleType === "@week" &&
        data.weekInput &&
        data.weekInput.length < 1
      ) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["weekInput"],
          message: "Must pick a minimum of one day",
        });
      }
    }),
});
