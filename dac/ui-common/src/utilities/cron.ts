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

const daysInWeek = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
const numbersInWeek = ["1", "2", "3", "4", "5", "6", "7"];

// Parse the cron for the time and day of week
export const cronParser = (cron: string) => {
  let scheduleType;
  const cronValues = cron.split(" ");

  if (cronValues.length === 5) cronValues.unshift("0"); // convert Unix cron to Spring cron
  let [, cronMinute, cronHour, , , cronDayOfWeek] = cronValues;

  if (cronDayOfWeek !== "*" && cronDayOfWeek !== "?") {
    scheduleType = "@week";
  } else {
    scheduleType = "@day";
  }

  const regExp = /[a-zA-Z]/g;
  if (regExp.test(cronDayOfWeek)) {
    daysInWeek.forEach(
      (day, i) => (cronDayOfWeek = cronDayOfWeek.replace(day, `${i + 1}`)),
    );
  }

  let weekInput = ["2"];
  if (cronDayOfWeek.length === 1) {
    if (cronDayOfWeek === "*" || cronDayOfWeek === "?") {
      weekInput = numbersInWeek;
    } else {
      weekInput = [cronDayOfWeek];
    }
  } else {
    if (cronDayOfWeek.includes(",")) {
      weekInput = cronDayOfWeek.split(",");
    } else if (cronDayOfWeek.includes("-")) {
      const [startDay, endDay] = cronDayOfWeek.split("-");
      if (startDay < endDay) {
        weekInput = numbersInWeek.filter(
          (day) => day <= endDay && day >= startDay,
        );
      } else {
        weekInput = numbersInWeek.filter(
          (day) => day <= endDay || day >= startDay,
        );
      }
    }
  }

  const timeInput = new Date();
  if (cronHour.length > 2) {
    timeInput.setHours(0);
  } else {
    timeInput.setHours(Number(cronHour));
  }

  if (cronMinute !== "*") {
    timeInput.setMinutes(Number(cronMinute));
  } else {
    timeInput.setMinutes(0);
  }

  return {
    timeInput,
    weekInput,
    scheduleType,
  };
};

type CronGeneratorProps = {
  minute: number;
  hour: number;
  weekValues: string[];
  scheduleType: "@day" | "@week";
};

// Generate the cron, evaluating the time and day of week
export const cronGenerator = (
  options: CronGeneratorProps,
  isUnixCron?: boolean,
) => {
  const { minute, hour, weekValues, scheduleType } = options;

  const seconds = isUnixCron ? "" : "0 "; // Unix cron doesn't have seconds value, Spring cron does- https://stackoverflow.com/a/57426242

  if (scheduleType === "@day") {
    return `${seconds}${minute} ${hour} * * *`;
  }

  if (scheduleType === "@week") {
    const weekdays = weekValues && weekValues.join();
    const star = "*";
    const value = weekdays ? weekdays : star;
    return `${seconds}${minute} ${hour} * * ${value}`;
  }

  return `${seconds}* * * * *`;
};
