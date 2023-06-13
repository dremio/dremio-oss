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
import { CronosExpression } from "cronosjs";
import { formatFixedDateTimeLong } from "@app/exports/utilities/formatDate";
import {
  getInitialValuesObjectResult,
  getInitialValuesResult,
} from "./Optimization.types";

type cronGeneratorProps = {
  everyDayAt: number;
  minute: number;
  hour: number;
  weekValues: string[];
  dayOfMonth: string | number;
  ordinalOfEveryMonth: string;
  weekdayOfEveryMonth: string;
  scheduleType: string;
  monthScheduleOption: string;
};

export const cronGenerator = (options: cronGeneratorProps) => {
  const {
    minute,
    hour,
    everyDayAt,
    weekValues,
    dayOfMonth,
    ordinalOfEveryMonth,
    weekdayOfEveryMonth,
    scheduleType,
    monthScheduleOption,
  } = options;

  if (scheduleType === "@hours") {
    return `0 */${everyDayAt} * * *`;
  }

  if (scheduleType === "@day") {
    return `${minute} ${hour} * * *`;
  }

  if (scheduleType === "@week") {
    const weekdays = weekValues && weekValues.join();
    const star = "*";
    const value = weekdays ? weekdays : star;
    return `${minute} ${hour} * * ${value}`;
  }

  if (scheduleType === "@month" && monthScheduleOption === "first") {
    return `${minute} ${hour} ${dayOfMonth} * *`;
  }

  if (scheduleType === "@month" && monthScheduleOption === "second") {
    return `${minute} ${hour} * * ${weekdayOfEveryMonth}#${ordinalOfEveryMonth}`;
  }

  return "* * * * *";
};

export const cronParser = (cron: string) => {
  let scheduleType, monthScheduleOption;
  const cronValues = cron.split(" ");
  const [cronMinute, cronHour, cronDay, cronMonth, cronOfEveryMonth] =
    cronValues;

  if (cronDay !== "*" && cronOfEveryMonth === "*") {
    monthScheduleOption = "first";
  }

  if (cronOfEveryMonth.indexOf("#") === 1) {
    monthScheduleOption = "second";
  }

  if (monthScheduleOption) {
    scheduleType = "@month";
  } else if (
    cronOfEveryMonth !== "*" &&
    (cronOfEveryMonth.indexOf(",") !== -1 || cronOfEveryMonth.length === 1)
  ) {
    scheduleType = "@week";
  } else if (cronHour.length > 2) {
    scheduleType = "@hours";
  } else {
    scheduleType = "@day";
  }

  return {
    cronMinute,
    cronHour,
    cronDay,
    cronMonth,
    cronOfEveryMonth,
    scheduleType,
    monthScheduleOption,
  };
};

export const transformAdvancedConfigToString = (value: number): string => {
  return `${value} MB`;
};

export const transformAdvancedConfigToNumber = (value: string): number => {
  return Number(value.split(" ")[0]);
};

export const getNextCompactionDate = (
  cronValue: string,
  scheduleType?: string
) => {
  const splitCron = cronValue.split(" ");
  const cronDay = splitCron[2];
  const cronWeek = splitCron[4];

  if (
    (scheduleType === "@week" && cronWeek === "*") ||
    cronDay == "0" ||
    Number(cronDay) > 30 ||
    (cronDay && cronDay.indexOf(",") !== -1)
  ) {
    return "-";
  }

  const nextDate = CronosExpression.parse(cronValue).nextDate();

  const formattedNextDate = nextDate && formatFixedDateTimeLong(nextDate);

  return formattedNextDate;
};

const getInitialValuesObject = (
  scheduleType: string,
  weekInput: string[],
  monthScheduleOption: string,
  dayOfMonth: number,
  ordinalOfEveryMonth: string,
  weekdayOfEveryMonth: string
): getInitialValuesObjectResult => {
  return {
    scheduleType,
    weekInput,
    dayOfMonth,
    monthScheduleOption,
    ordinalOfEveryMonth,
    weekdayOfEveryMonth,
  };
};

export const getInitialValues = (
  cron: string,
  advancedConfig: {
    maxFileSize: string;
    minFileSize: string;
    minFiles: number;
    targetFileSize: string;
  }
): getInitialValuesResult => {
  let initialValues;
  let hourInput = 1;
  const timeInput = new Date();
  const {
    cronMinute,
    cronHour,
    cronDay,
    cronOfEveryMonth,
    scheduleType,
    monthScheduleOption,
  } = cronParser(cron);
  const { maxFileSize, minFileSize, minFiles, targetFileSize } = advancedConfig;

  if (scheduleType === "@day" || scheduleType === "@hours") {
    initialValues = getInitialValuesObject(
      scheduleType,
      ["1"],
      "first",
      1,
      "1",
      "0"
    );
  }

  if (scheduleType === "@week") {
    const weekDays =
      cronOfEveryMonth.length > 1
        ? cronOfEveryMonth.split(",")
        : [cronOfEveryMonth];
    initialValues = getInitialValuesObject(
      scheduleType,
      weekDays,
      "first",
      1,
      "1",
      "0"
    );
  }

  if (monthScheduleOption === "first" && scheduleType === "@month") {
    initialValues = getInitialValuesObject(
      scheduleType,
      ["1"],
      monthScheduleOption,
      Number(cronDay),
      "1",
      "0"
    );
  }

  if (monthScheduleOption === "second" && scheduleType === "@month") {
    initialValues = getInitialValuesObject(
      scheduleType,
      ["1"],
      monthScheduleOption,
      1,
      cronOfEveryMonth[2],
      cronOfEveryMonth[0]
    );
  }

  if (cronHour.length > 2) {
    timeInput.setHours(0);
    hourInput = Number(1);
  } else {
    timeInput.setHours(Number(cronHour));
  }

  if (cronMinute !== "*") {
    timeInput.setMinutes(Number(cronMinute));
  } else {
    timeInput.setMinutes(0);
  }

  return {
    //@ts-ignore
    schedule: {
      ...initialValues,
      hourInput,
      timeInput,
    },
    advancedConfig: {
      maxFileSize: transformAdvancedConfigToNumber(maxFileSize),
      minFileSize: transformAdvancedConfigToNumber(minFileSize),
      minFiles,
      targetFileSize: transformAdvancedConfigToNumber(targetFileSize),
    },
  };
};
