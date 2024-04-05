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

import { Select } from "@mantine/core";
import { Controller, useForm, FormProvider } from "react-hook-form";
import { intl } from "@app/utils/intl";

import { TimeInput } from "@app/components/Schedule/TimeInput/TimeInput";
import { WeekInput } from "@app/components/Schedule/WeekInput/WeekInput";
import { cronParser, cronGenerator } from "dremio-ui-common/utilities/cron.js";
import { Label } from "dremio-ui-lib/components";
import {
  DaysOfWeek,
  ScheduleRefreshWeekLabels,
  getNextRunFromEveryday,
  getNextRunFromWeek,
  validationSchema,
} from "@app/utils/scheduleRefreshUtils";
import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect } from "react";

enum ScheduleRefreshOptionsEnum {
  DAY = "@day",
  WEEK = "@week",
}
type ScheduleRefreshOptions =
  | ScheduleRefreshOptionsEnum.DAY
  | ScheduleRefreshOptionsEnum.WEEK;

const options = [
  { value: ScheduleRefreshOptionsEnum.DAY, label: "day" },
  { value: ScheduleRefreshOptionsEnum.WEEK, label: "week" },
];

const shortTimeFormat = new Intl.DateTimeFormat("en", {
  timeStyle: "short",
});

type ScheduleRefreshProps = {
  accelerationRefreshSchedule: any;
};

export const ScheduleRefresh = ({
  accelerationRefreshSchedule,
}: ScheduleRefreshProps) => {
  const defaultValues = cronParser(accelerationRefreshSchedule.initialValue);
  const methods = useForm<any>({
    mode: "onChange",
    resolver: zodResolver(validationSchema),
    defaultValues: {
      accelerationRefreshSchedule: {
        scheduleType: defaultValues.scheduleType,
        timeInput: defaultValues.timeInput,
        weekInput: defaultValues.weekInput,
      },
    },
  });

  // Watch form values
  const { watch } = methods;
  const scheduleType: ScheduleRefreshOptions = watch(
    "accelerationRefreshSchedule.scheduleType",
  );
  const week: string[] = watch("accelerationRefreshSchedule.weekInput");
  const time: Date = watch("accelerationRefreshSchedule.timeInput");

  // Update accelerationRefreshSchedule when values change
  useEffect(() => {
    const generatedCron = cronGenerator({
      minute: time.getMinutes(),
      hour: time.getHours(),
      weekValues: week,
      scheduleType: scheduleType,
    });
    accelerationRefreshSchedule.onChange(generatedCron);
  }, [scheduleType, week, time, accelerationRefreshSchedule]);

  // Get schedule values
  const sortedWeek = week.sort();
  const runTime = shortTimeFormat.format(time);
  const nextRun =
    scheduleType === ScheduleRefreshOptionsEnum.DAY
      ? getNextRunFromEveryday()
      : getNextRunFromWeek(time, sortedWeek);

  return (
    <FormProvider {...methods}>
      <div>
        <p className="pb-1 pt-1 color-faded text-sm">
          {intl.formatMessage({ id: "Reflection.Refresh.WillRun" })}{" "}
          <b>
            {intl.formatMessage({ id: "Common.Every" }).toLowerCase?.()}{" "}
            {sortedWeek.length > 0
              ? `${
                  scheduleType === ScheduleRefreshOptionsEnum.DAY
                    ? "day"
                    : sortedWeek
                        .map((d: string) => ScheduleRefreshWeekLabels[d])
                        .join(", ")
                }`
              : `--`}
            , {intl.formatMessage({ id: "Common.At" }).toLowerCase?.()}{" "}
            {runTime}
          </b>
          . {intl.formatMessage({ id: "Reflection.Refresh.NextScheduleJob" })}{" "}
          <b>
            {sortedWeek.length > 0 ? nextRun : "--"}, {runTime}.
          </b>
        </p>
      </div>
      <Controller
        name="accelerationRefreshSchedule.scheduleType"
        render={({ field, fieldState: { error } }) => {
          return (
            <div className="flex">
              <Label
                value={
                  <p className="pr-1">
                    {intl.formatMessage({ id: "Common.Every" })}
                  </p>
                }
              />
              <Select
                {...field}
                data={options}
                rightSection={<dremio-icon name="interface/caretDown" alt="" />}
                error={error?.message}
              />
            </div>
          );
        }}
      />
      {scheduleType === ScheduleRefreshOptionsEnum.WEEK && (
        <WeekInput
          daysOfWeekProp={DaysOfWeek}
          name="accelerationRefreshSchedule.weekInput"
        />
      )}
      <TimeInput name="accelerationRefreshSchedule.timeInput" />
    </FormProvider>
  );
};

export default ScheduleRefresh;
