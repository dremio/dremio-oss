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

import { useIntl } from "react-intl";
import { useFormContext, Controller } from "react-hook-form";
import { Select, NumberInput } from "@mantine/core";
import { Label } from "dremio-ui-lib/components";
import clsx from "clsx";
import * as classes from "./DaySelector.module.less";

const options = [
  { value: "@hours", label: "hours" },
  { value: "@day", label: "day" },
  { value: "@week", label: "week" },
  { value: "@month", label: "month" },
];

type DaySelectorProps = {
  name: string;
};

export const DaySelector = ({ name }: DaySelectorProps) => {
  const { formatMessage } = useIntl();
  const { watch } = useFormContext();
  const showHourInput = watch("schedule.scheduleType") === "@hours";
  return (
    <div
      className={clsx(
        classes["day-selector"],
        showHourInput && classes["additional-margin"]
      )}
    >
      <Label
        value={formatMessage({ id: "Common.Every" })}
        classes={{ root: classes["day-selector-label"] }}
      />
      <Controller
        name="schedule.hourInput"
        render={({ field, fieldState: { error } }) => {
          return showHourInput ? (
            <NumberInput
              {...field}
              min={1}
              max={23}
              className={classes["hour-input"]}
              error={!!error?.message}
            />
          ) : (
            <></>
          );
        }}
      />
      <Controller
        name={name}
        render={({ field, fieldState: { error } }) => {
          return (
            <Select
              {...field}
              className={classes["schedule-type-selector"]}
              data={options}
              rightSection={<dremio-icon name="interface/caretDown" />}
              error={error?.message}
            />
          );
        }}
      />
    </div>
  );
};

export default DaySelector;
