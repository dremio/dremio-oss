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
import { useFormContext, Controller } from "react-hook-form";
import { Radio, NumberInput, Select } from "@mantine/core";
import { intl } from "@app/utils/intl";
import { Label } from "dremio-ui-lib/components";
import * as classes from "./MonthInput.module.less";

const { formatMessage } = intl;
const ordinalOfEveryMonthOptions = [
  { value: "1", label: "1st" },
  { value: "2", label: "2nd" },
  { value: "3", label: "3rd" },
  { value: "4", label: "4th" },
];

const weekdayOfEveryMonthOptions = [
  { value: "1", label: formatMessage({ id: "Common.Monday" }) },
  { value: "2", label: formatMessage({ id: "Common.Tuesday" }) },
  { value: "3", label: formatMessage({ id: "Common.Wednesday" }) },
  { value: "4", label: formatMessage({ id: "Common.Thursday" }) },
  { value: "5", label: formatMessage({ id: "Common.Friday" }) },
  { value: "6", label: formatMessage({ id: "Common.Saturday" }) },
  { value: "0", label: formatMessage({ id: "Common.Sunday" }) },
];

export const MonthInput = () => {
  const { watch } = useFormContext();
  const showOptionOne = watch("schedule.monthScheduleOption") === "first";

  return (
    <div className={classes["month-schedule"]}>
      <Controller
        name="schedule.monthScheduleOption"
        render={({ field, fieldState: { error } }) => {
          return (
            <Radio.Group
              {...field}
              size="xs"
              spacing={8}
              error={error?.message}
            >
              <div className={classes["month-container"]}>
                <div className={classes["radio-container"]}>
                  <Radio value="first" />
                  <Label
                    value={formatMessage({ id: "Common.On" })}
                    classes={{ root: classes["pre-number-label"] }}
                  />
                  <Controller
                    name="schedule.dayOfMonth"
                    render={({ field, fieldState: { error } }) => {
                      return (
                        <NumberInput
                          {...field}
                          min={1}
                          max={30}
                          disabled={!showOptionOne}
                          className={classes["input-width"]}
                          error={!!error?.message}
                        />
                      );
                    }}
                  />
                </div>
                <Label
                  value={formatMessage({
                    id: "Data.Optimization.DayOfEveryMonth",
                  })}
                  classes={{ root: classes["label"] }}
                />
              </div>

              <div className={classes["month-container"]}>
                <div className={classes["radio-container"]}>
                  <Radio value="second" />
                  <Label
                    value={formatMessage({ id: "Common.On" })}
                    classes={{ root: classes["pre-number-label"] }}
                  />
                  <Controller
                    name="schedule.ordinalOfEveryMonth"
                    render={({ field, fieldState: { error } }) => {
                      return (
                        <Select
                          {...field}
                          className={classes["input-width"]}
                          disabled={showOptionOne}
                          data={ordinalOfEveryMonthOptions}
                          rightSection={
                            <dremio-icon
                              name="interface/caretDown"
                              disabled={showOptionOne}
                            />
                          }
                          error={error?.message}
                        />
                      );
                    }}
                  />
                  <Controller
                    name="schedule.weekdayOfEveryMonth"
                    render={({ field, fieldState: { error } }) => {
                      return (
                        <Select
                          {...field}
                          className={classes["day-select-container"]}
                          disabled={showOptionOne}
                          data={weekdayOfEveryMonthOptions}
                          rightSection={
                            <dremio-icon
                              name="interface/caretDown"
                              disabled={showOptionOne}
                            />
                          }
                          error={error?.message}
                        />
                      );
                    }}
                  />
                </div>
                <Label
                  value={formatMessage({
                    id: "Data.Optimization.OfEveryMonth",
                  })}
                  classes={{ root: classes["label"] }}
                />
              </div>
            </Radio.Group>
          );
        }}
      />
    </div>
  );
};

export default MonthInput;
