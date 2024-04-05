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
import { Controller } from "react-hook-form";
import { Checkbox } from "@mantine/core";
import { Label } from "dremio-ui-lib/components";

import * as classes from "./WeekInput.module.less";

const daysOfTheWeek = [
  ["1", "M"],
  ["2", "T"],
  ["3", "W"],
  ["4", "T"],
  ["5", "F"],
  ["6", "S"],
  ["0", "S"],
];

type WeekInputProps = {
  name: string;
  daysOfWeekProp: string[][];
};

export const WeekInput = ({ name, daysOfWeekProp }: WeekInputProps) => {
  const { formatMessage } = useIntl();

  return (
    <div className={classes["weekInput-wrapper"]}>
      <div className={classes["weekInput-container"]}>
        <Label
          value={formatMessage({ id: "Common.On" })}
          classes={{ root: classes["checkbox-group-label"] }}
        />
        <Controller
          name={name}
          render={({ field, fieldState: { error } }) => {
            return (
              <Checkbox.Group {...field} spacing="xs" error={error?.message}>
                {(daysOfWeekProp || daysOfTheWeek).map((element: string[]) => (
                  <Checkbox
                    key={element[0]}
                    value={element[0]}
                    label={element[1]}
                    size="xs"
                    radius="xs"
                    className={classes["checkbox"]}
                  />
                ))}
              </Checkbox.Group>
            );
          }}
        />
      </div>
    </div>
  );
};

export default WeekInput;
