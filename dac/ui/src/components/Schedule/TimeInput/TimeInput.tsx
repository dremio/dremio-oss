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

import { Controller } from "react-hook-form";
import { useIntl } from "react-intl";
import { Label } from "dremio-ui-lib/components";
import { TimeInput as TimeInputMantine } from "@mantine/dates";

import * as classes from "./TimeInput.module.less";

type TimeInputProps = {
  name: string;
};

export const TimeInput = ({ name }: TimeInputProps) => {
  const { formatMessage } = useIntl();

  return (
    <div className={classes["time-input-container"]}>
      <Label
        value={formatMessage({ id: "Common.At" })}
        classes={{ root: classes["label"] }}
      />
      <Controller
        name={name}
        render={({ field, fieldState: { error } }) => {
          return (
            <TimeInputMantine
              {...field}
              name={name}
              className={classes["time-input"]}
              icon={
                <dremio-icon name="interface/time" class={classes["icon"]} />
              }
              error={error?.message}
            />
          );
        }}
      />
    </div>
  );
};

export default TimeInput;
