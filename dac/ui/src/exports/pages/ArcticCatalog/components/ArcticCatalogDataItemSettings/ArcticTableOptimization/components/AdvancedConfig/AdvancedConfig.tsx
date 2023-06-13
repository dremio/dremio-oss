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
import { NumberInput } from "@mantine/core";
import clsx from "clsx";
import * as classes from "./AdvancedConfig.module.less";

export const AdvancedConfig = ({
  showTitle = true,
}: {
  showTitle?: boolean;
}) => {
  const { formatMessage } = useIntl();

  return (
    <div
      className={clsx(
        classes["advanced-config"],
        !showTitle && classes["small-margin-top"]
      )}
    >
      {showTitle && (
        <div className={classes["advanced-config-title"]}>
          {formatMessage({ id: "Advanced.Configuration" })}
        </div>
      )}
      <Controller
        name="advancedConfig.targetFileSize"
        render={({ field, fieldState: { error } }) => {
          return (
            <NumberInput
              {...field}
              label={formatMessage({ id: "Target.File.Sie" })}
              className={classes["advanced-config-input"]}
              error={error?.message}
            />
          );
        }}
      />
      <Controller
        name="advancedConfig.minFiles"
        render={({ field, fieldState: { error } }) => {
          return (
            <NumberInput
              {...field}
              label={formatMessage({ id: "Min.Input.Files" })}
              className={classes["advanced-config-input"]}
              error={error?.message}
            />
          );
        }}
      />
      <div className={classes["advanced-config-bottom"]}>
        <Controller
          name="advancedConfig.minFileSize"
          render={({ field, fieldState: { error } }) => {
            return (
              <NumberInput
                {...field}
                label={formatMessage({ id: "Min.File.Size" })}
                className={classes["advanced-config-bottom-half"]}
                error={error?.message}
              />
            );
          }}
        />
        <Controller
          name="advancedConfig.maxFileSize"
          render={({ field, fieldState: { error } }) => {
            return (
              <NumberInput
                {...field}
                label={formatMessage({ id: "Max.File.Size" })}
                className={classes["advanced-config-bottom-half"]}
                error={error?.message}
              />
            );
          }}
        />
      </div>
    </div>
  );
};
