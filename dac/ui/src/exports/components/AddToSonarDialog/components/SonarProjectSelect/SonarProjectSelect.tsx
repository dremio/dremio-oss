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
import { Skeleton } from "@app/../../ui-lib/dist-esm/Skeleton";
import { intl } from "@app/utils/intl";
import { Select } from "@mantine/core";
import { orderBy } from "lodash";
import { forwardRef, useMemo } from "react";
import { RequestStatus } from "smart-resource";

import * as classes from "./SonarProjectSelect.module.less";

const ERROR_ITEM = "ERROR_ITEM";
const LOADING_ITEM = "LOADING_ITEM";

const Skeletons = new Array(5)
  .fill(null)
  .map((v, i) => ({ label: LOADING_ITEM, value: `${LOADING_ITEM}-${i}` }));

interface ItemProps extends React.ComponentPropsWithoutRef<"div"> {
  label?: string;
  value: string;
}

type SonarProjectSelectProps = {
  defaultProjectId?: string;
  className?: string;
  disabled: boolean;
  status: RequestStatus;
  projects: any[] | null;
  onChange: (value: string) => void;
  value?: string;
  error?: any;
  label?: string;
  description?: string;
};

export default function SonarProjectSelect(props: SonarProjectSelectProps) {
  const SonarProjectSelectItem = forwardRef<HTMLDivElement, ItemProps>(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    ({ label, value, ...others }: ItemProps, ref) => {
      function getContent() {
        if (label === LOADING_ITEM) {
          return <Skeleton width="18ch" />;
        } else if (value === ERROR_ITEM) {
          return (
            <span>
              {intl.formatMessage({ id: "ArcticSource.ErrorCatalogItem" })}
            </span>
          );
        } else {
          return (
            <span className={classes["label-wrapper"]}>
              <dremio-icon name="brand/sonar-project" />
              {label}
            </span>
          );
        }
      }
      return (
        <div ref={ref} {...others}>
          {getContent()}
        </div>
      );
    }
  );

  const options = useMemo(() => {
    if (props.status === "pending" || props.status === "initial") {
      return Skeletons;
    } else if (props.projects) {
      return orderBy(props.projects, "name").map((cur) => ({
        value: cur.id,
        label:
          props.defaultProjectId && cur.id === props.defaultProjectId
            ? cur.name + " (Default)"
            : cur.name,
        disabled: cur.id === props.value,
      }));
    } else {
      return [
        {
          value: ERROR_ITEM,
          label: "",
        },
      ];
    }
  }, [props.projects, props.status, props.defaultProjectId, props.value]);

  return (
    <Select
      icon={<dremio-icon name="brand/sonar-project" />}
      label={props.label}
      description={props.description}
      error={props.error}
      disabled={props.disabled}
      itemComponent={SonarProjectSelectItem}
      data={options}
      value={props.value}
      onChange={props.onChange}
    />
  );
}
