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
import { useState } from "react";
// @ts-ignore
import { Select } from "dremio-ui-lib";
import * as classes from "./BreadcrumbSelector.module.less";

type BreadcrumbSelectorProps = {
  defaultValue: string;
  options: { label: any; value: string }[];
};
const BreadcrumbSelector = ({
  options,
  defaultValue,
}: BreadcrumbSelectorProps) => {
  const [value, setValue] = useState(defaultValue);
  return (
    <div className={classes["breadcrumbs-container"]}>
      <Select
        value={value}
        options={options}
        role="breadcrumb"
        onChange={(e: any) => setValue(e.target.value)}
      />
    </div>
  );
};

export default BreadcrumbSelector;
