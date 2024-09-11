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
import { useState, useRef } from "react";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { addTooltip } from "@app/components/DatasetSummary/datasetSummaryUtils";
import * as classes from "./SummaryItemLabelError.module.less";

type SummaryItemlabelErrorProps = {
  iconName?: string;
  title?: string;
};

const SummaryItemlabelError = ({
  iconName,
  title,
}: SummaryItemlabelErrorProps) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const titleRef = useRef(null);

  return (
    <div className={classes["dataset-not-found-summary-header"]}>
      <div className={classes["dataset-not-found-icon-container"]}>
        {iconName ? (
          <dremio-icon
            class="dremio-icon-label"
            name={
              iconName.startsWith("entities/")
                ? iconName
                : `entities/${iconName}`
            }
          />
        ) : (
          <dremio-icon name="interface/close-circle-error" />
        )}
      </div>

      {showTooltip ? (
        <Tooltip interactive placement="top" title={title}>
          <p className={classes["dataset-not-found-summary-header-title"]}>
            {title}
          </p>
        </Tooltip>
      ) : (
        <p
          ref={titleRef}
          onMouseEnter={() => addTooltip(titleRef, setShowTooltip)}
          className={classes["dataset-not-found-summary-header-title"]}
        >
          {title}
        </p>
      )}
    </div>
  );
};

export default SummaryItemlabelError;
