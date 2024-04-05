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

import { Tooltip } from "dremio-ui-lib";
import { TooltipProps as MuiTooltipProps } from "@mui/material/Tooltip";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { formatBytes } from "dremio-ui-common/utilities/formatBytes.js";

type FootprintProps = {
  currentByteSize: number | undefined;
  totalByteSize: number | undefined;
  tooltipPlacement?: MuiTooltipProps["placement"];
};

const { t } = getIntlContext();

function Footprint({
  currentByteSize = 0,
  totalByteSize,
  tooltipPlacement,
}: FootprintProps) {
  const currentSize = formatBytes(currentByteSize);
  const totalSize = formatBytes(totalByteSize || currentByteSize);

  return (
    <Tooltip
      title={
        <div>
          <div className="flex --alignCenter justify-between mb-05">
            {t("Sonar.Reflection.Column.CurrentFootprint.Label", {
              b: (chunk: string[]) => <b>{chunk}</b>,
            })}
            <b>{currentSize}</b>
          </div>
          <p className="mb-1">
            {t("Sonar.Reflection.Column.CurrentFootprint.Hint")}
          </p>
          <div className="flex --alignCenter justify-between mb-05">
            {t("Sonar.Reflection.Column.TotalFootprint.Label", {
              b: (chunk: string[]) => <b>{chunk}</b>,
            })}
            <b>{totalSize}</b>
          </div>
          <p>{t("Sonar.Reflection.Column.TotalFootprint.Hint")}</p>
        </div>
      }
      placement={tooltipPlacement}
    >
      <span>
        {currentSize}
        {totalByteSize !== currentByteSize && <> ({totalSize})</>}
      </span>
    </Tooltip>
  );
}

export default Footprint;
