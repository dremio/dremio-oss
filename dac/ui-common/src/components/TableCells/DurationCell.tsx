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

// @ts-ignore
import { Tooltip as DremioTooltip } from "dremio-ui-lib";
import { NumericCell } from "./NumericCell";
import { formatDurationUnderSecond } from "../../utilities/formatDuration";

export const DurationCell = ({
  duration,
  metricContent,
}: {
  duration: number;
  metricContent?: JSX.Element | null | undefined;
}) => {
  const cellContent = (
    <NumericCell>{formatDurationUnderSecond(duration)}</NumericCell>
  );

  return metricContent != null ? (
    <DremioTooltip
      interactive
      type="richTooltip"
      enterDelay={500}
      title={metricContent}
    >
      <div>{cellContent}</div>
    </DremioTooltip>
  ) : (
    cellContent
  );
};
