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
import {
  formatDateTimestamp,
  formatDateTimestampShort,
  formatDateTimestampShortNoTZ,
} from "../../utilities/formatDate";

type TimestampCellProps = {
  timestamp: Date;
  // Used for changing the value after it has been formatted. I.e. removing comma from string
  applyFormat?: (dateValue: string) => string;
};

const CellRenderer = (props: TimestampCellProps & { children: string }) => {
  return (
    <div
      className="dremio-typography-tabular-numeric"
      style={{ textAlign: "right" }}
    >
      <time dateTime={props.timestamp.toISOString()}>
        {props.applyFormat ? props.applyFormat(props.children) : props.children}
      </time>
    </div>
  );
};

export const ShortTimestampCell = (props: TimestampCellProps) => {
  return (
    <CellRenderer {...props}>
      {formatDateTimestampShort(props.timestamp)}
    </CellRenderer>
  );
};

export const TimestampCell = (props: TimestampCellProps) => {
  return (
    <CellRenderer {...props}>
      {formatDateTimestamp(props.timestamp)}
    </CellRenderer>
  );
};

export const TimestampCellShortNoTZ = (props: TimestampCellProps) => {
  return (
    <CellRenderer {...props}>
      {formatDateTimestampShortNoTZ(props.timestamp)}
    </CellRenderer>
  );
};
