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
import { useEffect, useMemo, useState } from "react";
import { DateRange, DateRangeProps, Range } from "react-date-range";
import { PALE_NAVY } from "@app/uiTheme/radium/colors";
import "react-date-range/dist/styles.css";
import "react-date-range/dist/theme/default.css";

import * as classes from "./DateRangePicker.module.less";

type DateRangePickerProps = {
  startDate: Date;
  endDate: Date;
  onChange?: (arg: Range) => void;
  rangeColors?: string[];
  months?: number;
};

function DateRangePicker({
  startDate,
  endDate,
  onChange,
  rangeColors = [PALE_NAVY],
  months = 3,
}: DateRangePickerProps) {
  const ranges = useMemo(
    () => [
      {
        startDate,
        endDate,
        key: "selection",
      },
    ],
    [startDate, endDate]
  );

  const [state, setState] = useState<any>(ranges);

  const handleChange: DateRangeProps["onChange"] = (result) => {
    setState([result.selection]);
    if (onChange) onChange(result.selection);
  };

  useEffect(() => {
    setState(ranges);
  }, [ranges, endDate]);

  const currentYear = new Date().getFullYear();

  return (
    <div className={classes["dremio-daterangepicker"]}>
      <DateRange
        months={months}
        onChange={handleChange}
        ranges={state}
        rangeColors={rangeColors}
        shownDate={endDate}
        scroll={{ enabled: true }}
        preventSnapRefocus
        minDate={new Date(2015, 0)} // start from 2015
        maxDate={new Date(currentYear + 1, 11)} // till next year
      />
    </div>
  );
}

export default DateRangePicker;
