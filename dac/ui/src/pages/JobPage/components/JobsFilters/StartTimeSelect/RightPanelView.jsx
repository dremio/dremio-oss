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
import { PureComponent } from "react";
import moment from "#oss/utils/dayjs";

import PropTypes from "prop-types";

import DateTimeInput from "./DateTimeInput";
import DateRangePicker from "#oss/components/DateRangePicker/DateRangePicker";

import "./RightPanelView.less";

class RightPanelView extends PureComponent {
  static propTypes = {
    endMoment: PropTypes.object.isRequired,
    startMoment: PropTypes.object.isRequired,
    onChange: PropTypes.func.isRequired,
    onChangeInInput: PropTypes.func.isRequired,
  };

  state = {
    ranges: [
      {
        startDate: this.props.startMoment.toDate(),
        endDate: this.props.endMoment.toDate(),
        key: "selection",
      },
    ],
  };

  handleSelect = (item) => {
    const { startDate, endDate } = item;
    this.props.onChange({
      startDate: moment(startDate),
      endDate: moment(endDate),
    });
  };
  handleInit = (date) => this.props.onChange(date, true);

  render() {
    const { startMoment, endMoment } = this.props;
    return (
      <div className="start-time-right-panel">
        <div className="start-time-right-panel__pickers">
          <DateTimeInput
            date={this.props.startMoment || moment()}
            onChange={this.props.onChangeInInput}
            label="From"
            type="startDate"
          />
          <DateTimeInput
            date={this.props.endMoment || moment()}
            onChange={this.props.onChangeInInput}
            label="To"
            type="endDate"
          />
        </div>
        <div>
          <DateRangePicker
            onChange={this.handleSelect}
            startDate={startMoment.toDate()}
            endDate={endMoment.toDate()}
          />
        </div>
      </div>
    );
  }
}
export default RightPanelView;
