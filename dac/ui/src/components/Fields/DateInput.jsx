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
import { Fragment, PureComponent } from "react";
import PropTypes from "prop-types";
import moment from "#oss/utils/dayjs";
import classNames from "clsx";

import { SelectView } from "#oss/components/Fields/SelectView";

import { dateTypeToFormat, TIME, DATE } from "#oss/constants/DataTypes";

import TimePicker from "./TimePicker";
import DateRangePicker from "../DateRangePicker/DateRangePicker";

import * as classes from "./DateInput.module.less";

export default class DateInput extends PureComponent {
  static propTypes = {
    onChange: PropTypes.func,
    value: PropTypes.string,
    disabled: PropTypes.bool,
    type: PropTypes.string,
    style: PropTypes.object,
  };

  static mergeDateWithTime(dateMoment, timeMoment, type) {
    const newMoment = dateMoment.clone().startOf("day");
    const justTime = timeMoment.diff(timeMoment.clone().startOf("day"));

    //Adding NaN in dayJS results in Invalid Date
    if (!isNaN(justTime)) {
      newMoment.add(justTime, "milliseconds");
    }

    return newMoment.format(dateTypeToFormat[type]);
  }

  constructor(props) {
    super(props);

    this.state = {
      value: props.value,
    };
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    const { value } = nextProps;
    if (value !== "") {
      this.setState({ value });
    }
  }

  getPopoverStyle() {
    const { type } = this.props;
    switch (type) {
      case TIME:
        return styles.timePopover;
      case DATE:
        return styles.datePopover;
      default:
        return styles.dateTimePopover;
    }
  }
  handleCalendarSelect = (closeDD, date) => {
    const { onChange, value, type } = this.props;

    const dateMoment = moment(date.startDate);
    const newValue = dateMoment.format(dateTypeToFormat[type]);
    if (onChange && newValue !== value) {
      const currentMoment = moment(value, dateTypeToFormat[type]);
      onChange(DateInput.mergeDateWithTime(dateMoment, currentMoment, type));
      closeDD();
    }
  };
  handleTimeChange = (timeMoment) => {
    const { onChange, value, type } = this.props;
    if (onChange) {
      const dateMoment = !value
        ? moment().startOf("day")
        : moment(value, dateTypeToFormat[type]);
      onChange(DateInput.mergeDateWithTime(dateMoment, timeMoment, type));
    }
  };
  handleInputChange = (e) => {
    this.setState({ value: e.target.value });
  };
  handleInputBlur = () => {
    const { value } = this.state;
    const { onChange, type } = this.props;
    const valueMoment = moment(value, dateTypeToFormat[type]);
    if (valueMoment.isValid()) {
      onChange(value);
    }
  };
  renderCalendar(closeDD) {
    const { value, type } = this.props;
    const showTimePicker = type !== DATE;
    const showCalendar = type !== TIME;
    const date = !value ? moment().startOf("day") : moment(value);
    const time = value || moment().format("hh:mm:ss");

    return (
      <>
        {showTimePicker ? (
          <TimePicker
            key="time-picker"
            columnType={type}
            value={time}
            onBlur={this.handleTimeChange}
          />
        ) : null}
        {showCalendar ? (
          <DateRangePicker
            months={1}
            onChange={this.handleCalendarSelect.bind(this, closeDD)}
            startDate={date.toDate()}
            endDate={date.toDate()}
          />
        ) : null}
      </>
    );
  }

  render() {
    const { props, state } = this;
    const popoverStyle = this.getPopoverStyle();

    return (
      <SelectView
        content={
          <Fragment>
            <input
              key="date-input"
              className={classes["dremio-date-input__input"]}
              style={props.style}
              type="text"
              value={state.value}
              onChange={this.handleInputChange}
              onBlur={this.handleInputBlur}
            />
            <dremio-icon
              key="date-icon"
              class="icon-primary"
              name="data-types/TypeDateTime"
              style={{
                blockSize: 24,
                inlineSize: 24,
                position: "relative",
                right: 40,
                cursor: "pointer",
              }}
            ></dremio-icon>
          </Fragment>
        }
        hideExpandIcon
        className={classNames("field", classes["dremio-date-input"])}
        style={styles.base}
        useLayerForClickAway={false}
      >
        {({ closeDD }) => (
          <div style={popoverStyle}>{this.renderCalendar(closeDD)}</div>
        )}
      </SelectView>
    );
  }
}

const styles = {
  dateTimePopover: {},
  timePopover: {
    height: 40,
  },
  datePopover: {
    height: "auto",
  },
};
