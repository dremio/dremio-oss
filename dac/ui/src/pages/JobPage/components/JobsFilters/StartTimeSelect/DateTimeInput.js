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

import PropTypes from "prop-types";

import FontIcon from "components/Icon/FontIcon";
import MaskedInput from "./MaskedInput";

const MAX_MONTH = 12;
const MIN_YEAR = 1970;
const MAX_YEAR = new Date().getFullYear();

import * as classes from "./DateTimeInput.module.less";

export default class DateTimeInput extends PureComponent {
  static propTypes = {
    date: PropTypes.object.isRequired,
    label: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired,
  };

  static formatDate(inputType, inputValue, date) {
    const currentMoment = date.clone();
    if (inputType === "timeInput") {
      let [hour, minute] = inputValue.split(":");
      if (Number(hour) >= 23) {
        hour = "23";
        minute = Number(minute) > 59 ? "59" : minute;
      }
      currentMoment.hour(hour);
      currentMoment.minute(minute);
    } else {
      let [month, day, year] = inputValue.split("/");

      if (year.length > 3 && Number(year).toString() === year) {
        year = Math.min(MAX_YEAR, Math.max(MIN_YEAR, year));
      }

      if (day === "00") {
        day = "1";
      } else if (day > currentMoment.daysInMonth()) {
        day = currentMoment.daysInMonth();
      }

      if (month === "00") {
        month = "1";
      } else if (Number(month) > MAX_MONTH) {
        month = MAX_MONTH;
      }

      if (month && Number(month) > 0) {
        currentMoment.month(month - 1);
      }
      if (day && Number(day) > 0) {
        currentMoment.date(day);
      }
      if (year) {
        currentMoment.year(year);
      }
    }
    return currentMoment;
  }

  constructor(props) {
    super(props);
    this.state = { currentMoment: this.props.date.clone() };
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (this.props.date !== nextProps.date) {
      this.setState({ currentMoment: nextProps.date.clone() });
    }
  }

  onInputChange = (inputType, inputValue) =>
    this.setState({
      currentMoment: DateTimeInput.formatDate(
        inputType,
        inputValue,
        this.state.currentMoment
      ),
    });

  onBlur = (inputValue, mask) => {
    if (mask === "dd/dd/dddd") {
      const [month, day, year] = inputValue.split("/");
      const correctedYear = Math.min(MAX_YEAR, Math.max(MIN_YEAR, year));
      if (correctedYear !== Number(year)) {
        this.setState({
          currentMoment: DateTimeInput.formatDate(
            "dateInput",
            [month, day, correctedYear].join("/"),
            this.state.currentMoment
          ),
        });
      }
    }
    this.props.onChange(this.props.type, this.state.currentMoment);
  };

  render() {
    const { date, label } = this.props;
    const { currentMoment } = this.state;
    const timePlaceholder = date.format("HH:mm");
    const datePlaceholder = date.format("M/D/YYYY");
    const timeValue = currentMoment.format("HH:mm");
    const dateValue = currentMoment.format("M/D/YYYY");
    return (
      <div className={classes["date-time-input"]}>
        <h5 className={classes["date-time-input__label"]}>{label}</h5>
        <div className={classes["date-time-input__innerBlock"]}>
          <FontIcon type="Date" theme={style.iconStyle} />
          <MaskedInput
            mask="dd/dd/dddd"
            value={dateValue}
            placeholder={datePlaceholder}
            onBlur={this.onBlur}
            onChange={this.onInputChange.bind(this, "dateInput")}
          />
        </div>
        <div className={classes["date-time-input__innerBlock"]}>
          <FontIcon type="Time" theme={style.iconStyle} />
          <MaskedInput
            mask="dd:dd"
            value={timeValue}
            placeholder={timePlaceholder}
            onBlur={this.onBlur}
            onChange={this.onInputChange.bind(this, "timeInput")}
          />
        </div>
      </div>
    );
  }
}
const style = {
  iconStyle: {
    Container: { display: "flex" },
    Icon: { width: "20px", height: "20px" },
  },
};
