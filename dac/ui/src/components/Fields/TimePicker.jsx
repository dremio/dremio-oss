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
import { Component } from "react";
import PropTypes from "prop-types";
import moment from "@app/utils/dayjs";
import { FormattedMessage } from "react-intl";
import classNames from "clsx";

import { dateTypeToFormat } from "@app/constants/DataTypes";

import * as classes from "./TimePicker.module.less";

const TIME_FORMAT = "HH:mm:ss";

export default class TimePicker extends Component {
  static propTypes = {
    value: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    columnType: PropTypes.string,
    onBlur: PropTypes.func,
  };
  static defaultProps = {
    value: "00:00:00",
  };
  constructor(props) {
    super(props);

    this.state = {
      value: moment(props.value, dateTypeToFormat[props.columnType]).format(
        TIME_FORMAT
      ),
    };
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.setState({
      value: moment(
        nextProps.value,
        dateTypeToFormat[nextProps.columnType]
      ).format(TIME_FORMAT),
    });
  }

  onChange = (e) => {
    const { value } = e.target;
    this.setState({
      value,
    });
  };
  onBlur = () => {
    const { value } = this.state;
    const valMoment = moment(value, TIME_FORMAT);
    if (valMoment.isValid()) {
      this.props.onBlur(valMoment);
    }
  };
  render() {
    const { value } = this.state;
    return (
      <div className={classNames("field", classes["dremio-time-field"])}>
        <label
          htmlFor="dremio-time-input"
          className={classes["dremio-time-field__label"]}
        >
          <FormattedMessage id="Type.Time" />:
        </label>
        <input
          id="dremio-time-input"
          type="text"
          value={value}
          onChange={this.onChange}
          onBlur={this.onBlur}
        />
        <div className={classes["dremio-time-field__formatLabel"]}>
          hh:mm:ss
        </div>
      </div>
    );
  }
}
