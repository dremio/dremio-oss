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
import Immutable from "immutable";
import PropTypes from "prop-types";

import TimeUtils from "utils/timeUtils";

import { getIntervals as getIntervalsUtil, Intervals } from "./utils";
import classNames from "classnames";

import * as classes from "./LeftPanel.module.less";

class LeftPanel extends PureComponent {
  static propTypes = {
    filterType: PropTypes.string.isRequired,
    activeType: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired,
  };

  static getDays(currentMoment, pastMoment) {
    const currentMonth = currentMoment.format("MMM");
    const pastMonth = pastMoment.format("MMM");
    const currentDay = TimeUtils.zeroesPadding(currentMoment.get("date"), 2);
    const pastDay = TimeUtils.zeroesPadding(pastMoment.get("date"), 2);
    return `${pastMonth} ${pastDay} - ${currentMonth} ${currentDay}, ${currentMoment.get(
      "year"
    )}`;
  }

  static getHours(currentMoment, pastMoment) {
    const format = "HH:mm";
    return `${pastMoment.format(format)} - ${currentMoment.format(format)}`;
  }

  static getIntervals = getIntervalsUtil; //TODO Remove static

  renderIntervals = () =>
    Intervals.map((item, index) =>
      item.label !== undefined && item.time !== undefined ? (
        <div
          className={classNames(classes["jobFiltersLeftPanel__interval"], {
            // [classes["jobFiltersLeftPanel__interval--activeInterval"]]: item.type === this.props.filterType, //Not sure when this is used
            [classes["jobFiltersLeftPanel__interval--activeFilter"]]:
              item.type === this.props.activeType,
          })}
          key={index}
          data-qa={item.dataQa}
          onClick={this.props.onChange.bind(
            this,
            item.type,
            Immutable.fromJS(item.time)
          )}
        >
          {item.label}
        </div>
      ) : (
        <div className={classes["jobFiltersLeftPanel__interval-separator"]} />
      )
    );

  render() {
    return (
      <div className={classes["jobFiltersLeftPanel"]}>
        {this.renderIntervals()}
      </div>
    );
  }
}

export default LeftPanel;
