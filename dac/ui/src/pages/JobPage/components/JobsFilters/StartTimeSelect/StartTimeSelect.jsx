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
import Immutable from "immutable";
import moment from "@app/utils/dayjs";
import { injectIntl } from "react-intl";
import PropTypes from "prop-types";
import clsx from "clsx";
import { SelectView } from "@app/components/Fields/SelectView";
import EllipsedText from "components/EllipsedText";
import * as IntervalTypes from "./IntervalTypes";
import LeftPanel from "./LeftPanel";
import RightPanel from "./RightPanel";

import "./StartTimeSelect.less";

class StartTimeSelect extends Component {
  static propTypes = {
    id: PropTypes.string.isRequired,
    startTime: PropTypes.number,
    endTime: PropTypes.number,
    defaultType: PropTypes.string,
    onChange: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    popoverFilters: PropTypes.string,

    className: PropTypes.string,
  };

  getLabel = () => {
    const {
      intl: { formatMessage },
      className,
    } = this.props;
    const selectedInterval = this.getSelectedInterval();
    const selectedType = this.getActiveTimeType(selectedInterval);
    const options = this.getOptions(selectedInterval);
    const startMoment = options.getIn(["range", "startMoment"]);
    const endMoment = options.getIn(["range", "endMoment"]);

    if (
      selectedType === IntervalTypes.CUSTOM_INTERVAL &&
      startMoment &&
      endMoment
    ) {
      const duration = moment.duration(endMoment.diff(startMoment));
      const days = Math.floor(duration.asDays());
      const rangeText =
        days > 0
          ? LeftPanel.getDays(endMoment, startMoment)
          : LeftPanel.getHours(endMoment, startMoment);
      return (
        <EllipsedText text={`Custom (${rangeText})`}>
          {`Custom (${rangeText})`}
        </EllipsedText>
      );
    }
    if (!selectedInterval || selectedType === IntervalTypes.ALL_TIME_INTERVAL) {
      return (
        <div>
          <span>{formatMessage({ id: "Common.StartTime" })} </span>
          <span className={className}>{selectedInterval.get("label")}</span>
        </div>
      );
    }
    const interval = LeftPanel.getIntervals().find(
      (int) => int.get("type") === selectedType,
    );
    return interval ? (
      <EllipsedText text={interval.get("label")}>
        {interval.get("label")}
      </EllipsedText>
    ) : null;
  };

  getOptions(selectedInterval) {
    return Immutable.fromJS({
      range: {
        startMoment: selectedInterval
          ? selectedInterval.getIn(["time", 0])
          : moment(this.props.startTime),
        endMoment: selectedInterval
          ? selectedInterval.getIn(["time", 1])
          : moment(this.props.endTime),
      },
    });
  }

  getSelectedInterval() {
    const startMoment = this.props.startTime && moment(this.props.startTime);
    const endMoment = this.props.endTime && moment(this.props.endTime);
    const intervals = LeftPanel.getIntervals();

    if (!startMoment || !endMoment) {
      return intervals.find(
        (item) => item.get("type") === this.props.defaultType,
      );
    }

    return intervals.find((item) => {
      const startTime =
        item.getIn(["time", 0]) && item.getIn(["time", 0]).unix();
      const currentStartTime = startMoment.unix();
      const endTime = item.getIn(["time", 1]).unix();
      const currentEndTime = endMoment.unix();
      const diffStart = Math.abs(currentStartTime - startTime);
      const diffEnd = Math.abs(currentEndTime - endTime);

      return diffStart < 1000 && diffEnd < 1000;
    });
  }
  getActiveTimeType(interval) {
    return interval ? interval.get("type") : IntervalTypes.CUSTOM_INTERVAL;
  }

  handleChange = (type, range) => {
    if (this.props.onChange) {
      this.props.onChange(type, range);
    }
  };

  renderDropdown = (selectedType, options) => {
    return (
      <div style={style.dropDown}>
        <LeftPanel
          filterType={this.props.defaultType}
          activeType={selectedType}
          onChange={this.handleChange}
        />
        <RightPanel
          key={selectedType} //Rerender datepicker until fixed: https://github.com/hypeserver/react-date-range/issues/552
          handleChange={this.handleChange}
          options={options}
        />
      </div>
    );
  };

  render() {
    const selectedInterval = this.getSelectedInterval();
    const selectedType = this.getActiveTimeType(selectedInterval);
    const options = this.getOptions(selectedInterval);

    const className = clsx("startTimeSelect__labelWrapper", {
      "--filtered":
        selectedInterval && selectedType !== IntervalTypes.ALL_TIME_INTERVAL,
    });
    const icon =
      selectedInterval && selectedType !== IntervalTypes.ALL_TIME_INTERVAL
        ? "ArrowDownBlue.svg"
        : "ArrowDown.svg";

    return (
      <SelectView
        popoverFilters={this.props.popoverFilters}
        className={className}
        content={this.getLabel}
        dataQa="st-filter"
        icon={icon}
        isArtIcon
        iconClass="startTimeSelect__arrowIcon"
      >
        {this.renderDropdown(selectedType, options)}
      </SelectView>
    );
  }
}
export default injectIntl(StartTimeSelect);
const style = {
  dropDown: {
    boxShadow: "var(--dremio--shadow--layer-1)",
    borderRadius: "2px",
    backgroundColor: "var(--fill--popover)",
    zIndex: "99",
    padding: "0 0 8px",
    display: "flex",
    flexWrap: "nowrap",
    fontFamily: "var(--dremio--font-family)",
    height: "100%",
    width: "100%",
  },
};
