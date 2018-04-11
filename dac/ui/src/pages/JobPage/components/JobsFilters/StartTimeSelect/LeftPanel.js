/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import Immutable  from 'immutable';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import moment from 'moment';

import TimeUtils from 'utils/timeUtils';
import { PALE_BLUE, PALE_NAVY } from 'uiTheme/radium/colors';

import * as IntervalTypes from './IntervalTypes';

@PureRender
@Radium
class LeftPanel extends Component {
  static propTypes = {
    filterType: PropTypes.string.isRequired,
    activeType: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired
  }

  static getDays(currentMoment, pastMoment) {
    const currentMonth = moment.monthsShort(currentMoment.month());
    const pastMonth = moment.monthsShort(pastMoment.month());
    const currentDay = TimeUtils.zeroesPadding(currentMoment.get('date'), 2);
    const pastDay = TimeUtils.zeroesPadding(pastMoment.get('date'), 2);
    return `${pastMonth} ${pastDay} - ${currentMonth} ${currentDay}, ${currentMoment.get('year')}`;
  }

  static getHours(currentMoment, pastMoment) {
    const format = 'HH:mm';
    return `${pastMoment.format(format)} - ${currentMoment.format(format)}`;
  }

  static getLastHours = (value) => {
    const currentHourMoment = moment().startOf('hour');
    const pastHourMoment = moment().startOf('hour').subtract(value, 'h');
    return LeftPanel.getHours(currentHourMoment, pastHourMoment);
  }

  static getLastDays = (value) => {
    const currentDayMoment = moment().startOf('date');
    const pastDayMoment = moment().startOf('date').subtract(value, 'd');
    return LeftPanel.getDays(currentDayMoment, pastDayMoment);
  }

  static getIntervalsStyles(itemType, filterType) {
    const result = [style.interval];
    if (itemType === filterType) {
      result.push(style.activeInterval);
    }
    return result;
  }

  static getIntervals = () => Immutable.fromJS([
    {
      type: IntervalTypes.LAST_HOUR_INTERVAL,
      label: `Last Hour (${LeftPanel.getLastHours(1)})`,
      time: [moment().subtract(1, 'h'), moment()]
    },
    {
      type: IntervalTypes.LAST_6_HOURS_INTERVAL,
      label: `Last 6 Hours (${LeftPanel.getLastHours(6)})`,
      time: [moment().subtract(6, 'h'), moment()]
    },
    {
      type: IntervalTypes.LAST_1_DAY_INTERVAL,
      label: `Last 24 Hours (${LeftPanel.getLastDays(1)})`,
      time: [moment().subtract(1, 'd'), moment()]
    },
    {
      type: IntervalTypes.LAST_3_DAYS_INTERVAL,
      label: `Last 3 Days (${LeftPanel.getLastDays(3)})`,
      time: [moment().subtract(3, 'd'), moment()]
    },
    {
      type: IntervalTypes.LAST_7_DAYS_INTERVAL,
      label: `Last 7 Days (${LeftPanel.getLastDays(7)})`,
      time: [moment().subtract(7, 'd'), moment()]
    },
    {
      type: IntervalTypes.LAST_30_DAYS_INTERVAL,
      label: `Last 30 Days (${LeftPanel.getLastDays(30)})`,
      time: [moment().subtract(30, 'd'), moment()]
    },
    {
      type: IntervalTypes.LAST_90_DAYS_INTERVAL,
      label: `Last 90 Days (${LeftPanel.getLastDays(90)})`,
      time: [moment().subtract(90, 'd'), moment()]
    },
    {
      type: IntervalTypes.YEAR_TO_DATE_INTERVAL,
      label: 'Year to Date',
      time: [moment().startOf('year'), moment()]
    },
    {
      type: IntervalTypes.ALL_TIME_INTERVAL,
      label: 'All Time',
      time: [moment(0), moment()]
    }
  ]);

  renderIntervals = () => LeftPanel.getIntervals().map((item, index) => (
    <div
      style={[LeftPanel.getIntervalsStyles(item.get('type'), this.props.filterType),
        item.get('type') === this.props.activeType && style.activeFilter]}
      key={index}
      onClick={this.props.onChange.bind(this, item.get('type'), item.get('time'))}>
      {item.get('label')}
    </div>
    )
  );

  render() {
    return (
      <div style={[style.panel]}>
        {this.renderIntervals()}
      </div>
    );
  }
}

const style = {
  panel: {
    width: 213,
    display: 'inline-block',
    height: 264,
    borderRight: '1px solid #C4C4C4',
    margin: '0 0 0 2px',
    verticalAlign: 'top',
    paddingRight: 3
  },
  interval: {
    padding: '6px 3px 6px 5px',
    cursor: 'pointer',
    ':hover': {
      'backgroundColor': PALE_BLUE
    }
  },
  activeInterval: {
    backgroundColor: PALE_NAVY
  },
  activeFilter: {
    backgroundColor: PALE_BLUE
  }
};

export default LeftPanel;
