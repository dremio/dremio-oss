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
import { DateRange } from 'react-date-range';
import Radium from 'radium';
import moment from 'moment';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { PALE_GREY, PALE_NAVY } from 'uiTheme/radium/colors';

import DateTimeInput from './DateTimeInput';
import './RightPanelView.less';

@PureRender
@Radium
class RightPanelView extends Component {
  static propTypes = {
    endMoment: PropTypes.object.isRequired,
    startMoment: PropTypes.object.isRequired,
    onChange: PropTypes.func.isRequired,
    onChangeInInput: PropTypes.func.isRequired
  };

  handleSelect = (date, source) => {
    const dateId = String(date.endDate.unix()) + String(date.startDate.unix());
    if (this.date === dateId) {
      date.endDate.add(23, 'hours');
    }
    if (source) {
      this.props.onChange(date);
    }
    this.date = dateId;
  }
  handleInit = (date) => this.props.onChange(date, true)

  render() {
    return (
      <div className='start-time-right-panel' style={[style.base, style.main]}>
        <div style={[style.header]}>
          <DateTimeInput
            date={this.props.startMoment || moment()}
            onChange={this.props.onChangeInInput}
            label='From'
            type='startDate'/>
          <DateTimeInput
            date={this.props.endMoment || moment()}
            onChange={this.props.onChangeInInput}
            label='To'
            type='endDate'/>
        </div>
        <div>
          <DateRange
            calendars='3'
            linkedCalendars
            theme={calendarTheme}
            classNames={{dayToday: 'today'}}
            onInit={this.handleInit}
            onChange={this.handleSelect}
            startDate={this.props.startMoment.clone()}
            endDate={this.props.endMoment.clone()}/>
        </div>
      </div>);
  }
}

export default RightPanelView;

const calendarTheme = {
  DateRange: {
    background: 'inherit',
    margin: '4px auto 4px auto',
    width: 430
  },
  Calendar: {
    background: 'inherit',
    boxSizing: 'content-box',
    width: 140,
    padding: '0'
  },
  MonthAndYear: {
    padding: 0,
    color: '#949494',
    height: 20
  },
  MonthButton: {
    margin: 0,
    background: 'inherit'
  },
  MonthArrow: {
    border: '6px solid transparent'
  },
  MonthArrowPrev: {
    borderRightColor: '#a4a4a4'
  },
  MonthArrowNext: {
    borderLeftColor: '#a4a4a4'
  },
  Weekday: {
    lineHeight: '16px',
    letterSpacing: 'none',
    height: 17,
    color: '#707070',
    fontWeight: 0,
    fontSize: 11,
    width: 19,
    marginBottom: 1
  },
  DayInRange: {
    background: PALE_NAVY,
    color: '#333'
  },
  DayStartEdge: {
    background: PALE_NAVY,
    color: '#333'
  },
  DayEndEdge: {
    background: PALE_NAVY,
    color: '#333'
  },
  Day: {
    width: 19,
    marginBottom: 1,
    borderBottom: '1px solid rgba(0,0,0,.1)',
    color: '#333'
  },
  DayHover: {
    color: '#f4f4f4'
  }
};

const style = {
  base: {
    display: 'inline-block',
    height: '100%',
    verticalAlign: 'top'
  },
  header: {
    padding: '5px 0 0 0',
    backgroundColor: PALE_GREY
  },
  main: {
    width: 450
  },
  underLine: {
    textDecoration: 'underline',
    margin: '0 5px 0 0',
    ':hover': {
      cursor: 'pointer'
    }
  },
  block: {
    padding: 6
  },
  nowBlock: {
    color: '#a9a9a9'
  },
  applyButton: {
    margin: '0 10px 0 0',
    display: 'inline-flex',
    float: 'right',
    width: 110,
    height: 25
  },
  buttonsHolder: {
    overflow: 'hidden',
    margin: '3px -6px 3px'
  }
};
