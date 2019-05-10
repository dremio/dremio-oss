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
import { Fragment, PureComponent } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { DateRange } from 'react-date-range';
import moment from 'moment';

import { SelectView } from '@app/components/Fields/SelectView';
import FontIcon from 'components/Icon/FontIcon';

import { PALE_NAVY } from 'uiTheme/radium/colors';
import { dateTypeToFormat, TIME, DATE } from 'constants/DataTypes';

import TimePicker from './TimePicker';

@Radium
export default class DateInput extends PureComponent {

  static propTypes = {
    onChange: PropTypes.func,
    value: PropTypes.string,
    disabled: PropTypes.bool,
    type: PropTypes.string
  };

  static mergeDateWithTime(dateMoment, timeMoment, type) {
    const newMoment = dateMoment.clone().startOf('day');
    const justTime = timeMoment.diff(timeMoment.clone().startOf('day'));
    newMoment.add(justTime, 'milliseconds');
    return newMoment.format(dateTypeToFormat[type]);
  }

  constructor(props) {
    super(props);

    this.state = {
      value: props.value
    };
  }

  componentWillReceiveProps(nextProps) {
    const { value } = nextProps;
    if (value !== '') {
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

    const dateMoment = date.startDate;
    const newValue = dateMoment.format(dateTypeToFormat[type]);
    if (onChange && newValue !== value) {
      const currentMoment = moment(value, dateTypeToFormat[type]);
      onChange(DateInput.mergeDateWithTime(dateMoment, currentMoment, type));
      closeDD();
    }
  }
  handleTimeChange = (timeMoment) => {
    const { onChange, value, type } = this.props;
    if (onChange) {
      const dateMoment = !value ? moment().startOf('day') : moment(value, dateTypeToFormat[type]);
      onChange(DateInput.mergeDateWithTime(dateMoment, timeMoment, type));
    }
  }
  handleInputChange = (e) => {
    this.setState({ value: e.target.value });
  }
  handleInputBlur = () => {
    const { value } = this.state;
    const { onChange, type } = this.props;
    const valueMoment = moment(value, dateTypeToFormat[type]);
    if (valueMoment.isValid()) {
      onChange(value);
    }
  }
  renderCalendar(closeDD) {
    const { value, type } = this.props;
    const showTimePicker = type !== DATE;
    const showCalendar = type !== TIME;
    const date = !value ? moment().startOf('day') : moment(value);
    const time = value || moment().format('hh:mm:ss');

    return (
      <div>
        {showTimePicker
          ? <TimePicker key='time-picker' columnType={type} value={time} onBlur={this.handleTimeChange} />
          : null}
        {showCalendar
          ? <DateRange
            key='date-range'
            calendars='3'
            linkedCalendars
            format={dateTypeToFormat[type]}
            theme={calendarTheme}
            classNames={{ dayToday: 'today' }}
            onChange={this.handleCalendarSelect.bind(this, closeDD)}
            startDate={date}
            endDate={date}
          />
          : null}
      </div>
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
              key='date-input'
              ref='input'
              style={[styles.input, props.style]}
              type='text'
              value={state.value}
              onChange={this.handleInputChange}
              onBlur={this.handleInputBlur}
            />
            <FontIcon
              key='date-icon'
              type='TypeDateTime'
              theme={styles.icon}
            />
          </Fragment>
        }
        hideExpandIcon
        className='field'
        style={styles.base}
        useLayerForClickAway={false}
      >
        {
          ({ closeDD }) => (
            <div style={popoverStyle}>
              {this.renderCalendar(closeDD)}
            </div>
          )
        }
      </SelectView>
    );
  }
}

const calendarTheme = {
  'DateRange': {
    'background': 'inherit',
    'margin': '4px auto 4px auto',
    'width': 430
  },
  'Calendar': {
    'background': 'inherit',
    'boxSizing': 'content-box',
    'width': '140',
    'padding': '0'
  },

  'MonthAndYear': {
    'padding': 0,
    'color': '#949494',
    'height': 20
  },
  'MonthButton': {
    'margin': 0,
    'background': 'inherit'
  },
  'MonthArrow': {
    'border': '6px solid transparent'
  },
  'MonthArrowPrev': {
    'borderRightColor': '#a4a4a4'
  },
  'MonthArrowNext': {
    'borderLeftColor': '#a4a4a4'
  },
  'Weekday': {
    'lineHeight': '16px',
    'letterSpacing': 'none',
    'height': 17,
    'color': '#707070',
    'fontWeight': 0,
    'fontSize': 11,
    'width': 19,
    'marginBottom': 1
  },
  'DayInRange': {
    'background': PALE_NAVY,
    'color': '#333'
  },
  'DayStartEdge': {
    'background': PALE_NAVY,
    'borderRadius': '15px 0 0 15px',
    'color': '#333'
  },
  'DayEndEdge': {
    'background': PALE_NAVY,
    'borderRadius': '0 15px 15px 0',
    'color': '#333'
  },
  'Day': {
    'width': 19,
    'marginBottom': 1,
    'borderBottom': '1px solid rgba(0,0,0,.1)',
    'color': '#333'
  },
  'DayHover': {
    'color': '#f4f4f4'
  }
};

const styles = {
  base: {
    width: 310,
    display: 'flex',
    height: 24,
    position: 'relative'
  },
  dateTimePopover: {
    height: 220
  },
  timePopover: {
    height: 33
  },
  datePopover: {
    height: 180
  },
  input: {
    width: 300,
    height: 24,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    marginLeft: 10,
    float: 'left',
    padding: 2
  },
  icon: {
    Container: {
      position: 'relative',
      width: 34,
      right: 40,
      cursor: 'pointer'
    },
    Icon: {
      width: 30,
      height: 24
    }
  }
};
