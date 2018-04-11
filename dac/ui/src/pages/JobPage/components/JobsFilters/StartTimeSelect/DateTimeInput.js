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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import MaskedInput from './MaskedInput';

const MAX_MONTH = 12;

@Radium
@PureRender
export default class DateTimeInput extends Component {
  static propTypes = {
    date: PropTypes.object.isRequired,
    label: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired
  };

  static formatDate(inputType, inputValue, date) {
    const currentMoment = date.clone();
    if (inputType === 'timeInput') {
      const [ hour, minute ] = inputValue.split(':');
      currentMoment.hour(hour);
      currentMoment.minute(minute);
    } else {
      const inputArray = inputValue.split('/');
      const month = inputArray[0] > MAX_MONTH ? MAX_MONTH : inputArray[0];
      const day = inputArray[1] > currentMoment.daysInMonth() ? currentMoment.daysInMonth() : inputArray[1];
      const year = inputArray[2];
      if (month && month > 0) {
        currentMoment.month(month - 1);
      }
      if (day) {
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

  componentWillReceiveProps(nextProps) {
    if (this.props.date !== nextProps.date) {
      this.setState({ currentMoment: nextProps.date.clone() });
    }
  }

  onInputChange = (inputType, inputValue) => this.setState({
    currentMoment: DateTimeInput.formatDate(inputType, inputValue, this.state.currentMoment)
  });

  onBlur = () => this.props.onChange(this.props.type, this.state.currentMoment);

  render() {
    const { date, label } = this.props;
    const { currentMoment } = this.state;
    const timePlaceholder = date.format('HH:mm');
    const datePlaceholder = date.format('M/D/YYYY');
    const timeValue = currentMoment.format('HH:mm');
    const dateValue = currentMoment.format('M/D/YYYY');
    return (
      <div style={[style.base]} className='date-time-input'>
        <h5>{label}</h5>
        <div style={[style.innerBlock]}>
          <FontIcon type='Date' theme={style.iconStyle}/>
          <MaskedInput
            mask='dd/dd/dddd'
            value={dateValue}
            placeholder={datePlaceholder}
            onBlur={this.onBlur}
            onChange={this.onInputChange.bind(this, 'dateInput')}
            ref='dateInput'/>
        </div>
        <div style={[style.innerBlock]}>
          <FontIcon type='Time' theme={style.iconStyle}/>
          <MaskedInput
            mask='dd:dd'
            value={timeValue}
            placeholder={timePlaceholder}
            onBlur={this.onBlur}
            onChange={this.onInputChange.bind(this, 'timeInput')}
            ref='timeInput'/>
        </div>
      </div>);
  }
}
const style = {
  base: {
    width: 150,
    height: '100%',
    display: 'inline-block',
    margin: '5px 5px 7px 5px'
  },
  innerBlock: {
    width: '100%',
    height: 22,
    display: 'flex',
    alignItems: 'center',
    margin: '3px 0 7px',
    background: '#fff',
    border: '1px solid rgba(0,0,0,.1)',
    borderRadius: '2px'
  },
  iconStyle: {
    Container: {
      position: 'relative',
      top: 2
    },
    Icon: {
      backgroundSize: '100% 100%'
    }
  }
};
