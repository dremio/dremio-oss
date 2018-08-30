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
import PropTypes from 'prop-types';
import { TextField, Select } from 'components/Fields';

const WEEK_IN_MILLISECONDS = 604800000;
const DAY_IN_MILLISECONDS = 86400000;
const HOUR_IN_MILLISECONDS = 3600000;
const MINUTES_IN_MILLISECONDS = 60000;
const MINUTES = 'MINUTES';
const HOURS = 'HOURS';
const DAYS = 'DAYS';
const WEEKS = 'WEEKS';

// WARING: legacy
// you probably want components/Fields/DurationField instead

export default class DurationInput extends Component {

  static propTypes = {
    fields: PropTypes.object,
    style: PropTypes.object,
    disabled: PropTypes.bool
  };

  periodOptions = [ // todo: proper (localized) pluralization
    { label: la('Minute(s)'), option: MINUTES },
    { label: la('Hour(s)'), option: HOURS },
    { label: la('Day(s)'), option: DAYS },
    { label: la('Week(s)'), option: WEEKS }
  ];

  static constructFields = (duration) => {
    if (duration >= WEEK_IN_MILLISECONDS) {
      return {
        unit: WEEKS,
        duration: Math.ceil(duration / WEEK_IN_MILLISECONDS)
      };
    } else if (duration >= DAY_IN_MILLISECONDS) {
      return {
        unit: DAYS,
        duration: Math.ceil(duration / DAY_IN_MILLISECONDS)
      };
    } else if (duration >= HOUR_IN_MILLISECONDS) {
      return {
        unit: HOURS,
        duration: Math.ceil(duration / HOUR_IN_MILLISECONDS)
      };
    }
    return {
      unit: MINUTES,
      duration: Math.ceil(duration / MINUTES_IN_MILLISECONDS)
    };
  };

  static convertToMilliseconds = (option) => {
    switch (option.unit) {
    case WEEKS:
      return option.duration * WEEK_IN_MILLISECONDS;
    case DAYS:
      return option.duration * DAY_IN_MILLISECONDS;
    case HOURS:
      return option.duration * HOUR_IN_MILLISECONDS;
    case MINUTES:
      return option.duration * MINUTES_IN_MILLISECONDS;
    default:
      return option.duration;
    }
  };

  render() {
    const { style, disabled, fields: { duration, unit } } = this.props;

    return (
      <div style={{...style, display: 'flex'}}>
        <TextField disabled={disabled} {...duration} type='number' style={styles.numberInput} step={1} min={1} />
        <Select
          {...unit}
          items={this.periodOptions}
          style={styles.select}
          disabled={disabled}
        />
      </div>
    );
  }
}

const styles = {
  select: {
    width: 164,
    textAlign: 'left'
  },
  numberInput: {
    width: 42
  }
};
