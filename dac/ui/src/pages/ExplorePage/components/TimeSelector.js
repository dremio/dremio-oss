/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import Select from 'react-select';

const HOURS = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'];
const MINUTES = ['0', '10', '20', '30', '40', '50', '59'];

class TimeSelector extends Component {
  static propTypes = {
    setParameters: PropTypes.func.isRequired,
    hours: PropTypes.object,
    part: PropTypes.object,
    minutes: PropTypes.string
  };

  constructor(props) {
    super(props);
    this.setHours = this.setHours.bind(this);
    this.setMinutes = this.setMinutes.bind(this);
    this.setPart = this.setPart.bind(this);
  }

  setHours(value) {
    this.props.setParameters(value, 'hours', 'time');
  }

  setMinutes(value) {
    this.props.setParameters(value, 'minutes', 'time');
  }

  setPart(value) {
    this.props.setParameters(value, 'part', 'time');
  }

  mapForSelect(items) {
    return  items.map((item) => {
      return {value: item, label: item};
    });
  }

  render() {
    return (
      <div>
        <Select
          clearable={false}
          name='hours'
          value={this.props.hours}
          options={this.mapForSelect(HOURS)}
          onChange={this.setHours}/>
        <span className='time-text'>:</span>
        <Select
          clearable={false}
          name='minutes'
          value={this.props.minutes}
          options={this.mapForSelect(MINUTES)}
          onChange={this.setMinutes}/>
        <Select
          clearable={false}
          name='part'
          value={this.props.part}
          options={this.mapForSelect(['AM', 'PM'])}
          onChange={this.setPart}/>
      </div>
    );
  }
}

export default TimeSelector;
