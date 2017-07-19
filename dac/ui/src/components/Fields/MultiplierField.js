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

// use a PrevalidatedTextField as a buffer to that you can temporarily have invalid numbers while typing (e.g. empty-string)
import PrevalidatedTextField from 'components/Fields/PrevalidatedTextField';
import Select from 'components/Fields/Select';

export default class MultiplierField extends Component {

  static propTypes = {
    error: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    disabled: PropTypes.bool, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    value: PropTypes.number,
    style: PropTypes.object,
    unitMultipliers: PropTypes.instanceOf(Map).isRequired
  };

  state = {
    unit: null
  }

  handleTextChange = (value) => {
    this.setState({unit: this.getUnit()}); // need to lock in the unit once the user starts making changes
    this.props.onChange(+value * this.props.unitMultipliers.get(this.state.unit));
  }

  handleSelectChange = (unit) => {
    this.setState({unit});
  }

  getUnit() {
    if (this.state.unit) return this.state.unit;

    const { value } = this.props;

    let unit = Array.from(this.props.unitMultipliers.keys())[0];

    if (typeof value !== 'number' || Number.isNaN(value)) {
      return unit;
    }

    for (const [key, multiplier] of this.props.unitMultipliers) {
      if (value < multiplier) break;
      if (stringifyWithoutExponent(value / multiplier).match(/\.[0-9]{3}/)) break;
      unit = key;
    }
    return unit;
  }

  render() {
    let convertedValue = this.props.value / this.props.unitMultipliers.get(this.getUnit());
    if (Number.isNaN(convertedValue)) {
      convertedValue = '0';
    } else {
      convertedValue = stringifyWithoutExponent(convertedValue);
    }

    return <span style={{display: 'inline-flex', ...this.props.style}}>
      <PrevalidatedTextField
        type='number'
        value={convertedValue}
        error={this.props.error}
        touched={this.props.touched}
        disabled={this.props.disabled}
        onChange={this.handleTextChange}
        style={{width: 180}}
      />
      <span style={{display: 'inline-block'}}>
        <Select
          items={Array.from(this.props.unitMultipliers.keys()).map(size => ({label: size}))}
          value={this.getUnit()}
          disabled={this.props.disabled}
          onChange={this.handleSelectChange}
          style={{width: 120}}
        />
      </span>
    </span>;
  }
}

// todo: loc
function stringifyWithoutExponent(number) {
  return number.toFixed(20).replace(/\.?0+$/, '');
}
