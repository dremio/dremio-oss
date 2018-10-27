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
import classNames from 'classnames';

// use a PrevalidatedTextField as a buffer to that you can temporarily have invalid numbers while typing (e.g. empty-string)
import PrevalidatedTextField from 'components/Fields/PrevalidatedTextField';
import Select from 'components/Fields/Select';
import { rowOfInputsSpacing } from '@app/uiTheme/less/forms.less';

export default class MultiplierField extends Component {

  static propTypes = {
    error: PropTypes.string,
    onChange: PropTypes.func.isRequired,
    touched: PropTypes.bool,
    disabled: PropTypes.bool, // todo: add a #readonly/readOnly(?) and switch existing uses of #disabled as appropriate)
    value: PropTypes.number,
    valueMultiplier: PropTypes.number,
    style: PropTypes.object,
    className: PropTypes.string,
    unitMultipliers: PropTypes.instanceOf(Map).isRequired,
    min: PropTypes.number.isRequired // this UI only enforces the units displayed, your form should have its own validation (BE and/or FE)
  };

  static defaultProps = {
    min: 0
  }

  state = {
    unit: null
  }

  handleTextChange = (value) => {
    const {valueMultiplier, unitMultipliers, onChange} = this.props;
    const unit = this.getUnit();
    this.setState({unit}); // need to lock in the unit once the user starts making changes

    let valueWithMultipliers = +value * unitMultipliers.get(unit);
    if (valueMultiplier) {
      valueWithMultipliers /= valueMultiplier;
    }
    onChange(valueWithMultipliers);
  }

  handleSelectChange = (unit) => {
    const {valueMultiplier, unitMultipliers, onChange} = this.props;
    // keep the same displayed number even when changing units (DX-9129)
    let valueWithMultipliers = this.getConvertedNumberForDisplay() * unitMultipliers.get(unit);
    if (valueMultiplier) {
      valueWithMultipliers /= valueMultiplier;
    }
    onChange(valueWithMultipliers);
    this.setState({unit});
  }

  getFilteredUnitMultipliers() {
    const unitMultipliersArray = [...this.props.unitMultipliers];
    let i = unitMultipliersArray.length;
    while (i--) {
      const unitMultiplier = unitMultipliersArray[i];
      const multiplier = unitMultiplier[1];
      if (this.props.min >= multiplier) break;
    }

    return new Map(unitMultipliersArray.slice(Math.max(0, i)));
  }

  getUnit() {
    if (this.state.unit) return this.state.unit;

    const { value } = this.props;

    let unit = Array.from(this.getFilteredUnitMultipliers().keys())[0];

    if (typeof value !== 'number' || Number.isNaN(value)) {
      return unit;
    }

    for (const [key, multiplier] of this.getFilteredUnitMultipliers()) {
      if (value < multiplier) break;
      if (stringifyWithoutExponent(value / multiplier).match(/\.[0-9]{3}/)) break;
      unit = key;
    }
    return unit;
  }

  getConvertedNumberForDisplay() {
    const {value, valueMultiplier, unitMultipliers} = this.props;
    const valueWithMultiplier = (valueMultiplier) ? value * valueMultiplier : value;
    let convertedValue = valueWithMultiplier / unitMultipliers.get(this.getUnit());
    if (Number.isNaN(convertedValue)) {
      convertedValue = 0;
    }
    return convertedValue;
  }

  render() {
    const { className, style } = this.props;
    return <span className={classNames(['field', rowOfInputsSpacing, className])} style={{...styles.base, ...style}}>
      <PrevalidatedTextField
        type='number'
        value={stringifyWithoutExponent(this.getConvertedNumberForDisplay())}
        error={this.props.error}
        touched={this.props.touched}
        disabled={this.props.disabled}
        onChange={this.handleTextChange}
        style={styles.textField}
      />
      <span style={{display: 'inline-block'}}>
        <Select
          items={Array.from(this.getFilteredUnitMultipliers().keys()).map(size => ({label: size}))}
          value={this.getUnit()}
          disabled={this.props.disabled}
          onChange={this.handleSelectChange}
          style={styles.select}
        />
      </span>
    </span>;
  }
}

// todo: loc
function stringifyWithoutExponent(number) {
  return number.toFixed(20).replace(/\.?0+$/, '');
}

const styles = {
  base: {
    display: 'inline-flex',
    width: 310
  },
  textField: {
    flexGrow: 1,
    width: 0 // override any preset width
  },
  select: {
    width: 164,
    textAlign: 'left'
  }
};
