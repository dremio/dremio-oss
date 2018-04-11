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

import PropTypes from 'prop-types';

import { Radio, PrevalidatedTextField, DateInput } from 'components/Fields';
import { isDateType } from 'constants/DataTypes';
import { formContext } from 'uiTheme/radium/typography';
import { LINE_START_CENTER } from 'uiTheme/radium/flexStyle';

@Radium
export default class TransformRangeBound extends Component {

  static propTypes = {
    defaultValue: PropTypes.string,
    columnType: PropTypes.string,
    noneLabel: PropTypes.string,
    field: PropTypes.object,
    fieldName: PropTypes.string,
    style: PropTypes.array,
    validate: PropTypes.func
  };

  constructor(props) {
    super(props);

    const { field: { value }} = props;
    this.state = {
      customValue: value || ''
    };
  }

  componentWillReceiveProps(nextProps) {
    const { field: { value }} = nextProps;
    if (value !== '') {
      this.setState({ customValue: value });
    }
  }

  handleRadioChange = (event) => {
    const { field, defaultValue } = this.props;
    if (event.target.value === 'none') {
      field.onChange('');
    } else {
      field.onChange(this.state.customValue || defaultValue);
    }
  }

  handleValueChange = (newValue) => {
    const { field: { value }} = this.props;

    if (value === newValue) {
      return;
    }

    if (newValue === '') {
      this.setState({ customValue: '' });
    } else {
      this.props.field.onChange(newValue);
    }
  }

  renderInput() {
    const { columnType } = this.props;
    return isDateType(columnType)
      ? (
        <DateInput
          type={columnType}
          style={styles.dateInput}
          value={this.state.customValue}
          onChange={this.handleValueChange}
      />
    )
    : <PrevalidatedTextField
      style={styles.input}
      value={this.state.customValue}
      onChange={this.handleValueChange}
      validate={this.props.validate}
    />;
  }

  render() {
    const { style, noneLabel, field, fieldName } = this.props;

    const hasBound = field.value !== '';
    return <div data-qa={fieldName} style={style}>
      <Radio
        onChange={this.handleRadioChange}
        style={styles.radio}
        label={noneLabel}
        name={fieldName}
        radioValue='none'
        checked={!hasBound}
      />
      <Radio
        onChange={this.handleRadioChange}
        style={styles.radioBottom}
        label={this.renderInput()}
        name='upper'
        radioValue='custom'
        checked={hasBound}
      />
      <span style={[formContext, { marginLeft: 20 }]}>
        {fieldName === 'lower' ? la('inclusive') : la('exclusive')}
      </span>
    </div>;
  }
}

const styles = {
  dateInput: {
    width: 195,
    marginLeft: 0
  },
  radio: {
    ...LINE_START_CENTER
  },
  radioBottom: {
    ...LINE_START_CENTER
  },
  input: {
    width: 100,
    height: 22,
    outline: 'none',
    position: 'relative',
    marginTop: 0,
    border: '1px solid rgba(0,0,0,0.10)',
    borderRadius: 2
  }
};
