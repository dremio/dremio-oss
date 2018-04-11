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
import Immutable from 'immutable';

import { Radio, TextField, DateInput } from 'components/Fields';
import { isDateType } from 'constants/DataTypes';

import CardFooter from './../CardFooter';

@Radium
export default class Exact extends Component {
  static getFields() {
    return ['replaceValues[]', 'replaceNull'];
  }

  static propTypes = {
    columnType: PropTypes.string.isRequired,
    submitForm: PropTypes.func,
    replaceValues: PropTypes.object,
    replaceNull: PropTypes.object,
    matchedCount: PropTypes.number,
    unmatchedCount: PropTypes.number
  };

  handleRadioChange = (event) => {
    const { replaceNull } = this.props;
    if (event.target.value === 'Null') {
      replaceNull.onChange(true);
    } else {
      replaceNull.onChange(false);
    }
  }

  renderInput() {
    const { columnType, replaceValues, replaceNull } = this.props;
    return isDateType(columnType)
      ? (
        <DateInput type={columnType} {...replaceValues} disabled={replaceNull.value} />
    )
    : <TextField
      type='number'
      disabled={replaceNull.value}
      style={[styles.input, { marginLeft: 10, width: 300 }]}
      {...replaceValues}
      />;
  }

  render() {
    const { replaceNull, matchedCount, unmatchedCount } = this.props;
    const data = Immutable.Map({
      matchedCount,
      unmatchedCount
    });
    return (
      <div className='exact'>
        <div style={styles.wrap}>
          <Radio
            radioValue='Value'
            onChange={this.handleRadioChange}
            label='Value:'
            name='Value:'
            style={styles.check}
            checked={!replaceNull.value}/>
          {this.renderInput()}
          <Radio
            radioValue='Null'
            onChange={this.handleRadioChange}
            label='Null'
            style={{ ...styles.check, marginLeft: -5 }}
            checked={replaceNull.value}/>
        </div>
        <div style={styles.wrapFooter}>
          <CardFooter card={data}/>
        </div>
      </div>
    );
  }
}

const styles = {
  wrap: {
    margin: '0 10px 0 5px',
    height: 24,
    display: 'flex'
  },
  wrapFooter: {
    marginTop: 10,
    marginLeft: 10,
    position: 'relative'
  },
  input: {
    width: 230,
    height: 24,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    float: 'left',
    padding: 2
  },
  check: {
    float: 'left',
    marginTop: 5
  }
};
