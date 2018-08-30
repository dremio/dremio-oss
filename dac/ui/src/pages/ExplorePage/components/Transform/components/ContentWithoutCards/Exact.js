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
import Immutable from 'immutable';

import { Radio, TextField, DateInput } from 'components/Fields';
import { isDateType } from 'constants/DataTypes';
import { rowOfInputsSpacing } from '@app/uiTheme/less/forms.less';
import { sectionMargin } from '@app/uiTheme/less/layout.less';

import CardFooter from './../CardFooter';

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
      style={styles.input}
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
        <div className={rowOfInputsSpacing}>
          <Radio
            radioValue='Value'
            onChange={this.handleRadioChange}
            label='Value:'
            name='Value:'
            checked={!replaceNull.value}/>
          {this.renderInput()}
          <Radio
            radioValue='Null'
            onChange={this.handleRadioChange}
            label='Null'
            checked={replaceNull.value}/>
        </div>
        <div className={sectionMargin}>
          <CardFooter card={data}/>
        </div>
      </div>
    );
  }
}

const styles = {
  input: {
    width: 300,
    height: 24,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    padding: 2
  }
};
