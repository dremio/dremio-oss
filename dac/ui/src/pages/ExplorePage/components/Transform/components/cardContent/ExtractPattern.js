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

import Select from 'components/Fields/Select';
import Checkbox from 'components/Fields/Checkbox';
import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';

import { applyValidators, isRequired, isRegularExpression } from 'utils/validation';

import { bodySmall } from 'uiTheme/radium/typography';
import { inlineFieldWrap, inlineLabel } from 'uiTheme/radium/exploreTransform';

// todo: loc

@Radium
class ExtractPattern extends Component {
  static getFields() {
    return [
      'pattern.value.index',
      'pattern.value.value',
      'pattern.pattern',
      'ignoreCase'
    ];
  }

  static validate(values) {
    const {pattern} = values;
    const value = pattern.value.value;
    const validators = [isRequired('pattern.pattern', 'Pattern'), isRegularExpression('pattern.pattern')];
    if (value === 'CAPTURE_GROUP' || value === 'INDEX') {
      validators.push(isRequired('pattern.value.index', 'Index'));
    }
    return applyValidators(values, validators);
  }

  static propTypes = {
    columnName: PropTypes.string,
    fields: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.items = [
      {
        option: 'FIRST',
        label: 'First'
      },
      {
        option: 'LAST',
        label: 'Last'
      },
      {
        option: 'INDEX',
        label: 'Index…'
      },
      {
        option: 'CAPTURE_GROUP',
        label: 'Capture Group…'
      }
    ];
  }

  render() {
    const { fields, columnName } = this.props;
    if (!fields) {
      return <div/>;
    }
    const {value, pattern} = fields.pattern;

    return (
      <div style={{height: 87}}>
        <div style={[inlineFieldWrap, {maxWidth: 550}]}>
          <span style={inlineLabel}>Extract</span>
          <Select
            dataQa='PatternValue'
            items={this.items}
            style={{width: 200, ...bodySmall}}
            {...value.value}
            />
          {(value.value.value === 'CAPTURE_GROUP' || value.value.value === 'INDEX') &&
          <FieldWithError {...value.index} errorPlacement='right'>
            <input
              data-qa='PatternIndex'
              type='number'
              style={[styles.input, {marginLeft: 10, width: 100}]}
              {...value.index}/>
          </FieldWithError>
            }
        </div>
        <div style={styles.item}>
          <FieldWithError
            {...pattern}
            errorPlacement='bottom'
            label='Regular Expression'
            >
            <TextField
              data-qa='PatternExpression'
              {...pattern}
              style={styles.text}/>
          </FieldWithError>
        </div>
        <div style={[styles.item, styles.checkboxItem]}>
          <Checkbox
            {...fields.ignoreCase}
            label={`Ignore case (${columnName})`}
            style={styles.checkbox}/>
        </div>
      </div>
    );
  }
}

export default ExtractPattern;

const styles = {
  item: {
    maxWidth: 200,
    marginLeft: 10,
    marginTop: 5,
    fontWeight: 400
  },
  text: {
    width: 230,
    height: 28
  },
  wrap: {
    display: 'flex',
    alignItems: 'center',
    flexWrap: 'wrap'
  },
  checkbox: {
    width: 180
  },
  checkboxFont: {
    fontWeight: 400
  },
  checkboxItem: {
    display: 'flex',
    alignItems: 'center',
    marginTop: 6,
    position: 'relative',
    left: 250,
    top: -30
  },
  input: {
    width: 230,
    height: 28,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    padding: 2,
    marginTop: -5
  },
  font: {
    fontWeight: 600,
    fontSize: 13,
    margin: 0
  }
};
