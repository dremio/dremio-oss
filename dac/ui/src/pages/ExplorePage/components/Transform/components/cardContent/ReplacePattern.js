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

import { FieldWithError, TextField, Checkbox } from 'components/Fields';

import { applyValidators, isRegularExpression } from 'utils/validation';

@Radium
export default class ReplacePattern extends Component {
  static getFields() {
    return [
      'replace.selectionPattern',
      'replace.ignoreCase',
      'replace.selectionType'
    ];
  }

  static propTypes = {
    fields: PropTypes.object
  };

  static validate(values) {
    const {selectionType} = values.replace;
    const validators = [];
    if (selectionType === 'MATCHES') {
      validators.push(isRegularExpression('replace.selectionPattern'));
    }
    return applyValidators(values, validators);
  }

  constructor(props) {
    super(props);
    this.items = [
      {
        option: 'CONTAINS',
        label: 'Contains'
      },
      {
        option: 'STARTS_WITH',
        label: 'Starts with'
      },
      {
        option: 'ENDS_WITH',
        label: 'Ends with'
      },
      {
        option: 'MATCHES',
        label: 'Matches regex'
      },
      {
        option: 'EXACT',
        label: 'Exactly matches'
      },
      {
        option: 'IS_NULL',
        label: 'Is null'
      }
    ];
  }

  render() {
    const { fields: { replace: {ignoreCase, selectionPattern, selectionType}} } = this.props;
    return (
      <div style={styles.base}>
        <Select
          dataQa='ReplacePatternSelect'
          items={this.items}
          style={styles.select}
          {...selectionType}
          defaultItem={selectionType.value}/>
        <div>
          {selectionType.value !== 'IS_NULL' &&
            <FieldWithError
              {...selectionPattern}
              errorPlacement='right'>
              <TextField
                data-qa='ReplacePatternText'
                {...selectionPattern}
                style={[styles.input]}/>
            </FieldWithError>
          }
          <div style={[styles.input]}>
            <Checkbox
              {...ignoreCase}
              label={la('Ignore case')}/>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    marginTop: 10,
    marginLeft: 10
  },
  select: {
    width: 200
  },
  input: {
    width: 200,
    margin: '0 10px'
  }
};
