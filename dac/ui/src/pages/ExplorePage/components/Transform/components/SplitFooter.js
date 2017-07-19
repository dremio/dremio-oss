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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import NewFieldSection from 'components/Forms/NewFieldSection';
import Select from 'components/Fields/Select';
import TextField from 'components/Fields/TextField';
import FieldWithError from 'components/Fields/FieldWithError';
import { applyValidators, isRequired } from 'utils/validation';
import { LINE_START_CENTER, FLEX_WRAP_COL_CENTER } from 'uiTheme/radium/flexStyle';
import { formLabel, body } from 'uiTheme/radium/typography';

@PureRender
@Radium
export default class SplitFooter extends Component {
  static getFields() {
    return ['position', 'index', 'maxFields', ...NewFieldSection.getFields()];
  }

  static validate(values) {
    const {position} = values;
    const validators = [isRequired('position', 'Position')];
    if (position === 'Index') {
      validators.push(isRequired('index', 'Index'));
    }
    if (position === 'All') {
      validators.push(isRequired('maxFields', 'All'));
    }
    return applyValidators(values, validators);
  }

  static propTypes = {
    columnName: PropTypes.string,
    positionSplit: PropTypes.number,
    fields: PropTypes.object,
    handleSplitFooter: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.options = [
      {
        label: 'All',
        option: 'All'
      },
      {
        label: 'First',
        option: 'First'
      },
      {
        label: 'Last',
        option: 'Last'
      },
      {
        label: 'Index',
        option: 'Index'
      }
    ];
  }

  renderTextField() {
    const { fields: {position, maxFields, index} } = this.props;
    if (position.value === 'Index') {
      return (
        <FieldWithError {...index} errorPlacement='top'>
          <div style={styles.wrap}>
            <span style={formLabel}>{la('[Index] =')}</span>
            <TextField
              type='number'
              style={styles.textField}
              {...index}
              type='number'/>
          </div>
        </FieldWithError>
      );
    } else if (position.value === 'All') {
      return (
        <FieldWithError {...maxFields} errorPlacement='top'>
          <div style={styles.wrap}>
            <span style={formLabel}>{la('Max Fields')}</span>
            <TextField
              type='number'
              style={styles.textField}
              {...maxFields}
              type='number'/>
          </div>
        </FieldWithError>
      );
    }
  }

  render() {
    const { fields: { position } } = this.props;
    return (
      <div style={{...LINE_START_CENTER, marginBottom: 5}} className='extract-footer'>
        <div style={styles.wrap}>
          <span style={formLabel}>{la('Position')}</span>
          <Select
            items={this.options}
            style={styles.select}
            {...position}/>
        </div>
        {this.renderTextField()}
        <NewFieldSection fields={this.props.fields}/>
      </div>
    );
  }
}

const styles = {
  wrap: {
    ...FLEX_WRAP_COL_CENTER,
    marginLeft: 10,
    marginBottom: 10
  },
  textField: {
    width: 100,
    height: 24
  },
  select: {
    marginLeft: -2,
    width: 100,
    height: 24,
    ...body
  }
};
