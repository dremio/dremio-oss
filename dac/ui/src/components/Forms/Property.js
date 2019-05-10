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

import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import {RemoveButton, RemoveButtonStyles } from 'components/Fields/FieldList';

import { applyValidators, isRequired } from 'utils/validation';
import { inputSpacing as inputSpacingCssValue} from '@app/uiTheme/less/variables.less';

const inputSpacing = parseInt(inputSpacingCssValue, 10);

export default class Property extends Component {
  static getFields() {
    return [
      'id', 'name', 'value'
    ];
  }

  static propTypes = {
    fields: PropTypes.object,
    style: PropTypes.object,
    onRemove: PropTypes.func
  };

  static validate(values) {
    return applyValidators(values, [
      isRequired('name'),
      isRequired('value')
    ]);
  }

  render() {
    const {onRemove, fields: {name, value}} = this.props;
    const propertyStyle = {display: 'flex'};
    const fieldStyle = {flex: '1 1 auto'};
    const textStyle = {width: '100%'};

    return (
      <div style={propertyStyle}>
        <FieldWithError label='Name' {...name} style={{...fieldStyle, marginRight: inputSpacing}} errorPlacement='top'>
          <TextField {...name} style={textStyle} />
        </FieldWithError>
        <FieldWithError label='Value' {...value} style={fieldStyle} errorPlacement='top'>
          <TextField {...value} style={textStyle} />
        </FieldWithError>
        {onRemove && <RemoveButton onClick={onRemove} style={styles.removeButton}/> }
      </div>
    );
  }
}

const styles = {
  removeButton: {
    ...RemoveButtonStyles.inline,
    marginTop: 17
  }
};
