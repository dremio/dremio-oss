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
import ByteField from 'components/Fields/ByteField';
import FieldWithError from 'components/Fields/FieldWithError';
import FormUtils from 'utils/FormUtils/FormUtils';
import PropTypes from 'prop-types';

// style classes the same as for duration field
import { durationLabel, durationBody, fieldWithError } from './FormWrappers.less';

export default class ByteWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  render() {
    const {elementConfig, field, disabled} = this.props;
    const valueMultiplier = elementConfig.getConfig().multiplier; // for example (1024 ** 2) if API uses MBs instead of bytes
    const { value, ...fieldProps } = field;
    const adjustedValue = (value === '') ? 0 : value;
    return (
      <div>
        <div className={durationLabel}>{elementConfig.getConfig().label}</div>
        <FieldWithError errorPlacement='top'
          {...field}
          className={fieldWithError}
          name={elementConfig.getPropName()}>
          <ByteField {...fieldProps}
            value={adjustedValue}
            min={FormUtils.getMinByte(elementConfig.getConfig().minOption)}
            disabled={disabled}
            valueMultiplier={valueMultiplier}
            className={durationBody}/>
        </FieldWithError>
      </div>
    );
  }
}

