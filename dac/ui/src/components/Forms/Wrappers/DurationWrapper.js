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
import DurationField from 'components/Fields/DurationField';
import FieldWithError from 'components/Fields/FieldWithError';
import FormUtils from 'utils/FormUtils/FormUtils';
import PropTypes from 'prop-types';

import {durationLabel, durationBody, fieldWithError} from './FormWrappers.less';

export default class DurationWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  render() {
    const {elementConfig, field, disabled} = this.props;
    const { value, ...fieldProps } = field;
    const adjustedValue = (value === '') ? 0 : value;
    const disableIf = elementConfig.getConfig().disableIf;
    const relatedElement = disableIf
      && FormUtils.getFieldByComplexPropName(this.props.fields, disableIf.propName);
    const isDisabled = disabled || !!relatedElement && relatedElement.value === disableIf.value;
    return (
      <div>
        <div className={durationLabel}>{elementConfig.getConfig().label}</div>
        <FieldWithError errorPlacement='top'
          {...field}
          className={fieldWithError}
          name={elementConfig.getPropName()}>
          <DurationField {...fieldProps}
            name={elementConfig.getPropName()}
            value={adjustedValue}
            min={FormUtils.getMinDuration(elementConfig.getConfig().minOption)}
            disabled={isDisabled}
            className={durationBody}/>
        </FieldWithError>
      </div>
    );
  }
}

