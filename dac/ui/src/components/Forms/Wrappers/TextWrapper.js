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
import TextField from 'components/Fields/TextField';
import FieldWithError from 'components/Fields/FieldWithError';
import FormUtils from 'utils/FormUtils/FormUtils';

import PropTypes from 'prop-types';

import { flexContainer, fieldWithError, textFieldWrapper, textFieldBody } from './FormWrappers.less';

export default class TextWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  onChangeHandler = (e) => {
    const {elementConfig, field} = this.props;
    if (elementConfig && elementConfig.getConfig().scale && field && field.onChange) {
      e.target.value = FormUtils.revertScaleValue(e.target.value, elementConfig.getConfig().scale);
      field.onChange(e);
    } else if (field && field.onChange) {
      field.onChange(e);
    }
  };

  render() {
    const {elementConfig, field} = this.props;
    const elementConfigJson = elementConfig.getConfig();
    const numberField = (elementConfig.getType() === 'number') ? {type: 'number'} : null;
    const passwordField = (elementConfigJson.secure) ? {type: 'password'} : null;
    const hoverHelpText = (elementConfigJson.tooltip) ? {hoverHelpText: elementConfigJson.tooltip} : null;
    const isDisabled = (elementConfigJson.disabled ||
      this.props.disabled ||
      // special case in source forms: source name can not be changed after initial creation
      elementConfig.getPropName() === 'name' && this.props.editing) ? {disabled: true} : null;

    const {value, onChange, onBlur, onUpdate, ...fieldProps} = field;
    const currentValue = (numberField) ? FormUtils.scaleValue(value, elementConfigJson.scale) : value;
    const onChangeHandler = (numberField) ? this.onChangeHandler : onChange;

    return (
      <div className={flexContainer}>
        <FieldWithError errorPlacement='top'
                        {...field}
                        {...hoverHelpText}
                        label={elementConfigJson.label}
                        name={elementConfig.getPropName()}
                        className={fieldWithError}>
          <div className={textFieldWrapper}>
            <TextField initialFocus={elementConfigJson.focus}
                       {...fieldProps}
                       {...numberField}
                       {...passwordField}
                       {...isDisabled}
                       value={currentValue}
                       onChange={onChangeHandler}
                       placeholder={elementConfigJson.placeholder || ''}
                       className={textFieldBody}/>
          </div>
        </FieldWithError>
      </div>
    );
  }
}
