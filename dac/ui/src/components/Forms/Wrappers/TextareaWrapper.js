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
import TextArea from 'components/Fields/TextArea';
import FieldWithError from 'components/Fields/FieldWithError';
import PropTypes from 'prop-types';

import { flexContainer, fieldWithError, textFieldWrapper, textAreaBody } from './FormWrappers.less';

export default class TextareaWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  render() {
    const {elementConfig, field} = this.props;
    const tooltip = elementConfig.getConfig().tooltip;
    const hoverHelpText = (tooltip) ? {hoverHelpText: tooltip} : null;
    const isDisabled = (elementConfig.getConfig().disabled || this.props.disabled) ? {disabled: true} : null;

    return (
      <div className={flexContainer}>
        <FieldWithError
          {...hoverHelpText}
          label={elementConfig.getConfig().label}
          name={elementConfig.getPropName()}
          className={fieldWithError}>
          <div className={textFieldWrapper}>
            <TextArea {...field} {...isDisabled} className={textAreaBody}/>
          </div>
        </FieldWithError>
      </div>
    );
  }
}
