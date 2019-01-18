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
import Select from 'components/Fields/Select';
import FieldWithError from 'components/Fields/FieldWithError';
import PropTypes from 'prop-types';
import { flexContainer, selectWrapper, selectBody, selectFieldWithError } from './FormWrappers.less';

export default class SelectWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  render() {
    const {elementConfig, field} = this.props;
    const isDisabled = (elementConfig.getConfig().disabled || this.props.disabled) ? {disabled: true} : null;
    const tooltip = elementConfig.getConfig().tooltip;
    const hoverHelpText = (tooltip) ? {hoverHelpText: tooltip} : null;

    return (
      <div className={flexContainer}>
        <FieldWithError errorPlacement='top'
                        {...hoverHelpText}
                        label={elementConfig.getConfig().label}
                        labelClass={selectFieldWithError} >
          <div className={selectWrapper}>
            <Select
              {...isDisabled}
              items={elementConfig.getConfig().options}
              className={selectBody}
              {...field} />
          </div>
        </FieldWithError>
      </div>
    );
  }
}
