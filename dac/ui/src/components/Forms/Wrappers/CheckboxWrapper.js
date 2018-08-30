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
import Checkbox from 'components/Fields/Checkbox';
import HoverHelp from 'components/HoverHelp';
import { checkboxStandalone } from '@app/components/Fields/Checkbox.less';
import PropTypes from 'prop-types';

import { flexContainer, tooltipIcon } from './FormWrappers.less';

export default class CheckboxWrapper extends Component {
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
    const isInverted = (elementConfig.getConfig().inverted) ? {inverted: true} : null;
    const tooltip = elementConfig.getConfig().tooltip;
    return (
      <div className={flexContainer}>
        <Checkbox className={checkboxStandalone}
                  {...field}
                  {...isDisabled}
                  {...isInverted}
                  label={elementConfig.getConfig().label}/>
        {tooltip &&
        <HoverHelp content={tooltip} className={tooltipIcon}/>
        }
      </div>
    );
  }
}
