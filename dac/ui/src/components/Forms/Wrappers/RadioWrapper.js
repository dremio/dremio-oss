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
import Radio from 'components/Fields/Radio';
import HoverHelp from 'components/HoverHelp';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import { rowOfInputsSpacing } from '@app/uiTheme/less/forms.less';
import {
  radioColumnWrapper,
  radioTopLabel,
  radioBody,
  radioBodyColumn,
  tooltipIcon,
  flexContainer
} from './FormWrappers.less';


export default class RadioWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool
  };

  render() {
    const {elementConfig, field} = this.props;
    const isLayoutColumn = elementConfig.getConfig().layout === 'column';
    const label = elementConfig.getConfig().label;
    const tooltip = elementConfig.getConfig().tooltip;

    return (
      <div className={flexContainer}>
        <div className={classNames({[radioColumnWrapper]: isLayoutColumn, [rowOfInputsSpacing]: !isLayoutColumn})}>
          {label &&
            <div className={radioTopLabel}>
              {label}
              {tooltip && <HoverHelp content={tooltip} className={tooltipIcon} iconStyle={{marginTop: -3}}/>}
            </div>
          }
          {elementConfig.getConfig().options.map((option, index) => {
            const radioClassName = classNames(radioBody, {[radioBodyColumn]: isLayoutColumn});
            return (
              <Radio key={index} label={option.label || option.value} radioValue={option.value} {...field} className={radioClassName}/>
            );
          })}
        </div>
      </div>
    );
  }
}
