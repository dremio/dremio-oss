/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import classNames from 'classnames';
import FormUtils from 'utils/FormUtils/FormUtils';
import FormSection from 'components/Forms/FormSection';
import FormElement from 'components/Forms/FormElement';
import Radio from 'components/Fields/Radio';
import FieldWithError from 'components/Fields/FieldWithError';

import Select from '@app/components/Fields/Select';
import {selectWrapper, selectBody, selectFieldWithError} from '@app/components/Forms/Wrappers/FormWrappers.less';
import { rowOfInputsSpacing, rowOfRadio } from '@app/uiTheme/less/forms.less';
import { componentContainer, topLabel} from './ContainerSelection.less';

export default class ContainerSelection extends Component {
  static propTypes = {
    fields: PropTypes.object,
    elementConfig: PropTypes.object,
    disabled: PropTypes.bool
  };

  renderRadioSelector = (elementConfig, selectedValue, radioProps) => {
    const label = elementConfig.getConfig().label;
    return (
      <div className={classNames(rowOfRadio, rowOfInputsSpacing)}>
        {label && <div className={topLabel}>{label}</div>}
        {elementConfig.getOptions().map((option, index) => {
          return (
            <Radio radioValue={option.value}
              value={selectedValue}
              key={index}
              label={option.label || option.value}
              {...radioProps}/>
          );
        })}
      </div>
    );
  };

  renderSelectSelector = (elementConfig, selectedValue, radioProps) => {
    const tooltip = elementConfig.getConfig().tooltip;
    const label = elementConfig.getConfig().label;
    const hoverHelpText = (tooltip) ? {hoverHelpText: tooltip} : null;
    const isDisabled = (elementConfig.getConfig().disabled || this.props.disabled) ?
      {disabled: true} : null;

    return <div style={{marginTop: 6}}>
      <FieldWithError errorPlacement='top'
        {...hoverHelpText}
        label={label}
        labelClass={selectFieldWithError} >
        <div className={selectWrapper}>
          <Select
            {...isDisabled}
            items={elementConfig.getConfig().options}
            className={selectBody}
            valueField='value'
            value={selectedValue}
            {...radioProps}
          />
        </div>
      </FieldWithError>
    </div>;
  };

  render() {
    const {fields, elementConfig} = this.props;
    const radioField = FormUtils.getFieldByComplexPropName(fields, elementConfig.getPropName());
    // radioField usually has a value. If not, use 1st option value

    const { value, ...radioProps } = radioField;
    const selectedValue = value || elementConfig.getOptions()[0].value;
    const selectedOptionObj = elementConfig.getOptions().find(option => option.value === selectedValue);

    const container = selectedOptionObj.container;
    const containerHelp = container.getConfig && container.getConfig().help;
    const selectorType = elementConfig.getConfig().selectorType;

    return (
      <div className={componentContainer}>
        {selectorType === 'select' && this.renderSelectSelector(elementConfig, selectedValue, radioProps)}
        {(!selectorType || selectorType === 'radio') && this.renderRadioSelector(elementConfig, selectedValue, radioProps)}
        {container.getPropName &&
        <FormElement fields={fields} elementConfig={container}/>
        }
        {container.getAllElements && (!!container.getAllElements().length || containerHelp) &&
        <FormSection fields={fields} sectionConfig={container}/>
        }
      </div>
    );
  }
}
