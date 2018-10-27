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
import classNames from 'classnames';
import FormUtils from 'utils/FormUtils/FormUtils';
import FormSection from 'components/Forms/FormSection';
import FormElement from 'components/Forms/FormElement';
import Radio from 'components/Fields/Radio';

import { rowOfInputsSpacing, rowOfRadio } from '@app/uiTheme/less/forms.less';
import { componentContainer, topLabel} from './ContainerSelection.less';

export default class ContainerSelection extends Component {
  static propTypes = {
    fields: PropTypes.object,
    elementConfig: PropTypes.object
  };

  render() {
    const {fields, elementConfig} = this.props;
    const radioField = FormUtils.getFieldByComplexPropName(fields, elementConfig.getPropName());
    // radioField usually has a value. If not, use 1st option value

    const { value, ...radioProps } = radioField;
    const selectedValue = value || elementConfig.getOptions()[0].value;
    const selectedOptionObj = elementConfig.getOptions().find(option => option.value === selectedValue);

    const container = selectedOptionObj.container;
    const label = elementConfig.getConfig().label;
    const containerHelp = container.getConfig && container.getConfig().help;

    // implementing default selector (radio) for now. TODO: other selectors
    return (
      <div className={componentContainer}>
        {label && <div className={topLabel}>{label}</div>}
        <div className={classNames(rowOfRadio, rowOfInputsSpacing)}>
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
