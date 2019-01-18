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
import FormElement from 'components/Forms/FormElement';
import FormSection from 'components/Forms/FormSection';
import HoverHelp from 'components/HoverHelp';
import Checkbox from 'components/Fields/Checkbox';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';
import FormElementConfig from 'utils/FormUtils/FormElementConfig';

import { flexContainer, flexElementAuto, flexColumnContainer } from '@app/uiTheme/less/layout.less';
import { checkboxMargin, enabledContainer, tooltipIcon } from './CheckEnabledContainer.less';

/**
 * Displays a checkbox and a container, which can be either shown/hidden or its inputs enabled/disabled based
 *   on checkbox state
 * Props:
 *   elementConfig: CheckEnabledConfig with getConfig() returning:
 *     - propName - for the checkbox
 *     - inverted - hides/disables container according to inverted visible state of checkbox
 *     - container - section type configuration object with elements and optional title, tooltip, etc.
 *     - whenNotChecked - optional "hide" value, otherwise defaults to "disable"
 */
export default class CheckEnabledContainer extends Component {

  static propTypes = {
    fields: PropTypes.object,
    mainCheckboxIsDisabled: PropTypes.bool,
    elementConfig: PropTypes.object
  };

  setCheckboxFieldCheckedProp = (field) => {
    if (field && field.checked === undefined) {
      field.checked = false;
    }
  };

  render() {
    const { fields, elementConfig, mainCheckboxIsDisabled } = this.props;
    const elementConfigJson = elementConfig.getConfig();
    const checkField = FormUtils.getFieldByComplexPropName(fields, elementConfig.getPropName());
    this.setCheckboxFieldCheckedProp(checkField); // to avoid react controlled/uncontrolled field warning
    const tooltipStyle = { width: 180 }; // since this component appears in narrow forms, need to limit tooltip width
    const enableContainer = checkField.checked;
    const isInverted = (elementConfigJson.inverted) ? {inverted: true} : null;
    const container = elementConfig.getContainer();
    const containerIsElement = container instanceof FormElementConfig;
    const containerIsSection = container instanceof FormSectionConfig;

    return (
      <div className={flexColumnContainer}>
        <div className={flexContainer}>
          <Checkbox {...checkField} {...isInverted}
            disabled={mainCheckboxIsDisabled}
            className={checkboxMargin}
            label={elementConfigJson.label}/>
          {elementConfigJson.checkTooltip &&
          <HoverHelp content={elementConfigJson.checkTooltip} className={tooltipIcon} tooltipInnerStyle={tooltipStyle}/>
          }
        </div>
        {(elementConfigJson.whenNotChecked !== 'hide' || enableContainer) &&
        <div className={classNames([flexElementAuto, enabledContainer])}>
          {containerIsElement &&
          <FormElement fields={fields}
                       disabled={!enableContainer || mainCheckboxIsDisabled}
                       elementConfig={container}/>
          }
          {containerIsSection &&
          <FormSection fields={fields}
                       disabled={!enableContainer || mainCheckboxIsDisabled}
                       sectionConfig={container}/>
          }
        </div>
        }
      </div>
    );
  }

}
