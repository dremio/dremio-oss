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
import FormUtils from 'utils/FormUtils/FormUtils';
import FormElement from 'components/Forms/FormElement';
import FormSection from 'components/Forms/FormSection';
import Checkbox from 'components/Fields/Checkbox';

import { flexContainer, flexElementAuto, flexColumnContainer } from '@app/uiTheme/less/layout.less';
import { checkboxMargin, tooltipIcon } from './CheckEnabledContainer.less';

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
    elementConfig: PropTypes.object
  };

  state = {
    disableInputs: false
  };

  componentWillMount() {
    if (!this.props.elementConfig.getConfig().value) {
      this.setState({disableInputs: true});
    }
  }

  onCheckboxChange = () => {
    this.setState((prevState) => {
      return {disableInputs: !prevState.disableInputs};
    });
  };

  render() {
    const { fields, elementConfig } = this.props;
    const elementConfigJson = elementConfig.getConfig();
    const checkField = FormUtils.getFieldByComplexPropName(fields, elementConfig.getPropName());
    const isInverted = (elementConfigJson.inverted) ? {inverted: true} : null;
    const container = elementConfig.getContainer();

    return (
      <div className={flexColumnContainer}>
        <div className={flexContainer}>
          <Checkbox {...checkField} {...isInverted}
            className={checkboxMargin}
            label={elementConfigJson.label}
            checked={!this.state.disableInputs}
            onChange={this.onCheckboxChange}/>
          {elementConfigJson.checkTooltip &&
          <HoverHelp content={elementConfigJson.checkTooltip} className={tooltipIcon}/>
          }
        </div>
        {(elementConfigJson.whenNotChecked !== 'hide' || !this.state.disableInputs) &&
        <div className={flexElementAuto}>
          {container.getPropName &&
          <FormElement fields={fields}
                       disabled={this.state.disableInputs}
                       elementConfig={container}/>
          }
          {container.getAllElelements && container.getAllElelements().length &&
          <FormSection fields={fields}
                       disabled={this.state.disableInputs}
                       sectionConfig={container}/>
          }
        </div>
        }
      </div>
    );
  }

}
