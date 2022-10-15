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
import { Component } from "react";

import PropTypes from "prop-types";
import FormSection from "components/Forms/FormSection";
import { HoverHelp } from "dremio-ui-lib";
import {
  tabTitle,
  tabSections,
  tabTallSections,
} from "uiTheme/less/forms.less";

export default class FormTab extends Component {
  static propTypes = {
    tabConfig: PropTypes.object,
    fields: PropTypes.object,
    formConfig: PropTypes.object,
    showTabTitle: PropTypes.bool,
    disabled: PropTypes.bool,
  };

  render() {
    const {
      fields,
      formConfig,
      tabConfig,
      showTabTitle = false,
      disabled,
    } = this.props;
    const tabConfigJson = tabConfig.getConfig();
    const tabTitleText = tabConfig.getTitle(formConfig);
    const tabSectionsClass =
      tabConfigJson.layout === "tall" ? tabTallSections : tabSections;

    const isSectionDisabled = (section, propsFields, propDisabled) => {
      const controller = section.getConfig().checkboxController;
      return (
        propDisabled ||
        (controller &&
          propsFields.config &&
          propsFields.config[controller] &&
          !propsFields.config[controller].value)
      );
    };
    return (
      <div>
        {showTabTitle && !!tabTitleText && (
          <div className={tabTitle}>
            {tabTitleText}
            {tabConfigJson.tooltip && (
              <HoverHelp content={tabConfigJson.tooltip} />
            )}
          </div>
        )}
        <div className={tabSectionsClass}>
          {tabConfig.getSections().map((section, index) => (
            <FormSection
              fields={fields}
              key={index}
              sectionConfig={section}
              tabTitleText={showTabTitle ? null : tabTitleText}
              disabled={isSectionDisabled(section, fields, disabled)}
              isFirstSection={index === 0}
            />
          ))}
        </div>
      </div>
    );
  }
}
