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
import HoverHelp from 'components/HoverHelp';
import FormSection from 'components/Forms/FormSection';
import { tabTitle, tabSections, tabTallSections } from 'uiTheme/less/forms.less';

export default class FormTab extends Component {

  static propTypes = {
    tabConfig: PropTypes.object,
    fields: PropTypes.object,
    formConfig: PropTypes.object,
    disabled: PropTypes.bool
  };

  render() {
    const { fields, formConfig, tabConfig, disabled } = this.props;
    const tabConfigJson = tabConfig.getConfig();
    const tabTitleText = tabConfig.getTitle(formConfig);
    const tabSectionsClass = (tabConfigJson.layout === 'tall') ? tabTallSections : tabSections;

    return (
      <div>
        {!!tabTitleText &&
        <div className={tabTitle}>
          {tabTitleText}
          {tabConfigJson.tooltip &&
          <HoverHelp content={tabConfigJson.tooltip} />
          }
        </div>
        }
        <div className={tabSectionsClass}>
          {tabConfig.getSections().map((section, index) => (
            <FormSection fields={fields} key={index} sectionConfig={section} disabled={disabled}/>
          ))}
        </div>
      </div>
    );
  }

}
