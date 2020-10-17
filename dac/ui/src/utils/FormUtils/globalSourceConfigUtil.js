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

export const getAlwaysPresentFunctionalConfig = () => [{
  label: la('Enable this source to be used with other sources even though Disable Cross Source is configured'),
  propertyName: 'allowCrossSourceSelection',
  type: 'boolean'
}];

export const crossSourceSelectionUiConfig = {
  propName: 'allowCrossSourceSelection',
  visibilityControl: {
    config: 'crossSourceDisabled',
    showCondition: true
  }
};

const addAlwaysPresent = ({ elements }, { form }) => {
  if (elements) {
    elements.push(...getAlwaysPresentFunctionalConfig());
  }

  form.tabs[1].sections = form.tabs[1].sections || [];

  const {
    sections: [firstSection = {}]
  } = form.tabs[1];

  if (form && form.tabs[1]) {
    form.tabs[1].sections[0] = {
      ...(firstSection),
      elements: [
        ...(firstSection.elements || []),
        crossSourceSelectionUiConfig
      ]
    };
  }
};

export default addAlwaysPresent;
