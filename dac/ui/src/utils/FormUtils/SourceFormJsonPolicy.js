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

import { pick } from 'lodash/object';
import { SHARING_TAB_JSON_TEMPLATE } from 'dyn-load/constants/sourceTypes';
import { DEFAULT_VLHF_DETAIL } from 'dyn-load/constants/vlh';
import FormUtils from 'utils/FormUtils/FormUtils';
import FormConfig from 'utils/FormUtils/FormConfig';
import FormTabConfig from 'utils/FormUtils/FormTabConfig';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';
import FormElementConfig from 'utils/FormUtils/FormElementConfig';
import CheckEnabledContainerConfig from 'utils/FormUtils/CheckEnabledContainerConfig';
import ContainerSelectionConfig from 'utils/FormUtils/ContainerSelectionConfig';
import CredentialsConfig from 'utils/FormUtils/CredentialsConfig';
import DataFreshnessConfig from 'utils/FormUtils/DataFreshnessConfig';
import HostListConfig from 'utils/FormUtils/HostListConfig';
import MetadataRefreshConfig from 'utils/FormUtils/MetadataRefreshConfig';
import PropertListConfig from 'utils/FormUtils/PropertyListConfig';
import SharingWidgetConfig from 'utils/FormUtils/SharingWidgetConfig';
import ValueListConfig from 'utils/FormUtils/ValueListConfig';


export default class SourceFormJsonPolicy {

  static deepCopyConfig(config) {
    return FormUtils.deepCopyConfig(config);
  }

  static makeFullPropName(propertyName) {
    return `config.${propertyName}`;
  }


  //== Source List support
  /**
   *
   * merge data from two config objects to make conbimed list of source types
   * @param defaultList - ui decorators for source types
   * @param loadedList - list of source types coming from API response
   * @returns {*} - source types from API decorated with features from default list
   */
  static combineDefaultAndLoadedList(loadedList, defaultList) {
    if (!defaultList || !defaultList.length) return loadedList;

    // for each entry in loadedList
    //   if found in default list, update entry properties with decorators
    return loadedList.map(loadedEntry => {
      const defaultEntry = defaultList.find(entry => entry.sourceType === loadedEntry.sourceType);

      // label is optional, use sourceType if not present
      if (!loadedEntry.label) {
        loadedEntry.label = loadedEntry.sourceType;
      }

      if (defaultEntry) {
        // mutates defaultEntry
        return {...loadedEntry, ...defaultEntry};
      }
      return loadedEntry;
    });
  }


  //== Source Detail support

  /**
   * @param typeCode
   * @param typeConfig
   */
  static getCombinedConfig(typeCode, typeConfig) {
    const uiConfig = DEFAULT_VLHF_DETAIL[typeCode];
    const conbinedConfig = SourceFormJsonPolicy.combineFunctionalAndPresentationalSourceTypeConfig(typeConfig, uiConfig);
    return SourceFormJsonPolicy.applyJsonPolicyToFormConfig(conbinedConfig, typeConfig);
  }

  /**
   * Take two json objects: 1) from API with functional data and 2) VLF with ui decorations
   * and return combined configuration object with properties converted to proper config classes
   * @param functionalConfig
   * @param uiConfig
   * @return {*}
   */
  static combineFunctionalAndPresentationalSourceTypeConfig(functionalConfig, uiConfig) {
    // if no functional, return empty uiConfig shell w/o elements
    if (!functionalConfig) return this.makeEmptyUiConfig(uiConfig);

    // if no uiConfig, return functional
    if (!uiConfig) return this.makeCombinedFromFunctionalConfig(functionalConfig);

    // combine high level form properties
    const mergedMetaConfig = this.mergeFormMetadataConfig(uiConfig, functionalConfig);

    // make all properties of joined config FormXxxConfig objects
    const joinedConfig = this.convertJsonConfigToObjects(mergedMetaConfig, functionalConfig);

    // add functional elements, not found in ui config, to un-decorated list of direct form member elements
    for (const element of functionalConfig.elements) {
      if (!element.foundInUiConfig) {
        const elementJson = this.makeConfigFromFunctional(element);
        const directElementConfig = this.convertElementConfigJsonToObject(elementJson);
        directElementConfig.setFoundInFunctionalConfig(true);
        joinedConfig.form.addDirectElement(directElementConfig);
      }
    }

    // filter out uiElements not found in functional
    joinedConfig.form.removeNotFoundElements();

    return joinedConfig;
  }

  static makeEmptyUiConfig(uiConfig) {
    return (uiConfig) ? {
      sourceType: uiConfig.sourceType,
      metadataRefresh: uiConfig.metadataRefresh,
      form: new FormConfig()
    } : null;
  }

  static makeCombinedFromFunctionalConfig(loadedFunctionalConfig) {
    return (loadedFunctionalConfig) ? {
      label: loadedFunctionalConfig.label,
      sourceType: loadedFunctionalConfig.sourceType,
      form: new FormConfig({
        elements: loadedFunctionalConfig.elements.map(element => this.makeConfigFromFunctional(element))
      })
    } : null;
  }

  static joinConfigsAndConvertElementToObj(elementJson, functionalElements) {
    if (functionalElements) {
      // find functional element for elementJson
      const functionalElement = functionalElements.find(
        element => this.makeFullPropName(element.propertyName) === FormUtils.dropTrailingBrackets(elementJson.propName));

      // join configs potentialy mutates both
      this.joinElementConfigs(elementJson, functionalElement);
    }
    // convert to objects
    return this.convertElementConfigJsonToObject(elementJson, functionalElements);
  }

  static joinElementConfigs(uiElementConfig, functionalElementConfig) {
    if (functionalElementConfig) {
      // mark functional as found, since not-found will be later added to a default section
      functionalElementConfig.foundInUiConfig = true;
      this.setFunctionalPropsInUiElement(uiElementConfig, functionalElementConfig);
    } else {
      uiElementConfig.type = uiElementConfig.type || uiElementConfig.uiType;
    }
  }

  static makeConfigFromFunctional(functionalElement) {
    const elementJson = {
      ...functionalElement,
      type: this.convertElementTypeForUi(functionalElement.type),
      propName: `config.${functionalElement.propertyName}`
    };
    if (functionalElement.defaultValue) {
      elementJson.value = functionalElement.defaultValue;
    }
    if (functionalElement.secret) {
      elementJson.secure = true;
    }
    if (elementJson.type === 'value_list') {
      if (!elementJson.propName.endsWith('[]')) {
        elementJson.propName = elementJson.propName + '[]';
      }
    }
    return elementJson;
  }

  static setFunctionalPropsInUiElement(elementJson, functionalElement) {
    elementJson.type = elementJson.uiType || this.convertElementTypeForUi(functionalElement.type);
    elementJson.propertyName = functionalElement.propertyName;
    elementJson.foundInFunctionalConfig = true;

    if (functionalElement.label && !elementJson.label) {
      elementJson.label = functionalElement.label;
    }
    if (functionalElement.defaultValue) {
      elementJson.value = functionalElement.defaultValue;
    }
    if (functionalElement.secret) {
      elementJson.secure = true;
    }
    if (functionalElement.options) {
      elementJson.options = this.joinOptions(elementJson.options, functionalElement.options);
    }
  }

  static joinOptions(uiOptions, functionalOptions) {
    if (!uiOptions || !uiOptions.length) return functionalOptions;
    if (!functionalOptions || !functionalOptions.length) return uiOptions;
    //merge based on option value;
    let map = uiOptions.reduce((accum, option) => ({...accum, ...{[option.value]: option}}), {});
    map = functionalOptions.reduce((accum, option) => {
      accum[option.value] = {...accum[option.value], ...option};
      return accum;
    }, map);
    return Object.values(map);
  }

  static convertElementConfigJsonToObject(elementJson, functionalElements) {
    switch (elementJson.type) {
      /* eslint-disable indent */
      case 'credentials':
        return new CredentialsConfig(elementJson);
      case 'data_freshness':
        return new DataFreshnessConfig(elementJson);
      case 'value_list':
        return new ValueListConfig(elementJson);
      case 'property_list':
        return new PropertListConfig(elementJson);
      case 'host_list':
        return new HostListConfig(elementJson);
      case 'metadata_refresh':
        return new MetadataRefreshConfig(elementJson);
      case 'check_enabled_container':
        return new CheckEnabledContainerConfig(elementJson, functionalElements);
      case 'container_selection':
        return new ContainerSelectionConfig(elementJson, functionalElements);
      case 'sharing_widget':
        return new SharingWidgetConfig(elementJson);
      case 'text':
      case 'number':
      case 'textarea':
      case 'checkbox':
      case 'duration':
      case 'select':
      case 'radio':
      default:
        return new FormElementConfig(elementJson);
      /* eslint-enable indent */
    }
  }

  static mergeFormMetadataConfig(uiConfig, functionalConfig) {
    // start with a copy of uiConfig, which parts will be used in final config
    const joinedConfig = this.deepCopyConfig(uiConfig);

    joinedConfig.label = (functionalConfig && functionalConfig.label) ? functionalConfig.label : joinedConfig.sourceType;
    // currently tags property is defined in uiConfig, which is already in joined
    return joinedConfig;
  }

  /**
   * json config text has been already parsed. It may have form with tabs, sections, elements
   * Here we convert its members into form member objects,
   * using element types and additional attributes from functional config
   * @param jsonConfig
   * @param functionalConfig
   */
  static convertJsonConfigToObjects(jsonConfig, functionalConfig) {
    if (!jsonConfig) return jsonConfig;

    jsonConfig.form = new FormConfig(jsonConfig.form, functionalConfig.elements);

    return jsonConfig;
  }


  /**
   * apply default convention to form configuration
   *   In this implementation this method should be called after config is
   *   created with combineFunctionalAndPresentationalSourceTypeConfig
   *   because it assumes proper config class objects to be in config
   * @param config - form configuration prepared with proper config classes
   * @param functionalConfig
   * @returns {*} - complete config used for form rendering
   */
  static applyJsonPolicyToFormConfig(config, functionalConfig) {
    // config is required to apply the policy, as we need the sourceType
    if (!config && !functionalConfig) return config;

    const COMMON_SECTION_JSON_TEMPLATE = {
      icon: '',
      elements: [
        {
          type: 'text',
          propName: 'name',
          label: 'Name',
          focus: true,
          validate: {isRequired: true}
        }
      ]
    };
    const ACCELERATION_TAB_JSON_TEMPLATE = {
      name: 'Reflection Refresh',
      title: 'Refresh Policy',
      tooltip: 'How often reflections are refreshed and how long data can be served before expiration.',
      sections: [
        {
          elements: [
            {
              type: 'data_freshness'
            }
          ]
        }
      ]
    };

    const functionalElements = functionalConfig.elements || [];

    // add Global tab section with name and description
    this.addGeneralTab(config, COMMON_SECTION_JSON_TEMPLATE, functionalElements);

    // add Acceleration tab
    config.form.addTab(new FormTabConfig(ACCELERATION_TAB_JSON_TEMPLATE, functionalElements));

    // add Reflection Refresh tab based on config.metadataRefresh
    this.addReflectionRefreshTab(config, functionalElements);

    // add Sharing tab
    if (SHARING_TAB_JSON_TEMPLATE.name) {
      const sharingTabJson = this.deepCopyConfig(SHARING_TAB_JSON_TEMPLATE);
      config.form.addTab(new FormTabConfig(sharingTabJson, functionalElements));
    }
    return config;
  }


  static addGeneralTab(config, commonSectionTemplate, functionalElements) {
    // mutates config adding a tab to it

    const form = config.form;
    // find tab in config, which is marked isGeneral
    let generalTab = form.getTabs().find(tab => tab.isGeneral());
    // if not found, add an empty general tab to config
    if (!generalTab) {
      generalTab = new FormTabConfig({name: 'General', isGeneral: true, sections: []}, functionalElements);
      config.form.addTab(generalTab, 'head');
    }
    // add section with icon, name, and description
    const commonSection = commonSectionTemplate;
    commonSection.icon = (config.icon) ? config.icon : `${config.sourceType}.svg`;
    generalTab.addSection(new FormSectionConfig(commonSection, functionalElements), 'head');

    // move loose sections to general tab
    const looseSections = form.getDirectSections();
    looseSections.forEach(section => {
      generalTab.addSection(section);
      form.removeDirectSections();
    });
    // move loose elements to general tab no-name section
    let looseElements = form.getDirectElements();

    // if we have credentials, remove username/password for loose elements
    const hasCredentials = functionalElements.find((elem) => {
      return elem.type === 'credentials';
    });

    if (hasCredentials) {
      looseElements = looseElements.filter((elem) => {
        const propName = elem.getConfig().propertyName;

        return ['username', 'password'].indexOf(propName) === -1;
      });
    }

    if (looseElements.length) {
      const addedSection = new FormSectionConfig({name: ''}, functionalElements);
      addedSection.addElements(looseElements);
      generalTab.addSection(addedSection);
      form.removeDirectElements();
    }
  }


  static addReflectionRefreshTab(config, functionalElements) {
    const isFileSystemSource = config.metadataRefresh && config.metadataRefresh.isFileSystemSource;

    const metadataRefreshEl = {
      type: 'metadata_refresh'
    };
    const tabTemplate = {
      name: 'Metadata',
      sections: [
        {
          name: 'Dataset Handling',
          elements: [{
            type: 'checkbox',
            label: la('Remove dataset definitions if underlying data is unavailable.'),
            propName: 'metadataPolicy.deleteUnavailableDatasets',
            value: true
          }, isFileSystemSource && {
            type: 'checkbox',
            label: la('Automatically format files into physical datasets when users issue queries.'),
            propName: 'metadataPolicy.autoPromoteDatasets',
            value: false
          }].filter(Boolean)
        },
        {
          name: 'Metadata Refresh',
          elements: [metadataRefreshEl]
        }
      ]
    };

    // Take only supported options
    const policyInfo = pick(config.metadataRefresh, ['datasetDiscovery', 'authorization']);

    const tab = tabTemplate;
    // we should alter object by reference, that is why Object.assign is used
    Object.assign(metadataRefreshEl, policyInfo); // eslint-disable-line no-restricted-properties

    const tabConfig = new FormTabConfig(tab, functionalElements);
    config.form.addTab(tabConfig);
  }


  static convertElementTypeForUi(type) {
    switch (type) {
      /* eslint-disable indent */
      case 'boolean':
        return 'checkbox';
      case 'selection':
      case 'enum':
        return 'radio';
      default:
        return type;
      /* eslint-enable indent */
    }
  }


}
