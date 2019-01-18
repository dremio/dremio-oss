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

import { SHARING_TAB_JSON_TEMPLATE } from 'constants/sourceTypes';
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import FormConfig from 'utils/FormUtils/FormConfig';

import TextWrapper from 'components/Forms/Wrappers/TextWrapper';
import TextareaWrapper from 'components/Forms/Wrappers/TextareaWrapper';
import CheckboxWrapper from 'components/Forms/Wrappers/CheckboxWrapper';
import SelectWrapper from 'components/Forms/Wrappers/SelectWrapper';
import RadioWrapper from 'components/Forms/Wrappers/RadioWrapper';
import DurationWrapper from 'components/Forms/Wrappers/DurationWrapper';
import CheckEnabledContainerWrapper from 'components/Forms/Wrappers/CheckEnabledContainerWrapper';
import ContainerSelectionWrapper from 'components/Forms/Wrappers/ContainerSelectionWrapper';
import CredentialsWrapper from 'components/Forms/Wrappers/CredentialsWrapper';
import DataFreshnessWrapper from 'components/Forms/Wrappers/DataFreshnessWrapper';
import HostListWrapper from 'components/Forms/Wrappers/HostListWrapper';
import ValueListWrapper from 'components/Forms/Wrappers/ValueListWrapper';
import PropertyListWrapper from 'components/Forms/Wrappers/PropertyListWrapper';
import MetadataRefreshWrapper from 'components/Forms/Wrappers/MetadataRefreshWrapper';
import SharingWrapper from 'components/Forms/Wrappers/SharingWrapper';

describe('SourceFormJsonPolicy', () => {

  describe('makeFullPropName', () => {
    it('should add config prefix', () => {
      expect(SourceFormJsonPolicy.makeFullPropName('a')).to.equal('config.a');
      expect(SourceFormJsonPolicy.makeFullPropName('name')).to.equal('config.name');
    });
  });

  describe('combineDefaultAndLoadedList', () => {
    let defaultList, loadedList;

    beforeEach(() => {
      defaultList = [{ sourceType: 'ONE', prop: 'p' }];
      loadedList = [{ sourceType: 'ONE', label: 'One' }];
    });

    it('should return loaded if no default list', () => {
      expect(SourceFormJsonPolicy.combineDefaultAndLoadedList(loadedList)).to.equal(loadedList);
      expect(SourceFormJsonPolicy.combineDefaultAndLoadedList(loadedList, [])).to.equal(loadedList);
    });
    it('should filter out default entries not present in loaded', () => {
      defaultList.push({ sourceType: 'TWO', prop: 'P', p2: 'P2' });
      const list = SourceFormJsonPolicy.combineDefaultAndLoadedList(loadedList, defaultList);
      expect(list.length).to.equal(1);
      expect(list[0].sourceType).to.equal('ONE');
    });
    it('should decorate entry from ui list', () => {
      const list = SourceFormJsonPolicy.combineDefaultAndLoadedList(loadedList, defaultList);
      expect(list.length).to.equal(1);
      expect(list[0].prop).to.equal('p');
      expect(list[0].label).to.equal('One');
      expect(list[0].sourceType).to.equal('ONE');
    });
  });

  describe('combineFunctionalAndPresentationalSourceTypeConfig', () => {
    let uiConfig, functionalConfig;
    beforeEach(() => {
      uiConfig = {sourceType: 'S3', metadataRefresh:{a: 'a'}};
      functionalConfig = {sourceType: 'S3', label: 's3', elements:[{propertyName: 'a', type: 'text', defaultValue: '9'}]};
    });

    describe('makeEmptyUiConfig', () => {
      it('should return default', () => {
        const config = SourceFormJsonPolicy.makeEmptyUiConfig(uiConfig);
        expect(config.sourceType).to.equal('S3');
        expect(config.metadataRefresh.a).to.equal('a');
        expect(config.form.getConfig()).to.eql({});
      });
    });

    describe('makeCombinedFromFunctionalConfig', () => {
      it('should return null if no functional', () => {
        const config = SourceFormJsonPolicy.makeCombinedFromFunctionalConfig();
        expect(config).to.be.null;
      });
      it('should return default', () => {
        const config = SourceFormJsonPolicy.makeCombinedFromFunctionalConfig(functionalConfig);
        expect(config.sourceType).to.equal('S3');
        expect(config.label).to.equal('s3');
        expect(config.form.getDirectElements().length).to.equal(1);
      });
      it('should prepare functional elements in config', () => {
        const config = SourceFormJsonPolicy.makeCombinedFromFunctionalConfig(functionalConfig);
        const elementConfig = config.form.getDirectElements()[0].getConfig();
        expect(elementConfig.propertyName).to.equal('a');
        expect(elementConfig.propName).to.equal('config.a');
        expect(elementConfig.value).to.equal('9');
      });
    });

    describe('mergeFormMetadataConfig', () => {
      it('should clone uiConfig', () => {
        const config = SourceFormJsonPolicy.mergeFormMetadataConfig(uiConfig);
        expect(config.sourceType).to.equal(uiConfig.sourceType);
        expect(config.metadataRefresh.a).to.equal(uiConfig.metadataRefresh.a);
        expect(config).to.not.equal(uiConfig);
      });
      it('should use sourceType for label when label is not in loaded config', () => {
        const config = SourceFormJsonPolicy.mergeFormMetadataConfig(uiConfig);
        expect(config.label).to.equal('S3');
      });
      it('should add label to uiConfig when available', () => {
        const config = SourceFormJsonPolicy.mergeFormMetadataConfig(uiConfig, functionalConfig);
        expect(config.label).to.equal('s3');
      });
    });

    describe('convertJsonConfigToObjects', () => {
      it('should return falsy argument', () => {
        expect(SourceFormJsonPolicy.convertJsonConfigToObjects(null, [])).to.be.null;
        expect(SourceFormJsonPolicy.convertJsonConfigToObjects()).to.be.undefined;
      });
      it('should call FormConfig constructor', () => {
        //TODO how to test that FormConfig was called
        // const spy = sinon.spy(global, 'FormConfig');
        // const config = SourceFormJsonPolicy.convertJsonConfigToObjects(uiConfig, functionalConfig);
        // expect(spy).to.have.been.called;
      });
    });

    describe('convertElementConfigJsonToObject', () => {
      it('should create FormElementConfig for unknown type', () => {
        const config = {type: 'a'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getConfig().type).to.equal('a');
        expect(elementConfig.foundInFunctionalConfig()).to.be.undefined;
        expect(elementConfig.getRenderer()).to.equal(TextWrapper);
      });
      it('should create config for text', () => {
        const config = {type: 'text'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(TextWrapper);
      });
      it('should create config for textarea', () => {
        const config = {type: 'textarea'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(TextareaWrapper);
      });
      it('should create config for number', () => {
        const config = {type: 'number'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(TextWrapper);
      });
      it('should create config for checkbox', () => {
        const config = {type: 'checkbox'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(CheckboxWrapper);
      });
      it('should create config for select', () => {
        const config = {type: 'select'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(SelectWrapper);
      });
      it('should create config for radio', () => {
        const config = {type: 'radio'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(RadioWrapper);
      });
      it('should create config for duration', () => {
        const config = {type: 'duration'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(DurationWrapper);
      });
      it('should create config for credentials', () => {
        const config = {type: 'credentials'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(CredentialsWrapper);
      });
      it('should create config for data freshness', () => {
        const config = {type: 'data_freshness'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(DataFreshnessWrapper);
      });
      it('should create config for host list', () => {
        const config = {type: 'host_list'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(HostListWrapper);
      });
      it('should create config for metadata refresh', () => {
        const config = {type: 'metadata_refresh'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(MetadataRefreshWrapper);
      });
      it('should create config for property list', () => {
        const config = {type: 'property_list'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(PropertyListWrapper);
      });
      it('should create config for sharing', () => {
        const config = {type: 'sharing_widget'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(SharingWrapper);
      });
      it('should create config for value list', () => {
        const config = {type: 'value_list'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(ValueListWrapper);
      });
      it('should create config for check enabled container', () => {
        const config = {type: 'check_enabled_container'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(CheckEnabledContainerWrapper);
      });
      it('should create config for container selection', () => {
        const config = {type: 'container_selection'};
        const elementConfig = SourceFormJsonPolicy.convertElementConfigJsonToObject(config);
        expect(elementConfig.getRenderer()).to.equal(ContainerSelectionWrapper);
      });
    });

    describe('joinElementConfigs', () => {
      let uiElem, functionalElem;
      beforeEach(() => {
        uiElem = {uiType: 'credentials', propName: 'config.a'};
        functionalElem = {type: 'credentials', propertyName: 'a', value: 'MASTER'};
      });
      it('should set ui type if no functional element given', () => {
        expect(uiElem.type).to.be.undefined;
        SourceFormJsonPolicy.joinElementConfigs(uiElem);
        expect(uiElem.type).to.equal('credentials');
      });
      it('should mark functional element as found', () => {
        expect(functionalElem.foundInUiConfig).to.be.undefined;
        SourceFormJsonPolicy.joinElementConfigs(uiElem, functionalElem);
        expect(functionalElem.foundInUiConfig).to.equal(true);
      });
      it('should set functional props in ui element', () => {
        functionalElem.label = 'label';
        functionalElem.secret = true;
        SourceFormJsonPolicy.joinElementConfigs(uiElem, functionalElem);
        expect(uiElem.type).to.equal('credentials');
        expect(uiElem.propertyName).to.equal('a');
        expect(uiElem.foundInFunctionalConfig).to.equal(true);
        expect(uiElem.label).to.equal('label');
        expect(uiElem.secure).to.equal(true);
      });
      it('should allow vlh label to overwrite functional', () => {
        uiElem.label = 'uilabel';
        functionalElem.label = 'label';
        SourceFormJsonPolicy.joinElementConfigs(uiElem, functionalElem);
        expect(uiElem.type).to.equal('credentials');
        expect(uiElem.label).to.equal('uilabel');
      });
      it('should call merge options', () => {
        uiElem.options = [{}, {}];
        functionalElem.options = [{}, {}];
        const spy = sinon.spy(SourceFormJsonPolicy, 'joinOptions');
        SourceFormJsonPolicy.joinElementConfigs(uiElem, functionalElem);
        expect(spy).to.have.been.called;
      });
    });

    describe('joinOptions', () => {
      let uiOptions, functionalOptions;
      beforeEach(() => {
        uiOptions = [
          {value: 'ANONYMOUS', container: {'elements': []}},
          {value: 'MASTER', container: {
            layout: 'row',
            elements: [
              {propName: 'config.accessKey', size: 'half',
                errMsg: 'Both access secret and key are required for private S3 buckets.'},
              {propName: 'config.accessSecret', size: 'half',
                errMsg: 'Both access secret and key are required for private S3 buckets.', secure: true}
            ],
            help: {position: 'bottom', text: 'All buckets associated with this access key will be available.'}
          }
          }
        ];
        functionalOptions = [
          { value: 'ANONYMOUS', label: 'Public buckets only' },
          { value: 'MASTER', label: 'AWS access key & secret' }
        ];
      });
      it('should return functional if no ui options', () => {
        const options = SourceFormJsonPolicy.joinOptions(null, functionalOptions);
        expect(options).to.equal(functionalOptions);
      });
      it('should return ui options if no functional', () => {
        const options = SourceFormJsonPolicy.joinOptions(uiOptions);
        expect(options).to.equal(uiOptions);
      });
      it('should merge based on option value', () => {
        const options = SourceFormJsonPolicy.joinOptions(uiOptions, functionalOptions);
        expect(options[0].value).to.equal('ANONYMOUS');
        expect(options[0].label).to.equal('Public buckets only');
        expect(options[0].container.elements.length).to.equal(0);
        expect(options[1].value).to.equal('MASTER');
        expect(options[1].label).to.equal('AWS access key & secret');
        expect(options[1].container.elements.length).to.equal(2);
        expect(options[1].container.layout).to.equal('row');
        expect(options[1].container.help.position).to.equal('bottom');
      });
    });

  });


  describe('applyJsonPolicyToFormConfig', () => {
    let uiConfig, functionalConfig;
    beforeEach(() => {
      uiConfig = SourceFormJsonPolicy.makeEmptyUiConfig({sourceType: 'S3'});
      functionalConfig = {};
    });

    it('should return invalid form config as is', () => {
      expect(SourceFormJsonPolicy.applyJsonPolicyToFormConfig()).to.be.undefined;
      expect(SourceFormJsonPolicy.applyJsonPolicyToFormConfig(false)).to.equal(false);
      expect(SourceFormJsonPolicy.applyJsonPolicyToFormConfig('')).to.equal('');
    });

    it('should add default general, metadata, and acceleration tabs', () => {
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, {});
      expect(config.form).to.not.be.undefined;
      const numberOfTabs = (SHARING_TAB_JSON_TEMPLATE.name) ? 4 : 3;
      expect(config.form.getTabs().length).to.equal(numberOfTabs);
      expect(config.form.getTabs()[0].getName()).to.equal('General');
      expect(config.form.getTabs()[1].getName()).to.equal('Reflection Refresh');
      expect(config.form.getTabs()[2].getName()).to.equal('Metadata');
    });

    it('should move loose sections to general tab',  () => {
      const looseSections = [
        {
          name: 'Test section',
          layout: 'row',
          elements: [
            { type: 'text', propName: 'test', label: 'TEST loose elements' }
          ]
        }];
      uiConfig = {sourceType: 'S3', form: new FormConfig({sections: looseSections})};
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, functionalConfig);
      expect(config.form.getTabs()[0].getSections().length).to.equal(2);
      expect(config.form.getTabs()[0].getSections()[1].getConfig().name).to.equal('Test section');
      expect(config.form.getConfig().sections).to.be.undefined;
    });

    it('should have icon, name, and descriptionin general tab', () => {
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, {});
      const section = config.form.getTabs()[0].getSections()[0];
      expect(section.getConfig().icon).to.equal('S3.svg');
      expect(section.getDirectElements()).to.have.length(1); // only name should be presented
      expect(section.getDirectElements()[0].getPropName()).to.equal('name');
    });

    it('should move loose elements to general tab',  () => {
      const looseElements = [
        { type: 'text', propName: 'test', label: 'TEST loose elements' }
      ];
      uiConfig = {sourceType: 'S3', form: new FormConfig({elements: looseElements})};
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, functionalConfig);
      expect(config.form.getTabs()[0].getSections().length).to.equal(2);
      expect(config.form.getTabs()[0].getSections()[1].getDirectElements()[0].getConfig().label).to.equal('TEST loose elements');
      expect(config.form.getConfig().elements).to.be.undefined;
    });

    it('should add refresh policy and sharing tabs if configured', () => {
      uiConfig = {sourceType: 'S3', metadataRefresh: true, form: new FormConfig({})};
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, functionalConfig);
      const numberOfTabs = (SHARING_TAB_JSON_TEMPLATE.name) ? 4 : 3;
      expect(config.form.getTabs().length).to.equal(numberOfTabs);
      expect(config.form.getTabs()[1].getName()).to.equal('Reflection Refresh');
      expect(config.form.getTabs()[2].getName()).to.equal('Metadata');
      if (numberOfTabs === 4) {
        expect(config.form.getTabs()[3].getName()).to.equal('Sharing');
      }
    });

    const findConfigContainer = (configContainers, field, value) => configContainers.find((container) => {
      return container.getConfig()[field] === value;
    });

    const getDataRefreshElement = form => {
      const metadataTab = findConfigContainer(form.getTabs(), 'name', 'Metadata');
      const refreshSection = findConfigContainer(metadataTab.getSections(), 'name', 'Metadata Refresh');
      return findConfigContainer(refreshSection.getDirectElements(), 'type', 'metadata_refresh');
    };

    it('should add refresh policy without datasetDiscovery and authorization', () => {
      uiConfig = {sourceType: 'S3', metadataRefresh: {}, form: new FormConfig({})};
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, functionalConfig);
      const element = getDataRefreshElement(config.form);
      expect(element.getConfig().datasetDiscovery).to.be.undefined;
      expect(element.getConfig().authorization).to.be.undefined;
    });

    it('should add refresh policy with datasetDiscovery and authorization', () => {
      uiConfig = {sourceType: 'S3', metadataRefresh: {datasetDiscovery: true, authorization: true}, form: new FormConfig({})};
      const config = SourceFormJsonPolicy.applyJsonPolicyToFormConfig(uiConfig, functionalConfig);
      const element = getDataRefreshElement(config.form);
      expect(element.getConfig().datasetDiscovery).to.equal(true);
      expect(element.getConfig().authorization).to.equal(true);
    });
  });
});
