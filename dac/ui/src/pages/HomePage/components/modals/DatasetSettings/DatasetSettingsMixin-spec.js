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
import { shallow } from 'enzyme';
import Immutable from 'immutable';

import {
  DatasetSettings as DatasetSettingsBase
} from './DatasetSettings';
import DatasetSettingsMixin from './DatasetSettingsMixin';

@DatasetSettingsMixin
class DatasetSettings extends DatasetSettingsBase {}


describe('DatasetSettingsMixin', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      viewState: Immutable.Map(),
      location: {state: { tab: '' }},
      loadDatasetForDatasetType: sinon.spy(),
      updateFormDirtyState: sinon.spy(),
      showUnsavedChangesConfirmDialog: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      entity: Immutable.fromJS({
        id: '1',
        entityType: 'file'
      }),
      viewState: Immutable.fromJS({
        isInProgress: false
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetSettings {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#getTabs', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<DatasetSettings {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('should return correct tabs when dataset type is file|folder and not queryable', () => {
      wrapper.setProps({
        entity: commonProps.entity.merge({
          entityType: 'file',
          queryable: false
        })
      });
      const expectedTabs = [
        'format'
      ];
      expect(instance.getTabs().keySeq().toJS()).to.be.eql(expectedTabs);
    });

    it('should return correct tabs when dataset type is file|folder and queryable', () => {
      wrapper.setProps({
        entity: commonProps.entity.merge({
          entityType: 'file',
          queryable: true
        })
      });
      const expectedTabs = [
        'overview',
        'format',
        'acceleration',
        'accelerationUpdates'
      ];
      expect(instance.getTabs().keySeq().toJS()).to.be.eql(expectedTabs);
    });

    it('should return correct tabs when dataset type is file|folder and queryable in home', () => {
      wrapper.setProps({
        entity: commonProps.entity.merge({
          entityType: 'file',
          queryable: true,
          isHomeFile: true
        })
      });
      const expectedTabs = [
        'overview',
        'format',
        'acceleration'
      ];
      expect(instance.getTabs().keySeq().toJS()).to.be.eql(expectedTabs);
    });

    it('should return correct tabs when dataset type is not file|folder', () => {
      wrapper.setProps({
        entity: commonProps.entity.merge({
          entityType: 'physicalDataset'
        })
      });

      const expectedTabs = [
        'overview',
        'acceleration',
        'accelerationUpdates'
      ];
      expect(instance.getTabs().keySeq().toJS()).to.be.eql(expectedTabs);
    });
  });
});
