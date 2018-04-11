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
import AccelerationController from 'components/Acceleration/AccelerationController';

import FileFormatController from './FileFormatController';
import AccelerationUpdatesController from './AccelerationUpdates/AccelerationUpdatesController';
import DatasetOverviewForm from './DatasetOverviewForm';

import { DatasetSettings } from './DatasetSettings';

describe('DatasetSettings', () => {
  let minimalProps;
  let commonProps;
  let context;
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
        entityType: 'physicalDataset'
      }),
      viewState: Immutable.fromJS({
        isInProgress: false
      })
    };
    context = {
      router: {
        push: sinon.spy(),
        replace: sinon.spy()
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetSettings {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render first tab if none specified', () => {
    const props = {
      ...commonProps,
      tab: 'format'
    };
    const wrapper = shallow(<DatasetSettings {...props}/>);
    expect(wrapper.find(FileFormatController)).to.have.length(1);
  });

  it('should render Format tab content when tab is active', () => {
    const props = {
      ...commonProps,
      tab: 'format'
    };
    const wrapper = shallow(<DatasetSettings {...props}/>);
    expect(wrapper.find(FileFormatController)).to.have.length(1);
  });

  it('should render Acceleration tab content when tab is active', () => {
    const props = {
      ...commonProps,
      tab: 'acceleration'
    };
    const wrapper = shallow(<DatasetSettings {...props}/>);
    expect(wrapper.find(AccelerationController)).to.have.length(1);
  });

  it('should render Acceleration Updates tab content when tab is active', () => {
    const props = {
      ...commonProps,
      tab: 'accelerationUpdates'
    };
    const wrapper = shallow(<DatasetSettings {...props}/>);
    expect(wrapper.find(AccelerationUpdatesController)).to.have.length(1);
  });

  it('should render Overview tab content when tab is active', () => {
    const props = {
      ...commonProps,
      tab: 'overview'
    };
    const wrapper = shallow(<DatasetSettings {...props}/>);
    expect(wrapper.find(DatasetOverviewForm)).to.have.length(1);
  });

  describe('#componentWillMount', () => {
    it('should not load dataset when datasetUrl is absent', () => {
      const props = {
        loadDatasetForDatasetType: sinon.stub().returns({ then: f => f()}),
        location: {state: {}}
      };
      shallow(<DatasetSettings {...props}/>, {context});
      expect(props.loadDatasetForDatasetType).to.not.be.called;
      expect(context.router.replace).to.not.be.called;
    });

    it('should load dataset and update location state when datasetUrl present and request succeed', () => {
      const response = {
        payload: Immutable.fromJS({
          entities: {
            dataset: {
              '/dataset_id': {}
            }
          },
          result: '/dataset_id'
        })
      };
      const props = {
        loadDatasetForDatasetType: sinon.stub().returns({ then: f => f(response)}),
        datasetUrl: '/dataset_url',
        datasetType: 'dataset',
        location: {state: {}}
      };
      shallow(<DatasetSettings {...props}/>, {context});
      expect(props.loadDatasetForDatasetType).to.be.calledWith('dataset', '/dataset_url', 'DATASET_SETTINGS_VIEW_ID');
      expect(context.router.replace).to.be.calledWith({state: { entityId: '/dataset_id', entityType: 'dataset'}});
    });

    it('should load dataset when datasetUrl is present and not update location when request failed', () => {
      const response = {
        error: true
      };
      const props = {
        loadDatasetForDatasetType: sinon.stub().returns({ then: f => f(response)}),
        datasetUrl: '/dataset_url',
        datasetType: 'dataset',
        location: {state: {}}
      };
      shallow(<DatasetSettings {...props}/>, {context});
      expect(props.loadDatasetForDatasetType).to.be.calledWith('dataset', '/dataset_url', 'DATASET_SETTINGS_VIEW_ID');
      expect(context.router.replace).to.not.be.called;
    });
  });

  describe('#componentDidMount', () => {
    it('should set first available tab as active if no active tab marked in location state', () => {
      const instance = shallow(<DatasetSettings {...commonProps}/>, {context}).instance();
      instance.componentDidMount();
      expect(context.router.replace).to.be.calledWith({ state: { tab: 'overview'}});
    });

    it('should set active according to props.tab', () => {
      const props = {
        ...commonProps,
        tab: 'overview'
      };
      const wrapper = shallow(<DatasetSettings {...props}/>, {context});
      const instance = wrapper.instance();
      instance.componentDidMount();
      expect(context.router.replace).to.not.be.called;
      expect(wrapper.find('NavPanel').props().activeTab).to.be.eql('overview');
    });
  });

  describe('#updateFormDirtyState', function() {

    it('should update isFormDirty state', function() {
      const instance = shallow(<DatasetSettings {...minimalProps}/>).instance();
      instance.updateFormDirtyState(true);
      expect(instance.state.isFormDirty).to.be.true;
      expect(minimalProps.updateFormDirtyState).to.be.calledWith(true);
    });
  });

  describe('#handleChangeTab', function() {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<DatasetSettings {...minimalProps}/>, {context});
    });

    it('should push next location when form not dirty', function() {
      const instance = wrapper.instance();
      instance.handleChangeTab();
      expect(context.router.push).to.be.calledOnce;
      expect(minimalProps.showUnsavedChangesConfirmDialog).to.not.be.called;
    });
    it('should show confirmation dialog when form dirty', function() {
      wrapper.setProps({
        location: {
          state: {
            tab: 'acceleration'
          }
        }
      });
      const instance = wrapper.instance();
      instance.setState({isFormDirty: true});
      instance.handleChangeTab();
      expect(context.router.push).to.not.be.called;
      expect(minimalProps.showUnsavedChangesConfirmDialog).to.be.calledOnce;
    });
  });
});
