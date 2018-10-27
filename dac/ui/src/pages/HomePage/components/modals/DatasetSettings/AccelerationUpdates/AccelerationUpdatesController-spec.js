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

import ApiUtils from 'utils/apiUtils/apiUtils';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import { ALL_TYPES, INCREMENTAL_TYPES } from 'constants/columnTypeGroups';
import { AccelerationUpdatesController } from './AccelerationUpdatesController';
import AccelerationUpdatesForm from './AccelerationUpdatesForm';

describe('AccelerationUpdatesController', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      viewState: Immutable.fromJS({
        isInProgress: false
      }),
      updateViewState: () => {}
    };
    commonProps = {
      ...minimalProps,
      accelerationSettings: Immutable.fromJS({
        refreshMethod: 'FULL',
        accelerationRefreshPeriod: DataFreshnessSection.defaultFormValueRefreshInterval(),
        accelerationGracePeriod: DataFreshnessSection.defaultFormValueGracePeriod()
      }),
      entity: Immutable.fromJS({
        fullPathList: ['path']
      }),
      loadDatasetAccelerationSettings: sinon.stub(),
      updateDatasetAccelerationSettings: sinon.stub(),
      onDone: sinon.stub().returns('onDone'),
      state: {
        fields: [
          {name: 'col1', type: {name: 'INTEGER'}},
          {name: 'col2', type: {name: 'TEXT'}}
        ]
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationUpdatesController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should not render acceleration updates form when settings are not available', () => {
    const wrapper = shallow(<AccelerationUpdatesController {...minimalProps}/>);
    expect(wrapper.find(AccelerationUpdatesForm)).to.have.length(0);
  });

  it('should render acceleration updates form when settings are available', () => {
    const wrapper = shallow(<AccelerationUpdatesController {...commonProps}/>);
    expect(wrapper.find(AccelerationUpdatesForm)).to.have.length(1);
  });

  describe('#componentWillMount', () => {
    it('should call recieveProps to perform summaryDataset and settings loading', () => {
      sinon.stub(AccelerationUpdatesController.prototype, 'receiveProps');
      const wrapper = shallow(<AccelerationUpdatesController {...minimalProps}/>);
      const instance = wrapper.instance();
      expect(instance.receiveProps).to.be.calledWith(minimalProps, {});
      AccelerationUpdatesController.prototype.receiveProps.restore();
    });
  });

  describe('#componentWillReceiveProps', () => {
    it('should call recieveProps with nextProps', () => {
      sinon.stub(AccelerationUpdatesController.prototype, 'receiveProps');
      const wrapper = shallow(<AccelerationUpdatesController {...minimalProps}/>);
      const instance = wrapper.instance();
      const nextProps = {entity: {}};
      instance.componentWillReceiveProps(nextProps);
      expect(instance.receiveProps).to.be.calledWith(nextProps, minimalProps);
      AccelerationUpdatesController.prototype.receiveProps.restore();
    });
  });

  describe('#recieveProps', () => {
    let wrapper;
    let instance;
    let props;
    beforeEach(() => {
      sinon.stub(AccelerationUpdatesController.prototype, 'componentWillMount');
      props = {
        ...commonProps,
        loadSummaryDataset: sinon.stub().returns({then: f => f()})
      };
      wrapper = shallow(<AccelerationUpdatesController {...props}/>);
      instance = wrapper.instance();

      sinon.stub(instance, 'loadDataset').returns(Promise.resolve({
        fields: [
          {name: 'col1', type: {name: 'INTEGER'}},
          {name: 'col2', type: {name: 'TEXT'}}
        ]
      }));
    });
    afterEach(() => {
      AccelerationUpdatesController.prototype.componentWillMount.restore();
    });

    it('should load settings when entity changed', (done) => {
      const nextProps = {
        ...props,
        entity: props.entity.set('fullPath', ['changed_path'])
      };

      instance.receiveProps(nextProps, props);
      setTimeout(() => {
        expect(props.loadDatasetAccelerationSettings).to.have.been.called;
        done();
      }, 50);
    });

    it('should not load settings when entity did not change', (done) => {
      instance.receiveProps(props, props);
      setTimeout(() => {
        expect(props.loadDatasetAccelerationSettings).to.have.not.been.called;
        done();
      }, 50);
    });
  });

  describe('#schemaToColumns', () => {
    it('should return only incremental columns', () => {
      const summaryDataset = {
        fields: ALL_TYPES.map((type, i) => ({type: {name: type}, name: `col${i}`}))
      };
      const wrapper = shallow(<AccelerationUpdatesController {...minimalProps}/>);
      const instance = wrapper.instance();
      const columnTypes = instance.schemaToColumns(summaryDataset).toJS().map(field => field.type);
      expect(columnTypes).to.have.members(INCREMENTAL_TYPES);
    });
  });

  describe('#submit', () => {
    it('should perform settings update and call onDone when saved', (done) => {
      sinon.stub(ApiUtils, 'attachFormSubmitHandlers').returns(Promise.resolve());
      const wrapper = shallow(<AccelerationUpdatesController {...commonProps}/>);
      const instance = wrapper.instance();
      const formValue = commonProps.accelerationSettings.toJS();
      expect(instance.submit(formValue)).to.eventually.equal('onDone').notify(done);
      expect(ApiUtils.attachFormSubmitHandlers).to.have.been.calledWith(
        commonProps.updateDatasetAccelerationSettings(commonProps.entity, formValue)
      );
      ApiUtils.attachFormSubmitHandlers.restore();
    });
  });
});

