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
import ViewStateWrapper from '../ViewStateWrapper';
import AccelerationForm from './AccelerationForm';
import { AccelerationController } from './AccelerationController';

describe.skip('AccelerationController', () => {
  let minimalProps;
  let commonProps;
  let clock;
  let loadResponse;

  beforeEach(() => {
    loadResponse = {
      payload: Immutable.fromJS({
        result: 'the-id',
        entities: {
          acceleration: {
            'the-id': {
              state: 'FAKE'
            }
          }
        }
      })
    };

    minimalProps = {
      datasetId: 'foo-id',
      getReflections: sinon.stub().resolves({}),
      getDataset: sinon.stub().resolves({})
    };
    commonProps = {
      ...minimalProps,
      onCancel: sinon.spy(),
      onDone: sinon.spy(),
      reflections: Immutable.fromJS({
        id: 'foo-id'
      }),
      viewState: Immutable.Map(),
      resetViewState: sinon.stub()
    };

  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<AccelerationController {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should not render AccelerationForm when acceleration is null', () => {
    const props = {...commonProps, acceleration: null};
    const wrapper = shallow(<AccelerationController {...props}/>);
    expect(wrapper.find(AccelerationForm)).to.have.length(0);
  });

  it('should render ViewStateWrapper when request failed', () => {
    const props = {
      ...commonProps,
      viewState: Immutable.fromJS({isFailed: true})
    };
    const wrapper = shallow(<AccelerationController {...props}/>);
    expect(wrapper.find(ViewStateWrapper)).to.have.length(1);
  });

  it('should otherwise render AccelerationForm when acceleration is map', () => {
    const wrapper = shallow(<AccelerationController {...commonProps}/>);
    expect(wrapper.find(AccelerationForm)).to.have.length(1);
  });

  describe('#componentWillUnmount', function() {
    it('stopPollingAccelerationData should be called', function() {
      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      sinon.stub(instance, 'stopPollingAccelerationData');
      instance.componentWillUnmount();
      expect(instance.stopPollingAccelerationData).to.be.calledOnce;
    });
  });

  describe('#componentWillMount', function() {
    beforeEach(() => {
      sinon.stub(AccelerationController.prototype, 'startPollingAccelerationDataWhileNew');
    });

    afterEach(() => {
      AccelerationController.prototype.startPollingAccelerationDataWhileNew.restore();
    });

    it('startPollingAccelerationDataWhileNew should be called if there is no #entity', function() {
      const instance = shallow(<AccelerationController {...commonProps} entity={null}/>).instance();
      expect(instance.startPollingAccelerationDataWhileNew).to.be.calledOnce;
      expect(commonProps.createEmptyAcceleration).to.not.have.been.called;
    });

    it('createEmptyAcceleration should be called if there is an #entity', function() {
      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      expect(commonProps.createEmptyAcceleration).to.be.calledOnce;
      expect(instance.startPollingAccelerationDataWhileNew).to.not.have.been.called;

      return commonProps.createEmptyAcceleration().then(() => {
        expect(instance.startPollingAccelerationDataWhileNew).to.be.calledOnce;
      });
    });
  });

  describe('#submit', function() {
    it('updateAcceleration should be called', function() {
      sinon.spy(ApiUtils, 'attachFormSubmitHandlers');
      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      instance.submit([]);
      expect(commonProps.updateAcceleration).to.be.calledWith([]);
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
      ApiUtils.attachFormSubmitHandlers.restore();
    });
  });

  describe('#startPollingAccelerationDataWhileNew', function() {
    it('stopPollingAccelerationData should be called', function() {
      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      sinon.spy(instance, 'stopPollingAccelerationData');
      instance.pollId = null;
      instance.startPollingAccelerationDataWhileNew();
      expect(instance.stopPollingAccelerationData).to.be.calledOnce;
      expect(instance.pollId).to.not.be.null;
    });
  });

  describe('#pollAccelerationData', function() {
    it('acceleration record should be loaded, and stop polling if not NEW', function() {
      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      const promise = instance.pollAccelerationData();
      expect(commonProps.loadAccelerationById).to.be.calledOnce;
      return promise.then(() => {
        expect(instance.pollId).to.equal(0); // does not queue another poll
      });
    });

    it('acceleration record should be loaded, and should queue another poll only if NEW', function() {
      loadResponse.payload = loadResponse.payload.setIn(['entities', 'acceleration', 'the-id', 'state'], 'NEW');

      clock = sinon.useFakeTimers();

      const instance = shallow(<AccelerationController {...commonProps}/>).instance();
      const promise = instance.pollAccelerationData();
      sinon.stub(instance, 'pollAccelerationData');
      return promise.then(() => {
        expect(instance.pollId).to.not.equal(0);
        clock.tick(1000);
        expect(instance.pollAccelerationData).to.have.been.called;
        clock.restore();
      });
    });
  });

  describe('#loadAcceleration', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<AccelerationController {...commonProps} entity={null}/>);
      instance = wrapper.instance();
    });

    it('should call loadAccelerationById', () => {
      wrapper.setProps({
        entity: Immutable.fromJS({ }),
        accelerationId: 'id'
      });
      instance.loadAcceleration();
      expect(commonProps.loadAccelerationById).to.be.called;
    });
  });

  describe('#getAccelerationVersion', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<AccelerationController {...commonProps} entity={null}/>).instance();
    });

    it('should return acceleration version when it defined and more than 0', () => {
      expect(instance.getAccelerationVersion({ acceleration: Immutable.fromJS({ version: 1 }) })).to.be.eql(1);
    });

    it('should return undefined when acceleration version is not defined or 0', () => {
      expect(instance.getAccelerationVersion({ acceleration: Immutable.Map()})).to.be.undefined;
      expect(instance.getAccelerationVersion({ acceleration: Immutable.fromJS({ version: 0 })})).to.be.undefined;
    });
  });
});
