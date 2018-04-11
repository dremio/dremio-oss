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
import ApiUtils from 'utils/apiUtils/apiUtils';
import { AddProvisionModal } from './AddProvisionModal';

describe('AddProvisionModal', () => {
  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    context = {
      router: {
        push: sinon.spy()
      }
    };
    minimalProps = {
      updateFormDirtyState: sinon.spy(),
      location: {
        state: {}
      }
    };
    commonProps = {
      ...minimalProps,
      createProvision: sinon.spy(),
      editProvision: sinon.spy(),
      hide: sinon.spy(),
      showConfirmationDialog: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AddProvisionModal {...minimalProps}/>, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectClusterType only if clusterType is absent', () => {
    const wrapper = shallow(<AddProvisionModal {...commonProps}/>, { context });
    expect(wrapper.find('SelectClusterType')).to.have.length(1);

    wrapper.setProps({
      clusterType: 'YARN'
    });
    expect(wrapper.find('SelectClusterType')).to.have.length(0);
  });

  describe('#getModalTitle', () => {
    let instance;
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<AddProvisionModal {...commonProps}/>, { context });
      instance = wrapper.instance();
    });

    it('should return appropriate title when edit existing configuration', () => {
      wrapper.setProps({
        clusterType: 'YARN',
        provisionId: 'provision_id'
      });
      expect(instance.getModalTitle()).to.be.eql('Edit YARN');
    });

    it('should return appropriate title when create new configuration', () => {
      wrapper.setProps({
        clusterType: 'YARN'
      });
      expect(instance.getModalTitle()).to.be.eql('Set Up YARN');
    });

    it('should return appropriate title if clusterType is absent', () => {
      wrapper.setProps({
        clusterType: null
      });

      expect(instance.getModalTitle()).to.be.eql('Set Up');
    });
  });

  describe('#promptEditProvisionRestart', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<AddProvisionModal {...commonProps}/>, { context });
      instance = wrapper.instance();
      sinon.stub(ApiUtils, 'attachFormSubmitHandlers').returns({
        then: f => {
          f();
          return {
            catch: c => c()
          };
        }
      });
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });

    it('should show confirmation modal', () => {
      instance.promptEditProvisionRestart();
      expect(commonProps.showConfirmationDialog).to.be.called;
    });

    it('should call editProvision when confirmation modal confirmed', () => {
      wrapper.setProps({
        showConfirmationDialog: (options) => options.confirm(),
        location: { state: { provisionId: 'provision_id'}}
      });
      const promise = instance.promptEditProvisionRestart('values');
      expect(commonProps.editProvision).to.be.calledWith('values', 'AddProvisionModal');
      expect(commonProps.hide).to.be.calledWith(null, true);

      return expect(promise).to.be.resolved;
    });
  });

  describe('#isEditMode', () => {
    it('should return false when provisionId is absent', () => {
      const instance = shallow(<AddProvisionModal {...minimalProps}/>, { context }).instance();
      expect(instance.isEditMode()).to.be.false;
    });
    it('should return true when provisionId passed', () => {
      const instance = shallow(<AddProvisionModal {...minimalProps} provisionId='1' />, { context }).instance();
      expect(instance.isEditMode()).to.be.true;
    });
  });

  describe('#submit', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<AddProvisionModal {...commonProps}/>, { context }).instance();
      sinon.stub(ApiUtils, 'attachFormSubmitHandlers').returns({
        then: f => {
          f();
          return {
            catch: c => c()
          };
        }
      });
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });

    it('should call createProvision when not editing', () => {
      sinon.stub(instance, 'isEditMode').returns(false);
      instance.submit('values', false);
      expect(commonProps.createProvision).to.be.calledWith('values', 'AddProvisionModal');
      expect(commonProps.hide).to.be.calledWith(null, true);
    });

    it('should show confirmation when edit existing configuration and restart is required', () => {
      sinon.stub(instance, 'isEditMode').returns(true);
      instance.submit('values', true);
      expect(commonProps.createProvision).to.be.not.called;
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.hide).to.be.not.called;
    });

    it('should not show confirmation when edit existing configuration and restart is not required', () => {
      sinon.stub(instance, 'isEditMode').returns(true);
      instance.submit('values', false);
      expect(commonProps.createProvision).to.be.not.called;
      expect(commonProps.showConfirmationDialog).to.be.not.called;
      expect(commonProps.hide).to.be.called;
      expect(commonProps.editProvision).to.be.calledWith('values', 'AddProvisionModal');
    });
  });
});
