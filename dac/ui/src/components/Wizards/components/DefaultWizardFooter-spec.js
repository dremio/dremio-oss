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

import DefaultWizardFooter from 'components/Wizards/components/DefaultWizardFooter';


const getApplyProps = (wrapper) => wrapper.find('SimpleButton').at(0).props();
const getPreviewProps = (wrapper) => wrapper.find('SimpleButton').at(1).props();
const getCancelProps = (wrapper) => wrapper.find('SimpleButton').at(2).props();
const getButtonText = (wrapper, buttonIndex) => wrapper.find('SimpleButton').at(buttonIndex).children().text();

describe('DefaultWizardFooter', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onFormSubmit: sinon.spy(),
      onCancel: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DefaultWizardFooter {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Apply, Preview and Cancel buttons', () => {
    const wrapper = shallow(<DefaultWizardFooter {...commonProps}/>);
    expect(getApplyProps(wrapper).buttonStyle).to.eql('primary');

    expect(getPreviewProps(wrapper).buttonStyle).to.eql('secondary');

    expect(getCancelProps(wrapper).buttonStyle).to.eql('secondary');
  });

  it('should render Apply and Cancel button when isPreviewAvailable is false', () => {
    const wrapper = shallow(<DefaultWizardFooter {...commonProps} isPreviewAvailable={false}/>);
    expect(wrapper.find('SimpleButton')).to.have.length(2);
    expect(getButtonText(wrapper, 0)).to.eql('Apply');
    expect(getButtonText(wrapper, 1)).to.eql('Cancel');
  });

  describe('onButtonClick', () => {
    let instance;
    let wrapper;
    beforeEach(() => {
      wrapper =  shallow(<DefaultWizardFooter {...commonProps}/>);
      instance = wrapper.instance();
    });

    it('should set submitType', () => {
      instance.onButtonClick('apply');
      expect(wrapper.state().submitType).to.eql('apply');
    });

    it('should call onFormSubmit', () => {
      instance.onButtonClick('apply');
      expect(commonProps.onFormSubmit).to.have.been.calledWith('apply');
    });

    it('should call onFormSubmit via handleSubmit if it exists', () => {
      const handleSubmit = sinon.stub().returnsArg(0);
      wrapper.setProps({handleSubmit});
      instance.onButtonClick('apply');
      expect(commonProps.onFormSubmit).to.have.been.called;
      expect(handleSubmit).to.have.been.called;
    });
  });

  describe('submitting', () => {
    it('should set submitting on submitState button and disable the other button', () => {
      const wrapper = shallow(<DefaultWizardFooter {...commonProps} submitting/>);
      expect(getApplyProps(wrapper).submitting).to.be.false;
      // do not block apply button even a preview is in progress
      expect(getApplyProps(wrapper).disabled).to.be.false;
      expect(getPreviewProps(wrapper).submitting).to.be.false;
      expect(getPreviewProps(wrapper).disabled).to.be.true;

      wrapper.setState({submitType: 'apply'});
      expect(getApplyProps(wrapper).submitting).to.be.true;
      expect(getApplyProps(wrapper).disabled).to.be.false;
      expect(getPreviewProps(wrapper).submitting).to.be.false;
      expect(getPreviewProps(wrapper).disabled).to.be.true;

      wrapper.setState({submitType: 'preview'});
      expect(getApplyProps(wrapper).submitting).to.be.false;
      // do not block apply button even a preview is in progress
      expect(getApplyProps(wrapper).disabled).to.be.false;
      expect(getPreviewProps(wrapper).submitting).to.be.true;
      expect(getPreviewProps(wrapper).disabled).to.be.false;
    });
  });
});
