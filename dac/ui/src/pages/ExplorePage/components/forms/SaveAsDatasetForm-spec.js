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

import ResourceTreeController from 'components/Tree/ResourceTreeController';
import DependantDatasetsWarning from 'components/Modals/components/DependantDatasetsWarning';
import { SaveAsDatasetForm, FIELDS } from 'pages/ExplorePage/components/forms/SaveAsDatasetForm';
import { minimalFormProps } from 'testUtil';

const context = {
  context: {
    location: {query: {}},
    username: 'dremio'
  }
};

describe('SaveAsDatasetForm', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onCancel: sinon.spy(),
      onFormSubmit: sinon.spy(),
      ...minimalFormProps(FIELDS)
    };
    commonProps = {
      ...minimalProps,
      ...minimalFormProps(FIELDS),
      datasetType: 'VIRTUAL_DATASET'
    };
    commonProps.fields.name.value = 'name';
    commonProps.fields.name.location = 'location';
    commonProps.fields.location.initialValue = Immutable.fromJS(['foo', 'bar']);
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SaveAsDatasetForm {...minimalProps}/>, context);
    expect(wrapper).to.have.length(1);
  });

  it('should render ModalForm, FormBody and ResourceTree', () => {
    const wrapper = shallow(<SaveAsDatasetForm {...commonProps}/>, context);

    expect(wrapper.find('ModalForm')).to.have.length(1);
    expect(wrapper.find('FormBody')).to.have.length(1);
    expect(wrapper.find(ResourceTreeController)).to.have.length(1);
  });

  it('should render FieldWithError and TextField for this.props.fields.name', () => {
    const wrapper = shallow(<SaveAsDatasetForm {...commonProps}/>, context);
    expect(wrapper.find('FieldWithError').prop('value')).to.equal(commonProps.fields.name.value);
    expect(wrapper.find('TextField').prop('value')).to.equal(commonProps.fields.name.value);
  });

  it('should render renderWarning if there are any dependentDatasets', () => {
    const wrapper = shallow(<SaveAsDatasetForm {...commonProps} dependentDatasets={['dependentDatasets']}/>, context);
    expect(wrapper.find(DependantDatasetsWarning)).to.have.length(1);
  });

  describe('#handleChangeSelectedNode', () => {
    it('should call fields.location.onChange with fullPathList', () => {
      const wrapper = shallow(<SaveAsDatasetForm {...commonProps}/>, context);
      const selected = 'my.folder';
      wrapper.instance().handleChangeSelectedNode(selected);
      expect(commonProps.fields.location.onChange).to.have.been.calledWith('my.folder');
    });

    it('should call fields.location.onChange with fullPathList that has a home space', () => {
      const wrapper = shallow(<SaveAsDatasetForm {...commonProps}/>, context);
      const selected = '"@home".folder';
      wrapper.instance().handleChangeSelectedNode(selected);
      expect(commonProps.fields.location.onChange).to.have.been.calledWith('"@home".folder');
    });

    it('should call fields.location.onChange with fullPath of node', () => {
      const wrapper = shallow(<SaveAsDatasetForm {...commonProps}/>, context);
      const selected = 'my.folder';
      const node = Immutable.fromJS({
        fullPath: ['foo']
      });
      wrapper.instance().handleChangeSelectedNode(selected, node);
      expect(commonProps.fields.location.onChange).to.have.been.calledWith('foo');
    });
  });
});
