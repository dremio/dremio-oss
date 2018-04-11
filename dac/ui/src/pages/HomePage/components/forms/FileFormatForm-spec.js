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
import merge from 'lodash/merge';
import { injectIntl } from 'react-intl';
import Immutable from 'immutable';
import { minimalFormProps } from 'testUtil';
import { FileFormatForm } from './FileFormatForm';
import { ExcelFormatForm, TextFormatForm, XLSFormatForm } from './FormatForms';

const FileFormatFormIntl = injectIntl(FileFormatForm);
describe('FileFormatForm', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      ...minimalFormProps(),
      handleSubmit: fn => fn,
      onPreview: sinon.spy(),
      fields: {
        type: {
          value: 'Text'
        }
      },
      previewViewState: Immutable.Map({
        isInProgress: false,
        isFailed: false
      }),
      onCancel: () => {},
      onFormSubmit: () => {},
      viewState: Immutable.Map(),
      previewData: Immutable.fromJS({rows: []})
    };
    commonProps = {
      ...minimalProps,
      fields: {
        type: {
          value: 'Text'
        },
        Text: {
          fieldDelimiter: {
            value: ','
          }
        }
      },
      dirty: false,
      valid: true,
      onFormSubmit: sinon.spy(),
      viewState: Immutable.Map({
        isInProgress: false,
        isFailed: false
      })
    };
    wrapper = shallow(<FileFormatFormIntl {...commonProps}/>);
  });


  describe('Rendering', () => {

    it('renders <ModalForm>', () => {
      const wrap = shallow(<FileFormatFormIntl {...minimalProps}/>);
      expect(wrap.find('ModalForm')).to.have.length(1);
    });

    it('renders type field', () => {
      expect(wrapper.find('Select').first().props().value).to.eql(commonProps.fields.type.value);
    });

    it('renders Format form according to type field', () => {
      expect(wrapper.find('TextFormatForm')).to.have.length(1);
      expect(wrapper.find('ExcelFormatForm')).to.have.length(0);

      const excelFields = merge({}, commonProps.fields, {type: {value: 'Excel'}});
      wrapper = shallow(<FileFormatFormIntl {...commonProps} fields={excelFields}/>);
      expect(wrapper.find('ExcelFormatForm')).to.have.length(1);
    });

    it('renders ExploreTableController when there is previewData', () => {
      expect(wrapper.find('ExploreTableController')).to.have.length(0);
    });
  });

  describe.skip('View State', () => {
    it('renders spinner when isInProgress', () => {
      expect(wrapper.find('Spinner')).to.have.length(1);
    });
  });

  describe('renderFormatSection', () => {
    it('should render TextFormatForm if current type of format is Text', () => {
      expect(wrapper.find(TextFormatForm)).to.have.length(1);
      expect(wrapper.find(ExcelFormatForm)).to.have.length(0);
      expect(wrapper.find(XLSFormatForm)).to.have.length(0);
    });

    it('should render ExcelFormatForm if current type of format is Excel', () => {
      wrapper.setProps({
        fields: {
          type: {
            value: 'Excel'
          }
        },
        values: {
          type: 'Excel'
        }
      });
      expect(wrapper.find(TextFormatForm)).to.have.length(0);
      expect(wrapper.find(ExcelFormatForm)).to.have.length(1);
      expect(wrapper.find(XLSFormatForm)).to.have.length(0);
    });

    it('should render XLSFormatForm if current type of format is XLS', () => {
      wrapper.setProps({
        fields: {
          type: {
            value: 'XLS'
          }
        },
        values: {
          type: 'XLS'
        }
      });
      expect(wrapper.find(TextFormatForm)).to.have.length(0);
      expect(wrapper.find(ExcelFormatForm)).to.have.length(0);
      expect(wrapper.find(XLSFormatForm)).to.have.length(1);
    });
  });
});
