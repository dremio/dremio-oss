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

import { TEXT, BOOLEAN } from 'constants/DataTypes';

import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';

import { ReplaceValuesForm } from './ReplaceValuesForm';

describe('ReplaceValuesForm', () => {
  let minimalProps;
  let commonProps;
  let values;
  let newValues;
  beforeEach(() => {
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'a',
        columnType: TEXT,
        transformType: 'replace'
      }),
      fields: {}
    };
    commonProps = {
      ...minimalProps,
      columnType: TEXT,
      submit: sinon.stub().returns('submitResponse')
    };
    values = {
      newFieldName: 'a2',
      dropSourceField: false,
      activeCard: 0,
      replaceSelectionType: 'VALUE',
      replaceTitleType: 'pattern',
      replaceType: 'VALUE',
      replacementValue: 'ss',
      replaceValues: ['address1']
    };
    newValues = {
      ...values,
      replaceValues: ['address1']
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplaceValuesForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('submit', () => {

    it('should pass submitType to props.submit', () => {
      const wrapper = shallow(<ReplaceValuesForm {...commonProps}/>);
      wrapper.instance().submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit when transformType=replace', () => {
      const wrapper = shallow(<ReplaceValuesForm {...commonProps}/>);

      wrapper.instance().submit(values);
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(newValues, commonProps.transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceValues(newValues, TEXT)
        }
      });
    });

    it('should reject the promise and not return submit if numSelected is 0', () => {
      const emptyValues = {...values, replaceValues: []};
      const wrapper = shallow(<ReplaceValuesForm {...commonProps}/>);
      expect(wrapper.instance().submit(emptyValues)).to.be.rejected;
    });


    it('should return correct values on submit when transformType=keeponly', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'keeponly',
        columnType: TEXT
      });
      const wrapper = shallow(<ReplaceValuesForm {...commonProps} transform={transform}/>);
      expect(wrapper.instance().submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(newValues, transform),
        filter: filterMappers.mapFilterExcludeValues(newValues, transform.get('columnType'))
      });
    });

    it('should return correct values for submit when columnType=BOOLEAN', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'replace',
        columnType: BOOLEAN
      });
      const booleanValues = {
        ...values,
        replacementValue: 'false',
        replaceValues: ['true']
      };
      const mapValues = {
        ...values,
        replacementValue: false,
        replaceValues: [true]
      };
      const wrapper = shallow(<ReplaceValuesForm {...commonProps} columnType={BOOLEAN} transform={transform}/>);
      wrapper.instance().submit(booleanValues);
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(mapValues, transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceValues(mapValues, BOOLEAN)
        }
      });
    });
  });

  describe('onValuesChange', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<ReplaceValuesForm {...commonProps} loadTransformValuesPreview={sinon.spy()}/>).instance();
    });

    it('should run loadTransformValuesPreview when replaceValues were changed', () => {
      instance.onValuesChange({replaceValues: {data: '1'}}, {replaceValues: {data: '2'}});
      expect(instance.props.loadTransformValuesPreview.calledOnce).to.be.true;
    });

    it('should do nothing when replaceValues were not changed', () => {
      instance.onValuesChange({replaceValues: {data: '1'}}, {replaceValues: {data: '1'}});
      expect(instance.props.loadTransformValuesPreview.called).to.be.false;
    });
  });

});
