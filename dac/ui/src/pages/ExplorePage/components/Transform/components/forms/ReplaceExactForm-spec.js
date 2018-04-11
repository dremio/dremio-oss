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

import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';

import {
  INTEGER,
  TEXT
} from '../../../../../../constants/DataTypes';

import { ReplaceExactForm, getInitialReplaceValue } from './ReplaceExactForm';

describe('ReplaceExactForm', () => {
  let minimalProps;
  let commonProps;
  let values;
  let context;
  beforeEach(() => {
    context = {
      router: {
        push: sinon.spy(),
        replace: sinon.spy()
      }
    };
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'age',
        transformType: 'replace',
        columnType: 'INTEGER'
      }),
      submit: sinon.stub().returns('submitResponse'),
      onCancel: sinon.spy(),
      loadTransformValuesPreview: sinon.spy(),
      fields: {replaceValues: [{value: 'address1'}], replaceNull: {value: false}}
    };
    commonProps = {
      ...minimalProps
    };
    values = {
      newFieldName: 'age',
      dropSourceField: false,
      activeCard: 0,
      replaceNull: false,
      replaceTitleType: 'pattern',
      replaceType: 'VALUE',
      replacementValue: '1'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplaceExactForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#componentWillReceiveProps', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<ReplaceExactForm {...commonProps}/>, {context});
      instance = wrapper.instance();
      sinon.stub(instance, 'loadTransformValuesPreview');
    });
    it('should update if replaceValues changed', () => {
      const nextProps = {
        ...commonProps,
        fields: {
          ...commonProps.fields,
          replaceValues: [{value: 'address2'}]
        }
      };
      instance.componentWillReceiveProps(nextProps);
      expect(commonProps.loadTransformValuesPreview).to.be.calledOnce;
    });
    it('should update if replaceNull changed', () => {
      const nextProps = {
        ...commonProps,
        fields: {
          ...commonProps.fields,
          replaceNull: {value: true}
        }
      };
      instance.componentWillReceiveProps(nextProps);
      expect(commonProps.loadTransformValuesPreview).to.be.calledOnce;
    });
  });

  describe('submit', () => {
    it('should pass submitType to props.submit', () => {
      const wrapper = shallow(<ReplaceExactForm {...commonProps}/>);
      wrapper.instance().submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit when transformType=replace', () => {
      const wrapper = shallow(<ReplaceExactForm {...commonProps}/>);

      wrapper.instance().submit(values);
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(values, commonProps.transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceExact(values, commonProps.transform.get('columnType'))
        }
      });
    });

    it('should return correct values on submit when transformType=keeponly', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'keeponly',
        columnType: 'INTEGER'
      });
      const wrapper = shallow(<ReplaceExactForm {...commonProps} transform={transform}/>);

      expect(wrapper.instance().submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeValues(values,  commonProps.transform.get('columnType'))
      });
    });

    it('should return correct values on submit when transformType=exclude', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'exclude',
        columnType: 'INTEGER'
      });

      const wrapper = shallow(<ReplaceExactForm {...commonProps} transform={transform}/>);

      expect(wrapper.instance().submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeValues(values,  commonProps.transform.get('columnType'))
      });
    });
    it('should correctly replace values with null if replaceNull is true', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'exclude',
        columnType: 'INTEGER'
      });

      const nullValues = {
        ...values,
        replaceNull: true
      };

      const expectedValues = {
        ...nullValues,
        replaceValues: [null]
      };

      const wrapper = shallow(<ReplaceExactForm {...commonProps} transform={transform}/>);

      expect(wrapper.instance().submit(nullValues)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(expectedValues, transform),
        filter: filterMappers.mapFilterExcludeValues(expectedValues,  commonProps.transform.get('columnType'))
      });
    });
  });

  describe('#getInitialReplaceValue', () => {
    it('should display firstCardValue if there is no selection', () => {
      expect(getInitialReplaceValue('', INTEGER, 'foo')).to.eql('foo');
    });
    it('should display firstCardValue if selection is null or undefined', () => {
      expect(getInitialReplaceValue(null, INTEGER, 'foo')).to.eql('foo');
      expect(getInitialReplaceValue(undefined, INTEGER, 'foo')).to.eql('foo');
    });
    it('should display the parsed cellText otherwise', () => {
      expect(getInitialReplaceValue('0', INTEGER, 'foo')).to.eql(0);
      expect(getInitialReplaceValue('bar', TEXT, 'foo')).to.eql('bar');
    });
  });
});
