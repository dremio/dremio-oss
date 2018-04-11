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
import exploreUtils from 'utils/explore/exploreUtils';

import { ReplaceRangeForm } from './ReplaceRangeForm';

const generateChartData = (length) => {
  const data = [];
  for (let i = 0; i < length; i++) {
    const item = {
      x: i,
      y: 1,
      percent: 100 / length,
      range: {
        lowerLimit: i,
        upperLimit: i + 1
      }
    };
    data.push(item);
  }

  return data;
};

describe('ReplaceRangeForm', () => {
  let minimalProps;
  let commonProps;
  let values;
  beforeEach(() => {
    sinon.stub(exploreUtils, 'getDocumentWidth').returns(600);
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'age',
        transformType: 'replace',
        columnType: 'INTEGER'
      }),
      cardValues: Immutable.fromJS({
        values: [
          {
            value: 1,
            valueRange: { lowerLimit: 1, upperLimit: 3 }
          }
        ]
      })
    };
    commonProps = {
      ...minimalProps,
      submit: sinon.stub().returns('submitResponse')
    };
    values = {
      newFieldName: 'age',
      dropSourceField: false,
      lowerBound: 2,
      upperBound: 4,
      keepNull: false,
      replaceType: 'VALUE',
      replacementValue: '1'
    };
  });

  afterEach(() => {
    exploreUtils.getDocumentWidth.restore();
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplaceRangeForm {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  describe('submit', () => {

    it('should pass submitType to props.submit', () => {
      const wrapper = shallow(<ReplaceRangeForm {...commonProps}/>);
      wrapper.instance().submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit when transformType=replace', () => {
      const wrapper = shallow(<ReplaceRangeForm {...commonProps}/>);
      expect(wrapper.instance().submit(values)).to.eql('submitResponse');

      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(values, commonProps.transform),
        fieldTransformation: {
          type: 'ReplaceRange',
          ...fieldsMappers.getReplaceRange(values, commonProps.transform.get('columnType'))
        }
      });
    });

    it('should return correct values on submit when transformType=keeponly', () => {
      const transform = Immutable.Map({
        columnName: 'age',
        transformType: 'keeponly',
        columnType: 'INTEGER'
      });
      const wrapper = shallow(<ReplaceRangeForm {...commonProps} transform={transform}/>);
      wrapper.instance().submit(values);
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeRange(values, transform.get('columnType'))
      });
    });
  });

  describe('combineBins', () => {
    it('should combine bins and decrease data length by 2', () => {
      const wrapper = shallow(<ReplaceRangeForm {...minimalProps} />);
      const instance = wrapper.instance();
      sinon.stub(instance, 'getMaxBinsCountByWidth').returns(50);
      const chartData = generateChartData(60);
      const originalBinLength = chartData[0].range.upperLimit - chartData[0].range.lowerLimit;
      const result = instance.combineBins(chartData);
      const resultBinLength = result[0].range.upperLimit - result[0].range.lowerLimit;
      expect(result).to.have.length(30);
      expect(resultBinLength / 2).to.equal(originalBinLength);
      expect(result[0].y).to.equal(chartData[0].y + chartData[1].y);
      expect(result[0].percent).to.equal(chartData[0].percent + chartData[1].percent);
      expect(result[0].x).to.equal(chartData[0].x);
      expect(result[0].range.lowerLimit).to.equal(chartData[0].range.lowerLimit);
      expect(result[0].range.upperLimit).to.equal(chartData[1].range.upperLimit);
    });
  });

  describe('getMaxBinsCountByWidth', () => {
    it('should return proper bins count based on width', () => {
      const wrapper = shallow(<ReplaceRangeForm {...minimalProps} />);
      const instance = wrapper.instance();
      expect(instance.getMaxBinsCountByWidth(1000)).to.equal(1000 / 20);
      expect(instance.getMaxBinsCountByWidth(1200)).to.equal(1200 / 20);
      expect(instance.getMaxBinsCountByWidth(1350)).to.equal(1350 / 20);
      expect(instance.getMaxBinsCountByWidth(1500)).to.equal(1500 / 20);
    });
  });
});
