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

import dataStoreUtils from 'utils/dataStoreUtils';

import Tabs from 'components/Tabs';
import ReplacePatternForm from 'pages/ExplorePage/components/Transform/components/forms/ReplacePatternForm';
import ExtractListForm from 'pages/ExplorePage/components/Transform/components/forms/ExtractListForm';
import ExtractMapForm from 'pages/ExplorePage/components/Transform/components/forms/ExtractMapForm';

import { LIST, MAP } from 'constants/DataTypes';

import cards from './mocks/cards.json';
import fields from './mocks/fields.json';

import TransformView from './TransformView';

describe('TransformView', () => {

  let commonProps;
  let context;
  let wrapper;
  beforeEach(() => {
    commonProps = {
      transform: Immutable.fromJS({
        columnType: LIST,
        transformType: 'extract',
        selection: {
          cellText: 'foo'
        }
      }),
      onTransformChange: () => {},
      cards: Immutable.fromJS(cards),
      type: 'extract',
      subTitles: dataStoreUtils.getSubtypeForTransformTab(),
      submit: () => {},
      cancel: () => {},
      loadTransformCardPreview: () => {},
      fields,
      location: {
        state: {
          hasSelection: true
        }
      }
    };
    context = {
      router : {push: sinon.spy()},
      location: {
        state: {
          hasSelection: true
        }
      }
    };

    wrapper = shallow(<TransformView {...commonProps}/>, {context});
  });

  it('should render .transform', () => {
    wrapper = shallow(<TransformView {...commonProps}/>, {context});
    const children = wrapper.children();
    expect(wrapper.hasClass('transform')).to.equal(true);
    expect(children.length).to.equal(2);
    expect(children.at(0).hasClass('subTitles')).to.equal(true);
  });

  it('should render subtitle', () => {
    expect(wrapper.find('.subTitles')).to.have.length(1);
  });

  it('should render Tabs', () => {
    expect(wrapper.find(Tabs)).to.have.length(6);
    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.exist;
    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql('extract');
    expect(wrapper.find(Tabs).at(1).prop('activeTab')).to.exist;
    expect(wrapper.find(Tabs).at(1).prop('tabId')).to.exist;
  });

  it('should render ReplacePatternForm', () => {
    const transform = commonProps.transform.merge({transformType: 'replace', method: 'Pattern'});
    wrapper = shallow(<TransformView {...commonProps} transform={transform}/>, {context});

    expect(wrapper.find(Tabs)).to.have.length(6);
    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql('replace');
    expect(wrapper.find(ReplacePatternForm)).to.exist;
    expect(wrapper.find(ReplacePatternForm).at(0).prop('transform')).to.eql(transform);
    expect(
      wrapper.find(ReplacePatternForm).at(0).prop('loadTransformCardPreview')
    ).to.eql(commonProps.loadTransformCardPreview);
  });

  it('should render ExtractListForm', () => {
    const transform = commonProps.transform.merge({
      transformType: 'extract',
      columnType: LIST
    });
    wrapper = shallow(<TransformView {...commonProps} transform={transform}/>, {context});

    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql('extract');
    expect(wrapper.find(Tabs).at(1).prop('activeTab')).to.eql(LIST);
    expect(wrapper.find(ExtractListForm)).to.have.length(1);
  });

  it('should render ExtractMapForm', () => {
    const transform = commonProps.transform.merge({
      transformType: 'extract',
      columnType: MAP
    });
    wrapper = shallow(<TransformView {...commonProps} transform={transform}/>, {context});

    expect(wrapper.find(Tabs).at(0).prop('activeTab')).to.eql('extract');
    expect(wrapper.find(Tabs).at(1).prop('activeTab')).to.eql(MAP);
    expect(wrapper.find(ExtractMapForm)).to.have.length(1);
  });

  describe('filterSubtitles', () => {
    const transform = Immutable.fromJS({columnType: 'type1'});
    const subTitles = [{
      types: ['type1']
    }, {
      types: ['type2']
    }];

    it('should render subTitles with a type that matches transform.columnType', () => {
      const instance = shallow(<TransformView {...commonProps}
        subTitles={subTitles}
        transform={transform}/>).instance();
      sinon.stub(instance, 'renderSubHeadersTitle', (subtitle) => (subtitle));
      expect(instance.filterSubtitles()[0]).to.eql([{types: ['type1']}]);
    });
  });

});
