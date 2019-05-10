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

import DatasetsSearch from './DatasetsSearch';

describe('DatasetsSearch-spec', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      searchData: Immutable.fromJS([
        {
          fullPath: ['@test_user', 'business_review'],
          displayFullPath: ['@test_user', 'business_review'],
          context: ['@test_user'],
          'parents': [{
            datasetPathList: ['@test_user', 'business_review'],
            type: 'VIRTUAL_DATASET'
          }],
          fields: [{name: 'A', type: 'BIGINT'}, {name: 'B', type: 'BIGINT'}],
          datasetType: 'VIRTUAL_DATASET',
          links: {
            edit: '/space/"@test_user"/"business_review"?mode=edit',
            self: '/space/"@test_user"/"business_review"'
          }
        },
        {
          fullPath: ['@test_user', 'foo'],
          displayFullPath: ['@test_user', 'foo'],
          context: ['@test_user'],
          parents: [{
            datasetPathList: ['@test_user', 'foo'],
            type: 'VIRTUAL_DATASET'
          }],
          fields: [{name: 'A', type: 'BIGINT'}, {name: 'B', type: 'BIGINT'}],
          datasetType: 'VIRTUAL_DATASET',
          links: {
            self: '/space/"@test_user"/"foo"',
            edit: '/space/"@test_user"/"foo"?mode=edit'
          }
        }
      ]),
      visible: true,
      globalSearch: true,
      searchViewState: Immutable.fromJS({isInProgress: false}),
      inputValue: 'foo',
      handleSearchHide: () => {}
    };
  });

  it('render elements', () => {
    const wrapper = shallow(<DatasetsSearch {...commonProps}/>);
    expect(wrapper.find('.datasets-search')).have.length(1);
    expect(wrapper.find('.dataset-wrapper')).have.length(1);
    expect(wrapper.find('.dataset-wrapper').find('Connect(ViewStateWrapper)')).to.have.length(1);
    expect(wrapper.find('h3')).have.length(1);
  });

  it('renders bad data without exploding', () => {
    const wrapper = shallow(<DatasetsSearch {...commonProps} searchData={Immutable.List()}/>);
    expect(wrapper.find('.datasets-search')).have.length(1);
  });

  it('check content', () => {
    const wrapper = shallow(<DatasetsSearch {...commonProps}/>);
    const dataset = wrapper.find('.dataset-wrapper').find('.dataset');
    const mainSettingsBtn = dataset.at(1).find('.main-settings-btn');

    expect(wrapper.find('h3').text()).to.contain('Search Results for "foo"');
    expect(dataset).have.length(2);

    expect(dataset.at(0).prop('to'))
      .equal(commonProps.searchData.get(0).getIn(['links', 'self']));
    expect(dataset.at(1).prop('to'))
      .equal(commonProps.searchData.get(1).getIn(['links', 'self']));
    expect(mainSettingsBtn.childAt(0).prop('to'))
      .equal(commonProps.searchData.get(1).getIn(['links', 'edit']));
    expect(mainSettingsBtn.childAt(1).prop('to'))
      .equal(commonProps.searchData.get(1).getIn(['links', 'self']));
  });
});
