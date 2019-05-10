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
import { hashHeightTopSplitter } from 'constants/explorePage/heightTopSplitter.js';
import { ExplorePageView as ExplorePage } from './ExplorePage';

describe('ExplorePage', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {
        query: ''
      },
      pageType: 'graph',
      sqlSize: 10,
      dataset: Immutable.fromJS({
        displayFullPath: ['tmp', 'UNTITLED']
      }),
      updateSqlPartSize: sinon.spy(),
      toggleRightTree: sinon.spy(),
      rightTreeVisible: true,
      sqlState: true,
      updateGridSizes: sinon.spy(),
      onUnmount: () => {}
    };
    commonProps = {
      ...minimalProps,
      pageType: 'details'
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ExplorePage {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render with common props without exploding', () => {
    const wrapper = shallow(<ExplorePage {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });
  describe('initSqlEditor', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<ExplorePage {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should call updateSqlPartSize with sqlState true if dataset displayFullPath starts with tmp', () => {
      instance.initSqlEditor(minimalProps);
      expect(minimalProps.updateSqlPartSize.called).to.be.true;
      expect(minimalProps.updateSqlPartSize.calledWith(hashHeightTopSplitter.getDefaultSqlHeight())).to.be.true;
    });
    it('should call updateSqlPartSize with sqlState true if dataset is new', () => {
      const props = {
        ...minimalProps,
        dataset: Immutable.fromJS({
          isNewQuery: true,
          displayFullPath: ['tmp', 'UNTITLED']
        }),
        location: {
          query: '',
          pathname: '/new_query?context=dremio'
        }
      };
      instance.initSqlEditor(props);
      expect(minimalProps.updateSqlPartSize.called).to.be.true;
      expect(minimalProps.updateSqlPartSize.calledWith(hashHeightTopSplitter.getNewQueryDefaultSqlHeight())).to.be.true;
    });
    it('should call updateSqlPartSize with sqlState false if dataset is not new', () => {
      const props = {
        ...minimalProps,
        dataset: Immutable.fromJS({
          displayFullPath: ['@dremio', 'extractMap']
        })
      };

      instance.initSqlEditor(props);
      expect(minimalProps.updateSqlPartSize.called).to.be.true;
      expect(minimalProps.updateSqlPartSize.calledWith(hashHeightTopSplitter.getDefaultSqlHeight())).to.be.true;
    });
    it('should not call updateSqlPartSize if pageType is not default', () => {
      instance.initSqlEditor(commonProps);
      expect(commonProps.updateSqlPartSize.called).to.be.false;
    });
  });
});
