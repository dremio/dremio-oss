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

import exploreUtils from 'utils/explore/exploreUtils';
import { PageTypes } from '@app/pages/ExplorePage/pageTypes';

import TableControlsView from './TableControlsView';
import { TableControls } from './TableControls';

describe('TableControls', () => {

  let minimalProps;
  let commonProps;
  let context;
  let wrapper;
  let instance;

  beforeEach(() => {
    minimalProps = {
      performTransform: sinon.spy(),
      transformHistoryCheck: sinon.spy(),
      pageType: PageTypes.default,
      sqlState: false,
      dataset: Immutable.Map({datasetVersion: '12345'}),
      sqlSize: 300,
      exploreViewState: Immutable.Map(),
      collapseExploreSql: sinon.stub(),
      location: {
        pathname: 'loc'
      }
    };
    commonProps = {
      ...minimalProps,
      currentSql: 'select * from foo',
      queryContext: Immutable.List(['context'])
    };
    context = {
      location: {},
      router: {
        push: sinon.spy()
      }
    };

    wrapper = shallow(<TableControls {...commonProps}/>, {context});
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<TableControls {...minimalProps}/>,  {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render TableControlsView', () => {
    expect(wrapper.find(TableControlsView)).to.have.length(1);
  });

  describe('navigateToTransformWizard', () => {
    const params = {params: 'params'};
    beforeEach(() => {
      sinon.stub(exploreUtils, 'getLocationToGoToTransformWizard').returns('location');
      instance.navigateToTransformWizard(params);
    });

    afterEach(() => {
      exploreUtils.getLocationToGoToTransformWizard.restore();
    });

    it('should call navigate to wizard location', () => {
      //call callback
      commonProps.performTransform.args[0][0].callback();
      expect(
        exploreUtils.getLocationToGoToTransformWizard
      ).to.be.calledWith({...params, location: commonProps.location});
      expect(context.router.push).to.be.calledWith('location');
    });
  });
});
