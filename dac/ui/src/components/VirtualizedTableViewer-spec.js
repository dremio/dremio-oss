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
import { shallow, mount } from 'enzyme';
import { Link } from 'react-router';
import Immutable from 'immutable';

import MainInfoItemName from 'pages/HomePage/components/MainInfoItemName';
import VirtualizedTableViewer, {DeferredRenderer} from './VirtualizedTableViewer';

describe('VirtualizedTableViewer-spec', () => {
  let commonProps;
  beforeEach(() => {
    commonProps = {
      tableData: Immutable.List([{
        name: {node: <MainInfoItemName item={Immutable.fromJS({name: 'ds1'})}/>},
        owner: {node: 'root'},
        jobs: {node: <Link to='/space/DG/dsg3'>10</Link>},
        descendants: {node: '12'}
      }]),
      columns: [
        {key: 'name', title: 'name'},
        {key: 'owner', title: 'owner'},
        {key: 'jobs', title: 'jobs'},
        {key: 'descendants', title: 'descendants'}
      ]
    };
  });

  it('render Table', () => {
    const wrapper = mount(<VirtualizedTableViewer {...commonProps}/>);
    expect(wrapper.find('Table')).to.have.length(1);
    expect(wrapper.find('Grid')).to.have.length(1);
  });

  describe('handleScroll', () => {
    beforeEach(() => {
      sinon.stub(Date, 'now').returns(100);
      sinon.stub(DeferredRenderer, '_flush');
      sinon.stub(DeferredRenderer, '_scheduleFlush');
    });
    afterEach(() => {
      Date.now.restore();
      DeferredRenderer._flush.restore();
      DeferredRenderer._scheduleFlush.restore();
    });

    it('should calculate speed', () => {
      const instance = shallow(<VirtualizedTableViewer {...commonProps}/>).instance();
      instance.lastScrollTime = 0;
      instance.handleScroll({scrollTop: 200});
      expect(instance.lastSpeed).to.be.equal(2);
      expect(instance.lastScrollTop).to.be.equal(200);
      expect(instance.lastScrollTime).to.be.equal(100);

    });

    it('should flush and schedule flush if scrolling slowly', () => {
      const instance = shallow(<VirtualizedTableViewer {...commonProps}/>).instance();
      instance.lastScrollTime = 0;
      instance.handleScroll({scrollTop: 10});

      expect(DeferredRenderer._flush).to.have.been.called;
      expect(DeferredRenderer._scheduleFlush).to.have.been.called;
    });

    it('should only schedule flush if scrolling fast', () => {
      const instance = shallow(<VirtualizedTableViewer {...commonProps}/>).instance();
      instance.lastScrollTime = 0;
      instance.handleScroll({scrollTop: 10000});

      expect(DeferredRenderer._flush).to.have.not.been.called;
      expect(DeferredRenderer._scheduleFlush).to.have.been.called;
    });

  });
});
