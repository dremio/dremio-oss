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
import { Overlay } from 'react-overlays';
import DatasetItemLabel from './DatasetItemLabel';

describe('DatasetItemLabel', () => {

  let commonProps;
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      fullPath: Immutable.List(['Prod-sample', 'ds1']),
      typeIcon: 'VirtualDataset'
    };
    commonProps = {
      ...minimalProps,
      showFullPath: false
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetItemLabel {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find('EllipsedText').first().props().text).to.equal('ds1');
  });

  it('should render TextHighlight, DatasetOverlayContent', () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps}/>);
    expect(wrapper.find('TextHighlight')).to.have.length(1);
    expect(wrapper.find(Overlay)).to.have.length(1);
  });

  it('should show fullPath in header', () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps}/>);
    expect(wrapper.find('TextHighlight')).to.have.length(1);
    wrapper.setProps({ showFullPath: true });
    expect(wrapper.find('TextHighlight')).to.have.length(2);
  });

  it('should render custom node', () => {
    const customNode = (
      <div className='customNode'>DG10</div>
    );
    const wrapper = shallow(<DatasetItemLabel {...commonProps} customNode={customNode}/>);
    expect(wrapper.find('.customNode')).to.have.length(1);
    expect(wrapper.find('TextHighlight')).to.have.length(0);
  });

  it('should only show Overlay and Portal when dataset is not new', () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps}/>);
    expect(wrapper.find(Overlay)).to.have.length(1);
    wrapper.setProps({ isNewQuery: true });
    expect(wrapper.find(Overlay)).to.have.length(0);
  });

  it('should not show Overlay and Portal for shouldShowOverlay=false', () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps} shouldShowOverlay={false}/>);
    expect(wrapper.find(Overlay)).to.have.length(0);
  });

  it('should only show Portal if state.isOpenOverlay=true and state.isDragInProgress=false', () => {
    const wrapper = shallow(<DatasetItemLabel {...commonProps}/>);
    expect(wrapper.find('Portal')).to.have.length(0);
    wrapper.setState({
      isOpenOverlay: true
    });
    expect(wrapper.find('Portal')).to.have.length(1);
    wrapper.setState({
      isDragInProgress: true
    });
    expect(wrapper.find('Portal')).to.have.length(0);

  });
});
