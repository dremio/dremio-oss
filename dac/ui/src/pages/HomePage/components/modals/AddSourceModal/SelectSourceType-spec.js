/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import {sourceProperties, isExternalSourceType, isDatalakeTableSourceType} from '@app/constants/sourceTypes';
import { expect } from 'chai';
import SelectSourceType from './SelectSourceType';

describe('SelectSourceType', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onSelectSource: sinon.spy(),
      onAddSampleSource: sinon.spy(),
      sourceTypes: sourceProperties
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectSourceType {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectConnectionButton for each external source', () => {
    const externalSources = sourceProperties.filter(source => isExternalSourceType(source.sourceType));
    const wrapper = shallow(<SelectSourceType {...commonProps} isExternalSource/>);
    expect(wrapper.find('SelectConnectionButton')).to.have.length(externalSources.length);
  });

  it('should render two sections for data lake list', () => {
    const wrapper = shallow(<SelectSourceType {...commonProps}/>);
    expect(wrapper.find('.source-type-section')).to.have.length(2);
  });

  it('should render SelectConnectionButton for each data lake file store source + sample source', () => {
    const wrapper = shallow(<SelectSourceType {...commonProps}/>);
    const fileStoreSources = sourceProperties.filter(source => !isExternalSourceType(source.sourceType) && !isDatalakeTableSourceType(source.sourceType));
    // The "+ 1" is for the sample source.  Using last-child as file stores show up on bottom.
    expect(wrapper.find('.source-type-section:last-child SelectConnectionButton')).to.have.length(fileStoreSources.length + 1);
  });

  it('should render SelectConnectionButton for each data lake table store source', () => {
    const wrapper = shallow(<SelectSourceType {...commonProps}/>);
    const tableSources = sourceProperties.filter(source => isDatalakeTableSourceType(source.sourceType));
    // Using first-child as table stores show up on top.
    expect(wrapper.find('.source-type-section:first-child SelectConnectionButton')).to.have.length(tableSources.length);
  });

});
