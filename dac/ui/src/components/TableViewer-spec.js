/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Link } from 'react-router';
import Immutable from 'immutable';

import MainInfoItemName from 'pages/HomePage/components/MainInfoItemName';
import TableViewer from './TableViewer';

describe('TableViewer-spec', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = Immutable.fromJS([{
      name: <MainInfoItemName item={Immutable.fromJS({name: 'ds1'})}/>,
      owner: 'root',
      jobs: <Link to='/space/DG/dsg3'>10</Link>,
      descendants: '12'
    }]);
  });

  it('render table', () => {
    const wrapper = shallow(<TableViewer tableData={commonProps}/>);
    expect(wrapper.hasClass('table')).to.be.true;
  });

  it('TableViewer rows', () => {
    const wrapper = shallow(<TableViewer tableData={commonProps}/>);
    expect(wrapper.find('Tr')).to.have.length(commonProps.size);
  });
});
