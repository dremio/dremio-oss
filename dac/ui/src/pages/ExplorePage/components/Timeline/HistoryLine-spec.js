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

import TimeDot from 'pages/ExplorePage/components/Timeline/TimeDot';

import HistoryLine from './HistoryLine';

describe('HistoryLine', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      tipVersion: 'abcde',
      activeVersion: '12345',
      location: {},
      datasetPathname: 'Prod-Sample.ds1',
      historyItems: Immutable.fromJS([
        {datasetVersion: '12345'}, {datasetVersion: 'abcde'}
      ]),
      onTimeDotClick: sinon.spy()
    };
  });

  it('renders TimeDots', () => {
    const wrapper = shallow(<HistoryLine {...commonProps}/>);
    const timeDots = wrapper.find(TimeDot);
    expect(timeDots).to.have.length(commonProps.historyItems.size);

    const props = timeDots.first().props();
    expect(props.historyItem).to.equal(commonProps.historyItems.get(0));
    expect(props.isLast).to.equal(false);
    expect(props.activeVersion).to.equal(commonProps.activeVersion);
    expect(props.tipVersion).to.equal(commonProps.tipVersion);

    expect(timeDots.at(1).props().isLast).to.equal(true);
  });
});
