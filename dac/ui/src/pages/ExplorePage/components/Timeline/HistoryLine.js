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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable  from 'immutable';
import Art from '@app/components/Art';

import { PageTypes, pageTypesProp } from '../../pageTypes';

import TimeDot from './TimeDot';

import './HistoryLine.less';

export default class HistoryLine extends PureComponent {

  static propTypes = {
    historyItems: PropTypes.instanceOf(Immutable.List),
    tipVersion: PropTypes.string,
    activeVersion: PropTypes.string,
    location: PropTypes.object,
    pageType: pageTypesProp
  };

  constructor(props) {
    super(props);
  }

  renderContent() {
    const { historyItems, tipVersion, activeVersion, location, pageType } = this.props;
    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return <></>;
    case PageTypes.default:
      return <div className='historyLine'>
        <Art
          src='DateTime.svg'
          alt=''
          style={{
            height: 20,
            width: 20,
            marginTop: 10,
            marginBottom: 10,
            filter: 'invert(54%) sepia(9%) saturate(412%) hue-rotate(165deg) brightness(97%) contrast(97%)'
          }}
        />

        <hr/>

        <div className='timeDotContainer'>
          { historyItems.map((item, index, arr) =>
            <TimeDot
              location={location}
              historyItem={item}
              key={item.get('datasetVersion')}
              isLast={index === arr.size - 1}
              tipVersion={tipVersion}
              activeVersion={activeVersion}
            />
          )}
        </div>
      </div>;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  render() {
    return (
      this.renderContent()
    );
  }
}
