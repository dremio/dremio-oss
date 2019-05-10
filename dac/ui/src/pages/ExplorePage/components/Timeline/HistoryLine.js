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
import { Component } from 'react';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable  from 'immutable';
import Radium from 'radium';

import { HISTORY_PANEL_SIZE } from 'uiTheme/radium/sizes';
import { GREY } from 'uiTheme/radium/colors';

import TimeDot from './TimeDot';
import './HistoryLine.less';

@pureRender
@Radium
export default class HistoryLine extends Component {

  static propTypes = {
    historyItems: PropTypes.instanceOf(Immutable.List),
    tipVersion: PropTypes.string,
    activeVersion: PropTypes.string,
    location: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { historyItems, tipVersion, activeVersion, location } = this.props;
    return (
      <div className='history-line' style={[styles.base]}>
        { historyItems.map((item, index, arr) =>
          <TimeDot
            location={location}
            historyItem={item}
            key={item.get('datasetVersion')}
            isLast={index === arr.size - 1}
            tipVersion={tipVersion}
            activeVersion={activeVersion}
          />
        )
        }
      </div>
    );
  }
}

const styles = {
  base: {
    overflowX: 'hidden',
    overflowY: 'auto',
    width: HISTORY_PANEL_SIZE,
    minWidth: HISTORY_PANEL_SIZE,
    position: 'relative',
    backgroundColor: GREY,
    display: 'flex',
    alignItems: 'center',
    flexDirection: 'column'
  }
};
