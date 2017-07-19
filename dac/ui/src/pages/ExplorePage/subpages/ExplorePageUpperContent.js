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
import { Component, PropTypes } from 'react';
import classNames from 'classnames';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';

import PullOutRightTree from 'components/PullOutRightTree';

import SqlEditorController from './../components/SqlEditor/SqlEditorController';
import ExploreInfoHeader from './../components/ExploreInfoHeader';
import TopSplitterContent from './../components/TopSplitterContent';

const MAX_WIDTH_SETTINGS_PANEL = 290;

@pureRender
@Radium
export default class ExplorePageUpperContent extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    startDrag: PropTypes.func,
    rightTreeVisible: PropTypes.bool,
    sqlState: PropTypes.bool,
    sqlSize: PropTypes.number,
    toggleRightTree: PropTypes.func.isRequired,
    dragType: PropTypes.string,
    exploreViewState: PropTypes.instanceOf(Immutable.Map)
  };

  static contextTypes = {
    location: PropTypes.object
  }

  render() {
    const { dataset, dragType, rightTreeVisible, sqlState, sqlSize } = this.props;
    const pullClass = classNames('settings-block',
      {'opened': rightTreeVisible},
      {'closed': !rightTreeVisible}
    );
    const widthSettingsPanel = rightTreeVisible ? MAX_WIDTH_SETTINGS_PANEL : 0;

    return (
      <TopSplitterContent startDrag={this.props.startDrag} sqlState={sqlState} sqlSize={sqlSize}>
        <div>
          <div className='wrap'>
            <div className='inner-wrap' style={styles.innerWrap}>
              <ExploreInfoHeader
                dataset={dataset}
                toggleRightTree={this.props.toggleRightTree}
                rightTreeVisible={rightTreeVisible}
                exploreViewState={this.props.exploreViewState}
              />
              <SqlEditorController
                dataset={dataset}
                dragType={dragType}
                sqlState={sqlState}
                sqlSize={sqlSize}
                exploreViewState={this.props.exploreViewState}
              />
            </div>
            { false && /* this feature disabled for now */
              <div className={pullClass} style={{ width: widthSettingsPanel }}>
                <PullOutRightTree
                  entity={this.props.dataset}
                  entityType={this.props.dataset.get('entityType')}
                  rightTreeVisible={this.props.rightTreeVisible}
                  toggleVisibility={this.props.toggleRightTree}
                />
              </div>
            }
          </div>
        </div>
      </TopSplitterContent>
    );
  }
}

const styles = {
  innerWrap: {
    width: '100%'
  }
};
