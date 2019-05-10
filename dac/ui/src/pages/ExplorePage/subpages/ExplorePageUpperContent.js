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
import Radium from 'radium';
import Immutable from 'immutable';

import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';

import SqlEditorController from './../components/SqlEditor/SqlEditorController';
import ExploreInfoHeader from './../components/ExploreInfoHeader';
import TopSplitterContent from './../components/TopSplitterContent';

@pureRender
@Radium
export default class ExplorePageUpperContent extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    pageType: pageTypesProp,
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
    const { dataset, pageType, dragType, rightTreeVisible, sqlState, sqlSize } = this.props;

    return (
      <div>
        <ExploreInfoHeader
          dataset={dataset}
          pageType={pageType}
          toggleRightTree={this.props.toggleRightTree}
          rightTreeVisible={rightTreeVisible}
          exploreViewState={this.props.exploreViewState}
        />
        {
          pageType === PageTypes.default && // show sql editor only on data page
          <TopSplitterContent startDrag={this.props.startDrag} sqlState={sqlState} sqlSize={sqlSize}>
            <div>
              <div className='wrap'>
                <div className='inner-wrap' style={styles.innerWrap}>
                  <SqlEditorController
                    dataset={dataset}
                    dragType={dragType}
                    sqlState={sqlState}
                    sqlSize={sqlSize}
                    exploreViewState={this.props.exploreViewState}
                  />
                </div>
              </div>
            </div>
          </TopSplitterContent>
        }
      </div>
    );
  }
}

const styles = {
  innerWrap: {
    width: '100%'
  }
};
