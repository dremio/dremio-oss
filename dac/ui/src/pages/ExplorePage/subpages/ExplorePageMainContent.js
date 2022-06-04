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
import Immutable from 'immutable';

import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';

import { connect } from 'react-redux';
import TopSplitterContent from '../components/TopSplitterContent';
class ExplorePageMainContent extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    pageType: pageTypesProp,
    startDrag: PropTypes.func,
    sqlState: PropTypes.bool,
    sqlSize: PropTypes.number,
    toggleRightTree: PropTypes.func.isRequired,
    dragType: PropTypes.string,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    sidebarCollapsed: PropTypes.bool,
    handleSidebarCollapse: PropTypes.func
  };

  static contextTypes = {
    location: PropTypes.object
  }

  explorePageMainContentRef = null;

  constructor(props) {
    super(props);
    this.handleSidebarCollapse = this.handleSidebarCollapse.bind(this);
  }

  handleSidebarCollapse = () => {
    this.props.handleSidebarCollapse();
  }

  render() {
    const { dataset, pageType, dragType, sqlState, sqlSize, sidebarCollapsed } = this.props;

    return (
      <>
        {
          pageType === PageTypes.default && // show sql editor only on data page
          <TopSplitterContent
            startDrag={this.props.startDrag}
            sqlState={sqlState}
            sqlSize={sqlSize}
            dataset={dataset}
            dragType={dragType}
            exploreViewState={this.props.exploreViewState}
            handleSidebarCollapse={this.handleSidebarCollapse}
            sidebarCollapsed={sidebarCollapsed}
            ref={(ref) => this.explorePageMainContentRef = ref}>
          </TopSplitterContent>
        }
      </>
    );
  }
}

export default connect(null, null, null, { forwardRef: true })(ExplorePageMainContent);
