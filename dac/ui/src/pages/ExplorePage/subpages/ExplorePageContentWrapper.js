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
import Immutable from 'immutable';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import Mousetrap from 'mousetrap';

import DataGraph from '@inject/pages/ExplorePage/subpages/datagraph/DataGraph';
import DetailsWizard from 'components/Wizards/DetailsWizard';
import { HISTORY_PANEL_SIZE } from 'uiTheme/radium/sizes';
import { WHITE } from 'uiTheme/radium/colors.js';

import { RECOMMENDED_JOIN } from '@app/constants/explorePage/joinTabs';
import { Wiki } from '@app/pages/ExplorePage/components/Wiki/Wiki';
import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';
import { getDatasetEntityId } from '@app/selectors/explore';
import { runDatasetSql, previewDatasetSql } from 'actions/explore/dataset/run';
import { navigateToExploreDefaultIfNecessary } from 'utils/pathUtils';
import { getExploreViewState } from '@app/selectors/resources';
import Reflections from '@app/pages/ExplorePage/subpages/reflections/Reflections';

import ExploreTableController from './../components/ExploreTable/ExploreTableController';
import JoinTables from './../components/ExploreTable/JoinTables';
import TableControls from './../components/TableControls';
import ExplorePageUpperContent from './ExplorePageUpperContent';

import SqlErrorSection from './../components/SqlEditor/SqlErrorSection';
import {
  base,
  bottomContent
} from './ExplorePageContentWrapper.less';


const EXPLORE_DRAG_TYPE = 'explorePage';

class ExplorePageContentWrapper extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    pageType: pageTypesProp.isRequired,
    rightTreeVisible: PropTypes.bool.isRequired,
    sqlSize: PropTypes.number.isRequired,
    sqlState: PropTypes.bool.isRequired,
    toggleRightTree: PropTypes.func.isRequired,
    startDrag: PropTypes.func.isRequired,
    errorData: PropTypes.object.isRequired,
    isError: PropTypes.bool.isRequired,
    location: PropTypes.object.isRequired,
    setRecommendationInfo: PropTypes.func,
    exploreViewState: PropTypes.instanceOf(Immutable.Map),
    // connected
    entityId: PropTypes.string,
    runDatasetSql: PropTypes.func,
    previewDatasetSql: PropTypes.func,
    canSelect: PropTypes.any
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.renderUpperContent = this.getUpperContent.bind(this);
    this.getBottomContent = this.getBottomContent.bind(this);
    this.getControlsBlock = this.getControlsBlock.bind(this);
  }

  componentDidMount() {
    Mousetrap.bind(['mod+enter', 'mod+shift+enter'], this.kbdShorthand);
  }

  componentWillUnmount() {
    Mousetrap.unbind(['mod+enter', 'mod+shift+enter']);
  }

  kbdShorthand = (e) => {
    if (!e) return;

    const { pageType, location } = this.props;
    navigateToExploreDefaultIfNecessary(pageType, location, this.context.router);

    if (e.shiftKey) {
      this.props.runDatasetSql();
    } else {
      this.props.previewDatasetSql();
    }
  };


  getBottomContent() {
    const {dataset, pageType, location, entityId, canSelect} = this.props;
    const locationQuery = location.query;

    if (locationQuery.type === 'JOIN' && locationQuery.joinTab !== RECOMMENDED_JOIN) {
      return (
        <JoinTables
          pageType={pageType}
          dataset={dataset}
          location={location}
          sqlSize={this.props.sqlSize}
          rightTreeVisible={this.props.rightTreeVisible}
          exploreViewState={this.props.exploreViewState}
        />
      );
    }

    switch (pageType) {
    case PageTypes.graph:
      return (DataGraph && <DataGraph
        ref='gridTable'
        dragType={EXPLORE_DRAG_TYPE}
        dataset={dataset}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
      />);
    case PageTypes.wiki: {
      // should allow edit a wiki only if we receive a entity id and permissions allow this.
      // If we do not receive permissions object, that means the current user is admin (CE)
      const isWikiEditAllowed = entityId && dataset.getIn(['permissions', 'canManageWiki'], true) && dataset.getIn(['permissions', 'canAlter'], true);

      return <Wiki
        entityId={entityId}
        isEditAllowed={isWikiEditAllowed}
        className={bottomContent} />;
    }
    case PageTypes.reflections: {
      return <Reflections
        datasetId={entityId}
      />;
    }
    case PageTypes.default:
    case PageTypes.details:
      return (<ExploreTableController
        pageType={pageType}
        dataset={dataset}
        dragType={EXPLORE_DRAG_TYPE}
        location={location}
        sqlSize={this.props.sqlSize}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
        exploreViewState={this.props.exploreViewState}
        canSelect={canSelect}
      />);
    default:
      throw new Error(`Not supported page type: '${pageType}'`);
    }
  }

  getControlsBlock() {
    const {
      pageType
    } = this.props;

    switch (pageType) {
    case PageTypes.graph:
    case PageTypes.details:
    case PageTypes.reflections:
    case PageTypes.wiki:
      return;
    case PageTypes.default:
      return <TableControls
        dataset={this.props.dataset}
        sqlSize={this.props.sqlSize}
        location={this.props.location}
        pageType={this.props.pageType}
        sqlState={this.props.sqlState}
        rightTreeVisible={this.props.rightTreeVisible}
        exploreViewState={this.props.exploreViewState}
      />;
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  getUpperContent() {
    const {
      pageType,
      canSelect,
      exploreViewState,
      dataset,
      location,
      startDrag,
      rightTreeVisible,
      sqlSize,
      sqlState,
      toggleRightTree
    } = this.props;

    switch (pageType) {
    case PageTypes.details:
      return <DetailsWizard
        dataset={dataset}
        location={location}
        startDrag={startDrag}
        dragType={EXPLORE_DRAG_TYPE}
        exploreViewState={exploreViewState}
        canSelect={canSelect}
      />;
    case PageTypes.default:
    case PageTypes.reflections:
    case PageTypes.graph:
    case PageTypes.wiki:
      return (
        <ExplorePageUpperContent
          dataset={dataset}
          pageType={pageType}
          rightTreeVisible={rightTreeVisible}
          sqlSize={sqlSize}
          sqlState={sqlState}
          dragType={EXPLORE_DRAG_TYPE}
          toggleRightTree={toggleRightTree}
          startDrag={startDrag}
          exploreViewState={exploreViewState}
        />
      );
    default:
      throw new Error(`not supported page type; '${pageType}'`);
    }
  }

  render() {
    const tableViewerStyle = this.props.rightTreeVisible
      ? styles.tableViewerStyleShort
      : styles.tableViewerStyleFull;

    return (
      <div className={classNames('table-parent', base)} style={tableViewerStyle}>
        {this.getUpperContent()}
        <div className='table-control-wrap' style={styles.tableControlWrap}>
          {this.getControlsBlock()}
          <SqlErrorSection
            visible={this.props.isError}
            errorData={this.props.errorData}
          />
          {this.getBottomContent()}
        </div>
      </div>
    );
  }
}

export default connect((state, { location }) => {
  // RBAC needs the permissions sent to the Acceleration components and passed down, in case any component along the way needs to be able to alter reflections
  const permissions = state.resources.entities.get('datasetUI')
  && state.resources.entities.get('datasetUI').first()
  && state.resources.entities.get('datasetUI').first().get('permissions');
  return ({
    canSelect: permissions && permissions.get('canSelect'),
    entityId: getDatasetEntityId(state, location),
    exploreViewState: getExploreViewState(state)
  });
}, {
  runDatasetSql,
  previewDatasetSql
})(ExplorePageContentWrapper);

const styles = {
  tableControlWrap: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    background: WHITE,
    overflowY: 'hidden'
  },
  tableViewerStyleShort: {
    width: '100%'
  },
  tableViewerStyleFull: {
    width: `calc(100% - ${HISTORY_PANEL_SIZE}px)`,
    flexShrink: 0
  }
};
