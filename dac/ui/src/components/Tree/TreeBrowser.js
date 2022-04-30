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
import { useState, useMemo, useEffect } from 'react';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';

import { SinglePageTypeButton } from '@app/pages/ExplorePage/components/PageTypeButtons';
import { getLocation } from '@app/selectors/routing';
import SearchDatasetsPopover from '../DatasetList/SearchDatasetsPopover';
import SQLScripts from '../SQLScripts/SQLScripts';
import Art from '../Art';
import TreeNode from './TreeNode';

import './TreeBrowser.less';

const TABS = {
  Data: 'Data',
  Scripts: 'Scripts'
};

const TreeBrowser = (props) => {
  const { location,
    sidebarCollapsed,
    isCollapsable,
    intl: { formatMessage }
  } = props;
  const [ selectedTab, setSelectedTab ] = useState(TABS.Data);
  const [ collapaseText, setCollapseText ] = useState();

  useEffect(() => {
    if (location.state && location.state.renderScriptTab) {
      setSelectedTab(TABS.Scripts);
    }
  }, [location, setSelectedTab]);

  useEffect(() => {
    if (!props.isSqlEditorTab) {
      setSelectedTab(TABS.Data);
    }
  }, [props.isSqlEditorTab, setSelectedTab]);

  useEffect(() => {
    setCollapseText(sidebarCollapsed ? formatMessage({ id: 'Explore.Left.Panel.Collapse.Text.Close' }) : formatMessage({ id: 'Explore.Left.Panel.Collapse.Text.Open' }));
  }, [sidebarCollapsed]);

  const [homeSource, sortedTree] = useMemo(() => {
    const tempHomeSource = Array.from(props.resourceTree)[0];
    // Remove home item for new copied list
    const tempOtherSources = Array.from(props.resourceTree).splice(1);
    const tempSortedTree = tempOtherSources.sort((a, b) => {
      const y = a.get('name').toLowerCase();
      const z = b.get('name').toLowerCase();

      if (y > z) return 1;
      if (y < z) return -1;
      return 0;
    });

    return [tempHomeSource, tempSortedTree];
  }, [props.resourceTree]);

  const renderHome = () => {
    if (homeSource) {
      return <TreeNode
        node={homeSource}
        key={0}
        renderNode={props.renderNode}
        isNodeExpanded={props.isNodeExpanded}
        selectedNodeId={props.selectedNodeId}
      />;
    }
  };

  const renderItems = () => {
    if (sortedTree.length > 0) {
      return sortedTree.map((node, index) => {
        return <TreeNode
          node={node}
          key={index}
          renderNode={props.renderNode}
          isNodeExpanded={props.isNodeExpanded}
          selectedNodeId={props.selectedNodeId}
        />;

      });
    }
  };

  const renderTabs = () => {
    return props.isSqlEditorTab ? (
      <>
        <SinglePageTypeButton
          classname={`TreeBrowser-tab ${TABS.Data === selectedTab ? '--active' : ''}`}
          text='Data' isSelected={TABS.Data === selectedTab}
          onClick={() => setSelectedTab(TABS.Data)}
        />
        <SinglePageTypeButton
          classname={`TreeBrowser-tab ${TABS.Scripts === selectedTab ? '--active' : ''}`}
          text='Scripts' isSelected={TABS.Scripts === selectedTab}
          onClick={() => setSelectedTab(TABS.Scripts)}
        />
      </>
    ) : (<div className='TreeBrowser-tab'>Data</div>);
  };

  const renderTabsContent = () => {
    if (selectedTab === TABS.Data) {
      return (
        <>
          <SearchDatasetsPopover
            changeSelectedNode={() => {}}
            dragType={props.dragType}
            addtoEditor={props.addtoEditor}
            shouldAllowAdd
          />
          <div className='TreeBrowser-items'>
            {renderHome()}
            {renderItems()}
          </div>
        </>
      );
    } else if (selectedTab === TABS.Scripts) {
      return <SQLScripts />;
    }
  };

  return (
    <div className='TreeBrowser'>
      <div className={`TreeBrowser-heading ${!props.isSqlEditorTab ? '--dataset' : ''}`}>
        {renderTabs()}

        { isCollapsable &&
          <Art src='CollapseLeft.svg'
            alt={formatMessage({ id: 'Explore.Left.Panel.Collapse.Alt' })}
            title={collapaseText}
            className='TreeBrowser__collapseButton'
            onClick={props.handleSidebarCollapse} />
        }

      </div>
      {renderTabsContent()}
    </div>
  );
};

TreeBrowser.propTypes = {
  resourceTree: PropTypes.instanceOf(Immutable.List),
  sources: PropTypes.instanceOf(Immutable.List),
  renderNode: PropTypes.func,
  isNodeExpanded: PropTypes.func,
  selectedNodeId: PropTypes.string,
  addtoEditor: PropTypes.func,
  dragType: PropTypes.string,
  isSqlEditorTab: PropTypes.bool,
  location: PropTypes.object,
  handleSidebarCollapse: PropTypes.func,
  sidebarCollapsed: PropTypes.bool,
  isCollapsable: PropTypes.bool,
  intl: PropTypes.object.isRequired
};

TreeBrowser.defaultProps = {
  resourceTree: Immutable.List(),
  sources: Immutable.List()
};

TreeBrowser.contextTypes = {
  loggedInUser: PropTypes.object
};


const mapStateToProps = (state) => ({
  location: getLocation(state)
});

export default injectIntl(connect(mapStateToProps, null)(TreeBrowser));
