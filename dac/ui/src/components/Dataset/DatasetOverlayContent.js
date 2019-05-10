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
import { connect }   from 'react-redux';
import Radium from 'radium';
import Immutable from 'immutable';
import { Link } from 'react-router';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { loadSummaryDataset } from 'actions/resources/dataset';
import { getViewState } from 'selectors/resources';
import { getSummaryDataset } from 'selectors/datasets';
import { stopPropagation } from '@app/utils/reactEventUtils';

import ViewStateWrapper from 'components/ViewStateWrapper';
import ColumnMenuItem from 'components/DragComponents/ColumnMenuItem';
import FontIcon from 'components/Icon/FontIcon';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';

import DatasetOverlayContentMixin from 'dyn-load/components/Dataset/DatasetOverlayContentMixin';

import { formDescription } from 'uiTheme/radium/typography';
import { CELL_EXPANSION_HEADER, WHITE } from 'uiTheme/radium/colors';
import { FLEX_COL_START, FLEX_NOWRAP_ROW_BETWEEN_CENTER } from 'uiTheme/radium/flexStyle';

import './DatasetOverlayContent.less';

const VIEW_ID = 'SummaryDataset';

@Radium
@pureRender
@DatasetOverlayContentMixin
export class DatasetOverlayContent extends Component {
  static propTypes = {
    fullPath: PropTypes.instanceOf(Immutable.List),
    summaryDataset: PropTypes.instanceOf(Immutable.Map),
    loadSummaryDataset: PropTypes.func,
    placement: PropTypes.string,
    onMouseEnter: PropTypes.func,
    onMouseLeave: PropTypes.func,
    toggleIsDragInProgress: PropTypes.func,
    onClose: PropTypes.func,
    showFullPath: PropTypes.bool,
    viewState: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,
    typeIcon: PropTypes.string.isRequired,
    dragType: PropTypes.string,
    iconStyles: PropTypes.object,
    onRef: PropTypes.func
  };

  static defaultProps = {
    iconStyles: {},
    dragType: ''
  };

  static contextTypes = {
    username: PropTypes.string,
    location: PropTypes.object
  };

  componentWillMount() {
    this.props.loadSummaryDataset(this.props.fullPath.join('/'), VIEW_ID);
  }

  renderHeader() {
    const { summaryDataset, showFullPath, typeIcon } = this.props;
    const name = summaryDataset.getIn(['fullPath', -1]);
    return (
      <div style={styles.header}>
        <div style={styles.breadCrumbs}>
          <DatasetItemLabel
            shouldShowOverlay={false}
            name={name}
            showFullPath={showFullPath}
            fullPath={summaryDataset.get('fullPath')}
            typeIcon={typeIcon}
          />
        </div>
        <div style={{display: 'flex', marginRight: 5}}>
          { /* disabled pending DX-6596 Edit link from DatasetOverlay is broken */ }
          {false && this.renderPencil(summaryDataset)}
          <Link to={summaryDataset.getIn(['links', 'query'])}><FontIcon type='Query'/></Link>
        </div>
      </div>
    );

  }

  renderColumn() {
    const { summaryDataset } = this.props;
    return summaryDataset.get('fields') && summaryDataset.get('fields').size
      ? <div style={{minHeight: 80, flexShrink: 1}}>
        <span style={styles.attributeLabel}>{la('Fields')}:</span>
        <div style={{marginLeft: 10}}>
          { summaryDataset.get('fields').map( (item, i) => {
            return (
              <ColumnMenuItem
                key={i}
                item={item}
                dragType={this.props.dragType}
                handleDragStart={this.props.toggleIsDragInProgress}
                onDragEnd={this.props.toggleIsDragInProgress}
                index={i}
                fullPath={this.props.fullPath}
                preventDrag={!this.props.dragType}
                nativeDragData={{
                  type: 'columnName',
                  data: {
                    name: item.get('name')
                  }
                }}/>
            );
          })}
        </div>
      </div>
      : null;
  }

  render() {
    const { summaryDataset, onMouseEnter, onMouseLeave, placement, viewState, onClose, onRef } = this.props;
    const position = placement === 'right' ? placement : '';

    return (
      <div
        ref={onRef}
        style={[styles.base, this.props.style]}
        className={`dataset-label-overlay ${position}`}
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
        onClick={stopPropagation}
      >
        <ViewStateWrapper viewState={viewState} onDismissError={onClose}>
          { summaryDataset.size > 0 &&
            <div>
              { this.renderHeader() }
              <div style={styles.attributesWrap}>
                <div style={styles.attribute}>
                  <div style={styles.attributeLabel}>{la('Jobs')}:</div>
                  <div style={styles.attributeValue}>
                    <Link to={summaryDataset.getIn(['links', 'jobs'])} onMouseDown={stopPropagation}>
                      {summaryDataset.get('jobCount') || 0} »
                    </Link>
                  </div>
                </div>
                <div style={styles.attribute}>
                  <div style={styles.attributeLabel}>{la('Descendants')}:</div>
                  <div style={styles.attributeValue}>{summaryDataset.get('descendants') || 0}</div>
                </div>
                {this.renderColumn()}
              </div>
            </div>
          }
        </ViewStateWrapper>
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const fullPath = props.fullPath.join(',');
  return {
    summaryDataset: getSummaryDataset(state, fullPath),
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, { loadSummaryDataset })(DatasetOverlayContent);

const styles = {
  base: {
    height: 100,
    marginTop: -1,
    paddingLeft: 5,
    paddingRight: 5,
    position: 'absolute',
    width: 220,
    zIndex: 32000,
    display: 'flex',
    flexDirection: 'column'
  },
  header: {
    padding: 5,
    height: 45,
    width: 210,
    borderRight: '1px solid #E9E9E9',
    borderTop: '1px solid #E9E9E9',
    borderLeft: '1px solid #E9E9E9',
    borderRadius: '2px 2px 0 0',
    backgroundColor: CELL_EXPANSION_HEADER,
    ...FLEX_NOWRAP_ROW_BETWEEN_CENTER
  },
  attributesWrap: {
    padding: '5px 10px',
    backgroundColor: WHITE,
    width: 210,
    minWidth: 200,
    maxWidth: 550,
    maxHeight: 400,
    overflowY: 'auto',
    borderRadius: '0 0 2px 2px',
    borderRight: '1px solid #E9E9E9',
    borderBottom: '1px solid #E9E9E9',
    borderLeft: '1px solid #E9E9E9',
    boxShadow: '0 0 5px 0 rgba(0, 0, 0, 0.05)',
    ...FLEX_COL_START
  },
  attribute: {
    flexShrink: 0,
    display: 'flex',
    marginBottom: 4
  },
  attributeLabel: {
    ...formDescription,
    width: 90
  },
  breadCrumbs: {
    overflow: 'auto',
    display: 'flex',
    flexDirection: 'row',
    justifyContent: 'flex-start',
    alignItems: 'center'
  }
};
