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
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import Spinner from 'components/Spinner';
import DragSource from 'components/DragComponents/DragSource';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import { constructFullPath } from 'utils/pathUtils';
import { bodySmall } from 'uiTheme/radium/typography';
import { PALE_ORANGE } from 'uiTheme/radium/colors';
import { getIconDataTypeFromDatasetType } from 'utils/iconUtils';

@Radium
@pureRender
export default class DatasetList extends Component {

  static propTypes = {
    data: PropTypes.instanceOf(Immutable.List).isRequired,
    changeSelectedNode: PropTypes.func.isRequired,
    isInProgress: PropTypes.bool.isRequired,
    inputValue: PropTypes.string,
    style: PropTypes.object,
    dragType: PropTypes.string,
    showParents: PropTypes.bool,
    showAddIcon: PropTypes.bool,
    addFullPathToSqlEditor: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.state = {
      activeDataset: ''
    };
  }

  componentWillReceiveProps(newProps) {
    if (newProps.data !== this.props.data) {
      this.resetSelectedData();
    }
  }

  setActiveDataset(node) {
    const fullPath = constructFullPath(node.get('fullPath'));
    this.setState({ activeDataset: fullPath });
    this.props.changeSelectedNode(fullPath, node);
  }

  getDatasetsList(data, inputValue) {
    const { showAddIcon, addFullPathToSqlEditor } = this.props;
    return data && data.map && data.map((value, key) => {
      const fullPath = constructFullPath(value.get('fullPath'));
      const isActive = this.state.activeDataset === fullPath ? {
        background: PALE_ORANGE
      } : {};
      const name = value.get('fullPath').get(value.get('fullPath').size - 1);
      const displayFullPath = value.get('displayFullPath') || value.get('fullPath');
      const parentItems = this.getParentItems(value, inputValue);

      return (
        <DragSource dragType={this.props.dragType || ''} key={key} id={displayFullPath}>
          <div
            key={key} style={[styles.datasetItem, bodySmall, isActive]}
            className='dataset-item'
            onClick={this.setActiveDataset.bind(this, value)}>
            {
              showAddIcon
              && <FontIcon
                style={{ height: 24 - 2 }} // fudge factor makes it look v-aligned better
                type='Add'
                hoverType='AddHover'
                theme={styles.addIcon}
                onClick={addFullPathToSqlEditor.bind(this, displayFullPath)}/>
            }
            <div style={{
              minWidth: parentItems ? 200 : null,
              width: parentItems ? '30%' : null,
              marginLeft: 6,
              flexShrink: parentItems ? 0 : 1,
              overflow: 'hidden' // make sure sub-element ellipsis happens
            }}>
              <DatasetItemLabel
                dragType={this.props.dragType}
                name={name}
                showFullPath
                inputValue={inputValue}
                fullPath={value.get('fullPath')}
                typeIcon={getIconDataTypeFromDatasetType(value.get('datasetType'))}
                placement='right'/>
            </div>
            {parentItems && (
              <div style={styles.parentDatasetsHolder}>
                {parentItems}
              </div>
            )}
          </div>
        </DragSource>
      );
    });
  }

  // always returns null if it has nothing to show
  getParentItems(dataset, inputValue) {
    if (!this.props.showParents) return null;
    const parents = dataset && dataset.get && dataset.get('parents');
    if (!parents) return null;
    // todo: chris: is this right? `value.get('type')` here, but `value.get('datasetType')` above?
    // denis b: Yes, this is correct. `type` attribute belongs to `parents` item from `/search` response.
    // chris: we should fix that API
    const parentItems = parents.map((value, key) => {
      if (!value.has('type')) return; // https://dremio.atlassian.net/browse/DX-7233
      return (
        <div style={styles.parentDataset}>
          <DatasetItemLabel
            name={value.get('datasetPathList').last()}
            inputValue={inputValue}
            fullPath={value.get('datasetPathList')}
            typeIcon={getIconDataTypeFromDatasetType(value.get('type'))}
            showFullPath />
        </div>
      );
    }).toJS().filter(Boolean);


    if (!parentItems.length) return null;
    return parentItems;
  }

  resetSelectedData() {
    this.setState({ activeDataset: '' });
    this.props.changeSelectedNode(null);
  }

  render() {
    const { data, inputValue, style, isInProgress } = this.props;
    const searchBlock = data && data.size && data.size > 0
      ? this.getDatasetsList(data, inputValue)
      : <div style={styles.notFound}>{la('Not found')}</div>;
    return (
      <div style={style} className='datasets-list'>
        {isInProgress ? <Spinner /> : searchBlock}
      </div>
    );
  }
}

const styles = {
  datasetItem: {
    borderBottom: '1px solid rgba(0,0,0,.1)',
    height: 45,
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer',
    ':hover': {
      background: PALE_ORANGE
    }
  },
  datasetData: {
    margin: '0 0 0 5px',
    minWidth: 300
  },
  parentDatasetsHolder: {
    display: 'flex',
    flex: '1 1 auto',
    overflow: 'hidden' // don't scroll - avoid windows scroll bars
  },
  parentDataset: {
    display: 'flex',
    margin: '0 10px 0 10px'
  },
  notFound: {
    padding: '10px 10px'
  },
  addIcon: {
    Container: {
      marginRight: 0,
      marginLeft: 10
    }
  }
};
