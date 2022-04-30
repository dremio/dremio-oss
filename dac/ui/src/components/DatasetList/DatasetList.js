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
import Radium from 'radium';

import PropTypes from 'prop-types';

import Spinner from 'components/Spinner';
import DragSource from 'components/DragComponents/DragSource';
import DatasetItemLabel from 'components/Dataset/DatasetItemLabel';
import { constructFullPath } from 'utils/pathUtils';
import { bodySmall } from 'uiTheme/radium/typography';
import { getIconDataTypeFromDatasetType } from 'utils/iconUtils';

@Radium
export default class DatasetList extends PureComponent {

  static propTypes = {
    data: PropTypes.instanceOf(Immutable.List).isRequired,
    changeSelectedNode: PropTypes.func.isRequired,
    isInProgress: PropTypes.bool.isRequired,
    inputValue: PropTypes.string,
    dragType: PropTypes.string,
    showParents: PropTypes.bool,
    shouldAllowAdd: PropTypes.bool,
    addtoEditor: PropTypes.func
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
    const { shouldAllowAdd, addtoEditor } = this.props;
    return data && data.map && data.map((value, key) => {
      const name = value.get('fullPath').get(value.get('fullPath').size - 1);
      const displayFullPath = value.get('displayFullPath') || value.get('fullPath');

      return (
        <DragSource dragType={this.props.dragType || ''} key={key} id={displayFullPath}>
          <div
            key={key} style={[styles.datasetItem, bodySmall]}
            className='dataset-item'
            onClick={this.setActiveDataset.bind(this, value)}>

            <DatasetItemLabel
              dragType={this.props.dragType}
              name={name}
              showFullPath
              inputValue={inputValue}
              fullPath={value.get('fullPath')}
              typeIcon={getIconDataTypeFromDatasetType(value.get('datasetType'))}
              placement='right'
              isExpandable
              shouldShowOverlay
              shouldAllowAdd={shouldAllowAdd}
              addtoEditor={addtoEditor}
              displayFullPath={displayFullPath} />

          </div>
        </DragSource>
      );
    });
  }

  resetSelectedData() {
    this.setState({ activeDataset: '' });
    this.props.changeSelectedNode(null);
  }

  render() {
    const { data, inputValue, isInProgress } = this.props;
    const searchBlock = data && data.size && data.size > 0
      ? this.getDatasetsList(data, inputValue)
      : <div style={styles.notFound}>{la('No results found')}</div>;
    return (
      <div style={styles.dataSetsList} className='datasets-list'>
        {isInProgress ? <Spinner /> : searchBlock}
      </div>
    );
  }
}

const styles = {
  dataSetsList: {
    background: '#fff',
    maxHeight: '50vh',
    overflow: 'auto',
    boxShadow: 'rgb(0 0 0 / 10%) 0px 0px 8px 0px',
    borderRadius: 5,
    padding: 10,
    minWidth: 480
  },
  datasetItem: {
    borderBottom: '1px solid #E9EDF0',
    width: '100%',
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(0,1fr))',
    cursor: 'pointer',
    padding: '10px 0',
    ':hover': {
      background: '#F1FAFB'
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
