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
import { Component } from 'react';
import { connect }   from 'react-redux';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import { createDataset, createDatasetFromExisting } from 'actions/explore/sqlActions';
import { getEntityByUrlPath } from 'selectors/resources';

import dataStoreUtils from 'utils/dataStoreUtils';
import { constructFullPath } from 'utils/pathUtils';

import AddDataset from './AddDataset';

//TODO change to real config
const DS_CONFIG = {
  'sql' : 'SELECT "address" AS "address", "age" AS "age", "user" AS "user" FROM dfs.tmp."dac-sample1.json"',
  'state' : {
    'parentDataset' : {
      'type' : 'Name',
      'name' : {
        'datasetPath' : 'dfs.tmp."dac-sample1.json"',
        'datasetVersion' : ''
      }
    },
    'columnsList' : [
      {
        'name' : 'address',
        'type' : 'string',
        'value' : {
          'type' : 'ColumnReference',
          'col' : {
            'name' : 'address'
          }
        }
      },
      {
        'name' : 'age',
        'type' : 'string',
        'value' : {
          'type' : 'ColumnReference',
          'col' : {
            'name' : 'age'
          }
        }
      },
      {
        'name' : 'user',
        'type' : 'string',
        'value' : {
          'type' : 'ColumnReference',
          'col' : {
            'name' : 'user'
          }
        }
      }
    ]
  }
};
// I think this component is unused and all files in AddDatasetModal directory
@pureRender
class AddDatasetController extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired,
    username: PropTypes.string
  };

  static propTypes = {
    hide: PropTypes.func.isRequired,
    pathname: PropTypes.string.isRequired,
    pathEntity: PropTypes.instanceOf(Immutable.Map),
    createDataset: PropTypes.func.isRequired,
    createDatasetFromExisting: PropTypes.func,
    routeParams: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.item = dataStoreUtils.getItemsForDataset();
    this.toogleDropdown = this.toogleDropdown.bind(this);
    this.hideDropdown = this.hideDropdown.bind(this);
    this.updateButtonText = this.updateButtonText.bind(this);
    this.createDataset = this.createDataset.bind(this);
    this.updateFileUploadState = this.updateFileUploadState.bind(this);
    this.changeSelectedNode = this.changeSelectedNode.bind(this);
    this.updateName = this.updateName.bind(this);
    this.createFromExisting = this.createFromExisting.bind(this);
    this.createNewDataset = this.createNewDataset.bind(this);
    this.creationCallBack = this.creationCallBack.bind(this);

    this.buttonsStates = [{
      name: 'Add',
      id: 'add'
    }, {
      name: 'Add and Query',
      id: 'addAndQuery'
    }, {
      name: 'Add and Add Another',
      id: 'addAndAnother'
    }];

    this.state = {
      activeId: 'existing',
      selectedNode: null,
      dropdownState: false,
      buttonText: this.buttonsStates[0],
      fileLoadingState: 0,
      name: ''
    };
  }


  changeSelectedNode(nodeId) {
    this.setState({ selectedNode : nodeId});
  }

  // TODO: Refactor
  createDataset(formNode, item) {
    const { name, fileLoadingState, activeId } = this.state;
    const actionHash = Immutable.Map({
      existing: this.createFromExisting,
      newdata: this.createNewDataset
    });
    if (fileLoadingState === 100 || name) {
      actionHash.get(activeId)(formNode, item);
    }
  }

  // TODO Refactor
  createFromExisting(formNode, type) {
    const { pathEntity } = this.props;
    const { username } = this.context;
    const { name, selectedNode } = this.state;
    const ds = { name };
    // TODO change logic
    // check if some node selected and it's dataset
    if (selectedNode) {
      const fullPathList = pathEntity ? pathEntity.get('fullPathList') : [`@${username}`];
      const path = constructFullPath(fullPathList.concat(name));
      this.props.createDatasetFromExisting(selectedNode, path, ds).then((data) => {
        this.creationCallBack(formNode, data, type);
      });

      this.setState({ fileLoadingState: 0 });
    }
  }

  //TODO Refactor
  createNewDataset(formNode) {
    const { pathname } = this.props;
    const { name } = this.state;
    DS_CONFIG.name = name;
    this.props.createDataset(pathname, name, DS_CONFIG).then((data) => {
      this.creationCallBack(formNode, data);
    });
    this.setState({ fileLoadingState: 0 });
  }

  //TODO Refactor
  creationCallBack(formNode, data, type) {
    const { routeParams } = this.props;
    if (type === 'add') {
      this.props.hide();
    } else if (type === 'addAndAnother') {
      this.reset(formNode);
    } else if (type === 'addAndQuery') {
      if (data && data.payload && data.payload.datasetConfig) {
        const { name } = data.payload.datasetConfig;
        const { spaceId } = routeParams;
        const { username } = this.context;
        const resources = routeParams.resources || 'space';
        const resourceName = spaceId || `@${username}`;
        this.context.router.replace({
          ...this.context.location,
          pathname: `/${resources}/${resourceName}/${name}`
        });
      } else {
        this.props.hide();
      }
    }
  }

  hideDropdown() {
    this.setState({
      dropdownState: false
    });
  }

  reset(form) {
    if (form && form.refs && form.refs.form && form.refs.form.reset) {
      form.refs.form.reset();
    }
    this.setState({name: '', activeId: 'existing'});
  }

  toogleDropdown() {
    this.setState({
      dropdownState: !this.state.dropdownState
    });
  }
  updateButtonText(state) {
    this.setState({
      buttonText: state
    });
    this.hideDropdown();
  }

  updateFileUploadState(state) {
    this.setState({
      fileLoadingState: state
    });
  }

  updateName(name) {
    this.setState({name});
  }

  render() {
    return (
      <AddDataset
        createDataset={this.createDataset}
        hide={this.props.hide}
        updateName={this.updateName}
        nameDataset={this.state.name}
        buttonsStates={this.buttonsStates}
        updateButtonText={this.updateButtonText}
        items={this.item.items}
        dropdownState={this.state.dropdownState}
        toogleDropdown={this.toogleDropdown}
        buttonText={this.state.buttonText}
        updateFileUploadState={this.updateFileUploadState}
        hideDropdown={this.hideDropdown}
        changeSelectedNode={this.changeSelectedNode}/>
    );
  }
}

function mapStateToProps(state, props) {
  return {
    pathEntity: getEntityByUrlPath(state, props.pathname)
  };
}

export default connect(mapStateToProps, {
  createDataset,
  createDatasetFromExisting
})(AddDatasetController);
