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
import { injectIntl } from 'react-intl';

import Modal from 'components/Modals/Modal';
import { CANCEL, CUSTOM, NEXT } from 'components/Buttons/ButtonTypes';
import { moveDataSet, createDatasetFromExisting } from 'actions/explore/sqlActions';
import { renameSpaceDataset, loadSpaceData, loadDependentDatasets } from 'actions/resources/spaceDetails';
import { getDescendantsList } from 'selectors/resources';
import ApiUtils from 'utils/apiUtils/apiUtils';
import { constructFullPath, splitFullPath } from 'utils/pathUtils';

import UpdateDatasetView from './UpdateDatasetView';

@injectIntl
@pureRender
export class UpdateDataset extends Component {
  static propTypes = {
    routeParams: PropTypes.object,
    location: PropTypes.object,
    item: PropTypes.instanceOf(Immutable.Map),
    isOpen: PropTypes.bool.isRequired,
    hide: PropTypes.func.isRequired,
    loadSpaceData: PropTypes.func,
    query: PropTypes.object.isRequired,
    createDatasetFromExisting: PropTypes.func.isRequired,
    moveDataSet: PropTypes.func.isRequired,
    renameSpaceDataset: PropTypes.func.isRequired,
    loadDependentDatasets: PropTypes.func.isRequired,
    dependentDatasets: PropTypes.array,
    space: PropTypes.object,
    pathname: PropTypes.string,
    queryContext: PropTypes.instanceOf(Immutable.List),
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    const { intl } = props;
    this.config = {
      rename: () => ({
        title: intl.formatMessage({ id: 'Dataset.RenameDataset'}),
        hidePath: true,
        buttons: [
          { name: intl.formatMessage({ id: 'Common.Cancel' }), key: 'cancel', type: CANCEL },
          { name: intl.formatMessage({ id: 'Common.Rename' }), key: 'renameDataset', type: NEXT }
        ]
      }),
      move: (dependentDatasets) => {
        const hasDeps = dependentDatasets && dependentDatasets.length;
        const buttons = [
          { name: intl.formatMessage({ id: 'Common.Cancel' }), key: 'cancel', type: CANCEL },
          { name: intl.formatMessage({ id: 'Common.MakeCopy' }), key: 'copyDataset', type: hasDeps ? NEXT : CUSTOM },
          {
            name: intl.formatMessage({ id: hasDeps ? 'Common.MoveAnyway' : 'Common.Move' }),
            key: 'moveDataset',
            type: hasDeps ? CUSTOM : NEXT
          }
        ];
        return { title: intl.formatMessage({ id: 'Dataset.MoveDataset' }), buttons };
      }
    };
  }

  componentWillMount() {
    this.receiveProps(this.props, {});
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  getCurrentFullPath = (item = this.props.item) => item && item.get('fullPathList')

  getNewFullPath = (datasetName, selectedEntity) => splitFullPath(selectedEntity).concat(datasetName)

  receiveProps = (nextProps, oldProps) => {
    if (!oldProps.item && nextProps.item) {
      nextProps.loadDependentDatasets(nextProps.item.get('fullPathList'));
    }
  }

  copyDataset = ({ datasetName, selectedEntity }) => {
    const cPathFrom = this.getCurrentFullPath();
    const cPathTo = this.getNewFullPath(datasetName, selectedEntity);
    return this.props.createDatasetFromExisting(cPathFrom, cPathTo, { name: datasetName });
  }

  moveDataset = ({ datasetName, selectedEntity }) => {
    const pathFrom = this.getCurrentFullPath();
    const cPathTo = this.getNewFullPath(datasetName, selectedEntity);
    return this.props.moveDataSet(pathFrom, cPathTo);
  }

  renameDataset = ({ datasetName }) => {
    return this.props.renameSpaceDataset(this.props.item, datasetName);
  }

  submit = (keyAction, values) => {
    return ApiUtils.attachFormSubmitHandlers(
      this[keyAction](values)
    ).then((res) => {
      if (res && !res.error) {
        this.props.hide();
      }
    });
  }

  render() {
    const { mode } = this.props.query;
    const config = mode && this.config[mode](this.props.dependentDatasets);
    const fullPath = this.getCurrentFullPath();
    // initialPath should be the parent folder
    const initialPath = fullPath ? constructFullPath(fullPath.slice(0, -1)) : null;

    const datasetView = config
      ? <UpdateDatasetView
        hide={this.props.hide}
        initialPath={initialPath}
        name={this.props.query.name}
        buttons={config.buttons}
        hidePath={config.hidePath}
        dependentDatasets={this.props.dependentDatasets}
        submit={this.submit}/>
      : null;
    return (
      <Modal
        hide={this.props.hide}
        size='small'
        isOpen={this.props.isOpen}
        title={config ? config.title : ''}>
        {datasetView}
      </Modal>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    dependentDatasets: getDescendantsList(state),
    // todo: should not need to normalize due to location state & reload
    item: Immutable.fromJS(ownProps.item)
  };
};

export default connect(mapStateToProps, {
  renameSpaceDataset,
  loadSpaceData,
  createDatasetFromExisting,
  moveDataSet,
  loadDependentDatasets
})(UpdateDataset);
