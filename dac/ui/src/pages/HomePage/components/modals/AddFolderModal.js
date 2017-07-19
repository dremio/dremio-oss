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
import { connect } from 'react-redux';
import Immutable from 'immutable';

import Modal from 'components/Modals/Modal';

import ApiUtils from 'utils/apiUtils/apiUtils';
import { addNewFolderForSpace } from 'actions/resources/spaceDetails';
import { getHomeContents } from 'selectors/datasets';
import { getViewState } from 'selectors/resources';
import { getEntityType } from 'utils/pathUtils';

import AddFolderForm from '../forms/AddFolderForm';
import './Modal.less';

const VIEW_ID = 'AddFolderModal';

export class AddFolderModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,

    //connected
    parentEntity: PropTypes.instanceOf(Immutable.Map),
    parentType: PropTypes.string,
    addNewFolderForSpace: PropTypes.func
  };

  static contextTypes = {
    username: PropTypes.string
  };

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
  }

  submit(values) {
    const {parentEntity, parentType} = this.props;
    return ApiUtils.attachFormSubmitHandlers(
      this.props.addNewFolderForSpace(parentEntity, parentType, values.name)
    ).then(() => this.props.hide());
  }

  render() {
    const { isOpen, hide } = this.props;
    return (
      <Modal
        size='small'
        title={la('Add Folder')}
        isOpen={isOpen}
        hide={hide}>
        <AddFolderForm onFormSubmit={this.submit} onCancel={hide}/>
      </Modal>
    );
  }
}

function mapStateToProps(state) {
  const parentType = getEntityType(location.pathname);

  return {
    parentEntity: getHomeContents(state, parentType, window.location.pathname) || Immutable.Map(),
    parentType,
    viewState: getViewState(state, VIEW_ID)
  };
}

export default connect(mapStateToProps, {addNewFolderForSpace})(AddFolderModal);
