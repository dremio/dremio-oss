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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { connect } from 'react-redux';
import { injectIntl } from 'react-intl';


import { getEntity, getViewState } from 'selectors/resources';
import Modal from 'components/Modals/Modal';
import FormUnsavedWarningHOC from 'components/Modals/FormUnsavedWarningHOC';

import ApiUtils from 'utils/apiUtils/apiUtils';
import { createNewSpace, updateSpace } from 'actions/resources/spaces';

import SpaceForm from '../forms/SpaceForm';
import './Modal.less';

export const VIEW_ID = 'SpaceModal';

const mapStateToProps = (state, props) => {
  const entity = getEntity(state, props.entityId, 'space');
  return {
    entity,
    viewState: getViewState(state, VIEW_ID)
  };
};

@injectIntl
export class SpaceModal extends Component {

  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    entityId: PropTypes.string,

    //connected
    entity: PropTypes.instanceOf(Immutable.Map),
    createNewSpace: PropTypes.func,
    updateSpace: PropTypes.func,
    initialFormValues: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    intl: PropTypes.object.isRequired
  }

  static contextTypes = {
    username: PropTypes.string
  }

  hide = () => {
    this.props.hide();
  }

  submit = (values) => {
    return ApiUtils.attachFormSubmitHandlers(
      this.props.entity ? this.props.updateSpace(values) : this.props.createNewSpace(values)
    ).then(() => this.props.hide(null, true));
  }

  renderForm() {
    const { entity, initialFormValues, updateFormDirtyState } = this.props;

    return <SpaceForm
      initialValues={entity ? {
        name: entity.get('name'),
        version: entity.get('version'),
        id: entity.get('id'),
        ...initialFormValues
      } : null}
      updateFormDirtyState={updateFormDirtyState}
      editing={entity !== undefined}
      onFormSubmit={this.submit}
      onCancel={this.hide}
    />;
  }

  render() {
    const { isOpen, entity, intl } = this.props;
    return (
      <Modal
        size='small'
        title={entity ? intl.formatMessage({ id: 'Space.EditSpace' }) : intl.formatMessage({ id: 'Space.AddSpace' })}
        isOpen={isOpen}
        hide={this.hide}>
        {this.renderForm()}
      </Modal>
    );
  }
}

export default connect(mapStateToProps, {
  createNewSpace,
  updateSpace
})(FormUnsavedWarningHOC(SpaceModal));
