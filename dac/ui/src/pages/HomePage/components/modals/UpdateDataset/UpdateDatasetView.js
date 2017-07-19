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

import Button from 'components/Buttons/Button';
import ModalFooter from 'components/Modals/components/ModalFooter';
import ResourceTreeController from 'components/Tree/ResourceTreeController';
import Message from 'components/Message';
import { formLabel } from 'uiTheme/radium/typography';
import { formRow } from 'uiTheme/radium/forms';
import { FieldWithError, TextField } from 'components/Fields';
import DependantDatasetsWarning from 'components/Modals/components/DependantDatasetsWarning';
import { applyValidators, isRequired } from 'utils/validation';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
// TODO: Use Radium
import './UpdateDataset.less';

function validate(values) {
  return applyValidators(values, [isRequired('datasetName', 'Dataset name')]);
}

export class UpdateDatasetView extends Component {
  static propTypes = {
    path: PropTypes.string,
    buttons: PropTypes.arrayOf(
      PropTypes.shape({
        key: PropTypes.string.isRequired,
        name: PropTypes.string.isRequired,
        type: PropTypes.string.isRequired
      })
    ),
    dependentDatasets: PropTypes.array,
    hidePath: PropTypes.bool.isRequired,
    name: PropTypes.string.isRequired,
    handleSubmit: PropTypes.func,
    fields: PropTypes.object,
    submit: PropTypes.func,
    error: PropTypes.object
  };

  static defaultProps = {
    name: '',
    hidePath: false
  };

  constructor(props) {
    super(props);
    this.renderWarning = this.renderWarning.bind(this);
  }

  clickHandler(e) {
    e.stopPropagation();
  }

  renderWarning() {
    const { dependentDatasets } = this.props;

    if (dependentDatasets && dependentDatasets.length > 0) {
      return ( // todo: loc
        <DependantDatasetsWarning
          text={`Changing the name or the location of this dataset
              will disconnect ${dependentDatasets.length} dependent
              datasets. Make a copy to preserve these connections.`}
          dependantDatasets={dependentDatasets}
        />
      );
    }

    return null;
  }

  renderErrorMessage() {
    const { error } = this.props;
    return error && (
      <Message
        messageType='error'
        message={error.message}
        messageId={error.id}
        detailsStyle={{maxHeight: 100}}
      />
    );
  }

  render() {
    const { hidePath, fields, path, handleSubmit, submit } = this.props;
    const locationBlock = hidePath
      ? null
      : <div className='property location'>
        <label style={formLabel}>{la('Location')}</label>
        <ResourceTreeController
          hideSources
          hideDatasets
          onChange={fields.selectedEntity.onChange}
          />
      </div>;
    const selectedLocationBlock = path
      ? <div className='selected-location'>
        {la('Selected location')}: {path}
      </div>
      : null;
    const buttons = this.props.buttons.map((button, index) => {
      return <Button
        style={{marginLeft: 5}}
        className={button.className}
        onClick={handleSubmit(submit.bind(this, button.key))}
        text={button.name}
        type={button.type}
        key={`${index}_button`}/>;
    });
    return (
      <div className='update-dataset' onClick={this.clickHandler} style={{display: 'flex', flexDirection: 'column'}}>
        {this.renderErrorMessage()}
        {this.renderWarning()}
        <div className='update-dataset-content'>
          <div style={formRow}>
            <FieldWithError {...fields.datasetName} touched label='Name' errorPlacement='right'>
              <TextField
                {...fields.datasetName}
                name='name'
                touched
                initialFocus
                placeholder={la('Dataset name ...')}/>
            </FieldWithError>
          </div>
          {locationBlock}
          {selectedLocationBlock}
        </div>
        <ModalFooter styles={style.footerStyle}>{buttons}</ModalFooter>
      </div>
    );
  }
}
function mapStateToProps(state, props) {
  return {
    initialValues: { datasetName: props.name }
  };
}

export default connectComplexForm({
  form: 'updateDataset',
  fields: ['datasetName', 'selectedEntity'],
  validate
}, [], mapStateToProps)(UpdateDatasetView);

const style = {
  footerStyle: {
    position: 'absolute'
  }
};
