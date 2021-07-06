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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import mergeWith from 'lodash/mergeWith';

import { createSource, loadSource, removeSource, updateSourcePrivileges } from 'actions/resources/sources';
import sourcesMapper from 'utils/mappers/sourcesMapper';
import ApiUtils from 'utils/apiUtils/apiUtils';
import FormUtils from 'utils/FormUtils/FormUtils';
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import { showConfirmationDialog } from 'actions/confirmation';
import { passDataBetweenTabs } from 'actions/modals/passDataBetweenTabs.js';
import ViewStateWrapper from 'components/ViewStateWrapper';
import Message from 'components/Message';
import ConfigurableSourceForm from 'pages/HomePage/components/modals/ConfigurableSourceForm';

import EditSourceViewMixin, {
  mapStateToProps,
  additionalMapDispatchToProps,
  getFinalSubmit
} from '@inject/pages/HomePage/components/modals/EditSourceViewMixin';
import { isExternalSourceType } from '@app/constants/sourceTypes';

import { viewStateWrapper } from 'uiTheme/less/forms.less';

export const VIEW_ID = 'EditSourceView';


export const processUiConfig = (uiConfig) => {
  if (!uiConfig || !uiConfig.elements) return uiConfig;

  return {
    ...uiConfig,
    elements: uiConfig.elements.map(el => ({
      ...el,
      propertyName: FormUtils.addFormPrefixToPropName(el.propertyName)
    }))
  };
};

@EditSourceViewMixin
export class EditSourceView extends PureComponent {
  static propTypes = {
    sourceName: PropTypes.string.isRequired,
    sourceType: PropTypes.string.isRequired,
    hide: PropTypes.func.isRequired,
    createSource: PropTypes.func.isRequired,
    updateSourcePrivileges: PropTypes.func,
    removeSource: PropTypes.func.isRequired,
    loadSource: PropTypes.func,
    messages: PropTypes.instanceOf(Immutable.List),
    viewState: PropTypes.instanceOf(Immutable.Map),
    source: PropTypes.instanceOf(Immutable.Map),
    initialFormValues: PropTypes.object,
    updateFormDirtyState: PropTypes.func,
    showConfirmationDialog: PropTypes.func,
    passDataBetweenTabs: PropTypes.func
  };

  constructor() {
    super();
    this.state = {
      isConfigLoaded: false,
      selectedFormType: {},
      isFileSystemSource: false
    };
  }

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  state = {
    didLoadFail: false,
    errorMessage: 'Failed to load source configuration.'
  };

  componentDidMount() {
    const { sourceName, sourceType } = this.props;
    this.props.loadSource(sourceName, VIEW_ID);
    this.fetchData();
    this.setStateWithSourceTypeConfigFromServer(sourceType);
  }

  setStateWithSourceTypeConfigFromServer(typeCode) {
    ApiUtils.fetchJson(`source/type/${typeCode}`, json => {
      const combinedConfig = SourceFormJsonPolicy.getCombinedConfig(typeCode, processUiConfig(json));
      const isFileSystemSource = combinedConfig.metadataRefresh;
      this.setState({isTypeSelected: true, isConfigLoaded: true, selectedFormType: combinedConfig, isFileSystemSource: isFileSystemSource.isFileSystemSource, isExternalQueryAllowed: json.externalQueryAllowed});
    }, () => {
      this.setState({didLoadFail: true});
    })
      .finally(() => {
        this.props.passDataBetweenTabs({isFileSystemSource: this.state.isFileSystemSource, isExternalQueryAllowed: this.state.isExternalQueryAllowed});
      });
  }

  reallySubmitEdit = (form, sourceType) => {
    const url = isExternalSourceType(sourceType) ? '/sources/external/list' : '/sources/datalake/list';
    return ApiUtils.attachFormSubmitHandlers(
      getFinalSubmit(form, sourceType, this.props)
    ).then(() => {
      this.context.router.replace(url);
    });
  };

  checkIsMetadataImpacting = (sourceModel) => {
    return ApiUtils.fetch('sources/isMetadataImpacting', {
      method: 'POST',
      body: JSON.stringify(sourceModel)
    }, 2)
      .then((response) => response.json())
      .catch((response) => {
        return response.json().then((error) => {
          throw error;
        });
      });
  };

  submitEdit = (form) => {
    const { sourceType } = this.props;

    const formData = this.mutateFormValues(form);

    return ApiUtils.attachFormSubmitHandlers(new Promise((resolve, reject) => {
      const sourceModel = sourcesMapper.newSource(sourceType, formData);
      this.checkIsMetadataImpacting(sourceModel)
        .then((data) => {
          if (data && data.isMetadataImpacting) {
            this.props.showConfirmationDialog({
              title: la('Warning'),
              text: la('You made a metadata impacting change.  This change will cause Dremio to clear permissions, formats and reflections on all datasets in this source.'),
              confirmText: la('Confirm'),
              dataQa: 'metadata-impacting',
              confirm: () => {
                this.reallySubmitEdit(formData, sourceType).then(resolve).catch(reject);
              },
              cancel: reject
            });
          } else {
            this.reallySubmitEdit(formData, sourceType).then(resolve).catch(reject);
          }
        }).catch(reject);
    }));
  };

  renderMessages() {
    const { messages } = this.props;
    return messages && messages.map((message, i) => {
      const type = message.get('level') === 'WARN' ? 'warning' : message.get('level').toLowerCase();
      return (
        <Message
          messageType={type}
          message={message.get('message')}
          messageId={i.toString()}
          key={i.toString()}
          style={{ width: '100%' }}
        />
      );
    });
  }

  render() {
    const {updateFormDirtyState, initialFormValues, source, sourceType, viewState, hide} = this.props;
    const initValues = mergeWith(source && source.size > 1 ? source.toJS() : {}, initialFormValues, arrayAsPrimitiveMerger);

    let vs = viewState;

    if (this.state.didLoadFail) {
      vs = new Immutable.fromJS({isFailed: true, error: {message: this.state.errorMessage}});
    }

    return <ViewStateWrapper viewState={vs} className={viewStateWrapper}>
      {this.renderMessages()}
      {this.state.isConfigLoaded &&
      <ConfigurableSourceForm sourceFormConfig={this.state.selectedFormType}
        ref='form'
        onFormSubmit={this.submitEdit}
        onCancel={hide}
        key={sourceType}
        editing
        updateFormDirtyState={updateFormDirtyState}
        fields={FormUtils.getFieldsFromConfig(this.state.selectedFormType)}
        validate={FormUtils.getValidationsFromConfig(this.state.selectedFormType)}
        initialValues={initValues}
        permissions={source.get('permissions')}
        EntityType='source'
      />}
    </ViewStateWrapper>;
  }
}


export default connect(mapStateToProps, {
  loadSource,
  createSource,
  updateSourcePrivileges,
  removeSource,
  showConfirmationDialog,
  passDataBetweenTabs,
  ...additionalMapDispatchToProps
})(EditSourceView);

function arrayAsPrimitiveMerger(objValue, srcValue) {
  if (Array.isArray(objValue)) {
    return srcValue;
  }
}

