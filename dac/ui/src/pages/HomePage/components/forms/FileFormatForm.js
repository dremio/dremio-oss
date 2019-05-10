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
import { propTypes as reduxFormPropTypes } from 'redux-form';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';
import deepEqual from 'deep-equal';
import { injectIntl } from 'react-intl';
import { debounce } from 'lodash/function';

import { ModalForm, FormBody, modalFormProps } from 'components/Forms';
import { Select } from 'components/Fields';
import ViewStateWrapper from 'components/ViewStateWrapper';
import ExploreTableController from 'pages/ExplorePage/components/ExploreTable/ExploreTableController';
import prefixSection from 'components/Forms/prefixSection';

import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { label, divider } from 'uiTheme/radium/forms';
import { PALE_GREY } from 'uiTheme/radium/colors';
import { ExcelFormatForm, TextFormatForm, XLSFormatForm } from './FormatForms';

function validate(values, props) {
  const { intl } = props;
  const errors = {};
  const curType = values.type && values[values.type];
  if (curType) {
    errors[values.type] = {};
    for (const key in curType) {
      // "sheetName" field on XLS form can be empty
      // https://dremio.atlassian.net/browse/DX-7497
      if (!curType[key] && curType[key] !== false && key !== 'sheetName') {
        errors[values.type][key] = intl.formatMessage({ id: 'Error.NotEmptyField' });
      }
    }
  }
  return errors;
}

const typeToForm = {
  Text: TextFormatForm,
  Excel: ExcelFormatForm,
  XLS: XLSFormatForm
};

const FIELDS = ['type', 'version', 'location'];
const SECTIONS = Object.keys(typeToForm).map((key) => prefixSection(key)(typeToForm[key]));
const DEBOUNCE_DELAY = 100;

const typeToInitialValues = {
  Text: {
    fieldDelimiter: ',',
    quote: '"',
    comment: '#',
    lineDelimiter: '\r\n',
    escape: '"',
    trimHeader: true,
    extractHeader: false,
    skipFirstLine: false
  },
  Excel: {
    extractHeader: false,
    hasMergedCells: false,
    sheetName: 'sheet name'
  },
  XLS: {
    extractHeader: false,
    hasMergedCells: false,
    sheetName: 'sheet name'
  }
};

@Radium
export class FileFormatForm extends Component {

  static propTypes = {
    ...reduxFormPropTypes,
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    onPreview: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map),
    previewViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    previewData: PropTypes.instanceOf(Immutable.Map),
    updateFormDirtyState: PropTypes.func,
    cancelText: PropTypes.string,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.onPreview = debounce(props.onPreview, DEBOUNCE_DELAY);
  }

  componentDidMount() {
    if (this.props.values.type && this.props.values.type !== 'Unknown' && this.props.valid
      && (!this.props.viewState || !this.props.viewState.get('isInProgress'))) {
      this.onPreview(this.mapFormatValues(this.props.values));
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.values.type && nextProps.values.type !== 'Unknown' && nextProps.valid
        && !deepEqual(nextProps.values, this.props.values)
        && (!this.props.viewState || !this.props.viewState.get('isInProgress'))) {
      this.onPreview(this.mapFormatValues(nextProps.values));
    }
  }

  componentWillUnmount() {
    this.onPreview.cancel();
  }

  onSubmit = (values) => {
    const {onFormSubmit} = this.props;
    return onFormSubmit(this.mapFormatValues(values));
  }

  getTableHeight(node) {
    const customWrapper = $(node).parents('.modal-form-wrapper')[0];
    return $(customWrapper).height() - $(customWrapper).children()[1].offsetTop;
  }

  mapFormatValues(values) {
    return {...values[values.type], type: values.type, location: values.location, version: values.version};
  }

  renderFormatSection() {
    const {fields} = this.props;

    if (fields.type) {
      switch (fields.type.value) {
      case 'Text':
        return <TextFormatForm {...this.props} fields={fields}/>;
      case 'Excel':
        return <ExcelFormatForm {...this.props} fields={fields}/>;
      case 'XLS':
        return <XLSFormatForm {...this.props} fields={fields}/>;
      default:
        return undefined;
      }
    }
  }

  render() {
    const {fields, handleSubmit, onCancel, viewState, previewData, previewViewState, cancelText, intl} = this.props;
    const line = fields.type.value === 'Text' ? <hr style={divider}/> : null;

    const formatOptions = [
      {option: 'Unknown', label: intl.formatMessage({ id: 'File.Unknown' })},
      {option: 'Text', label: intl.formatMessage({ id: 'File.TextDelimited' })},
      {option: 'JSON', label: intl.formatMessage({ id: 'File.JSON' })},
      {option: 'Parquet',  label: intl.formatMessage({ id: 'File.Parquet' })},
      {option: 'Excel',  label: intl.formatMessage({ id: 'File.Excel' })},
      {option: 'XLS', label: intl.formatMessage({ id: 'File.XLS' })}
    ];

    return (
      <ModalForm
        {...modalFormProps(this.props)}
        formBodyStyle={styles.formBodyStyle}
        confirmStyle={styles.confirmStyle}
        wrapperStyle={styles.formWrapper}
        confirmText={intl.formatMessage({ id: 'Common.Save' })}
        style={{ width: '100%' }}
        cancelText={cancelText || intl.formatMessage({ id: 'Common.Cancel' })}
        onSubmit={handleSubmit(this.onSubmit)}
        onCancel={onCancel}>
        <FormBody style={styles.formBody} dataQa='file-format-form'>
          <ViewStateWrapper
            viewState={viewState}
            hideSpinner
          >
            <div>
              <label style={[label]}>{intl.formatMessage({ id: 'File.Format' })}</label>
              <Select
                {...fields.type}
                dataQa='fileFormat'
                style={styles.formatMenu}
                items={formatOptions}
              />
            </div>
            {line}
            {this.renderFormatSection()}
          </ViewStateWrapper>
        </FormBody>
        <ViewStateWrapper
          viewState={previewViewState}
          spinnerStyle={{height: 'calc(100% - 48px)', paddingBottom: 0}}
          spinnerDelay={0}
          style={{ display: 'flex'}}
          dataQa='file-preview-mask'
        >
          <div className='table-parent file-preview-table' style={styles.previewTable}>
            {previewData.get('rows') && <ExploreTableController
              isDumbTable
              isResizeInProgress
              getTableHeight={this.getTableHeight}
              tableData={previewData}
              location={this.context.location}
              dragType='groupBy'
              exploreViewState={previewViewState}
              shouldRenderInvisibles/>} {/*EXPERIMENTAL: rendering invisibles here should be safe because we've disabled most other cell features*/}
          </div>
        </ViewStateWrapper>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, props) {
  const {file} = props;

  const fileFormat = file && file.get('fileFormat');

  let fromExisting = {};
  if (fileFormat) {
    const {type, location, ...values} = fileFormat.toJS();
    fromExisting = {type, location, [type]: values};
  }

  const initialValues = {
    type: 'Unknown',
    ...typeToInitialValues,
    ...(fileFormat && fileFormat.toJS() || {}),
    ...fromExisting
  };

  return {
    initialValues,
    previewData: state.modals.addFileModal.get('preview')
  };
}

export default injectIntl(connectComplexForm(
  {
    form: 'fileFormatForm',
    fields: FIELDS,
    validate
  },
  SECTIONS,
  mapStateToProps
)(FileFormatForm));


const styles = {
  formWrapper: {
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'column'
  },
  formBody: {
    flexShrink: 0,
    paddingBottom: 20,
    background: PALE_GREY
  },
  formBodyStyle: {
    height: 'calc(100% - 48px)',
    paddingBottom: 0
  },
  formatMenu: {
    height: 28,
    margin: 0
  },
  previewTable: {
    maxWidth: '98%',
    position: 'relative',
    left: '1%',
    display: 'flex',
    flexGrow: 1
  },
  confirmStyle: {
    position: 'relative'
  },
  typeColumn: {
    'Icon': {
      height: 18,
      width: 24,
      marginLeft: -2,
      marginTop: 5,
      cursor: 'auto'
    }
  }
};
