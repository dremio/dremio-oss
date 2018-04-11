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
import PropTypes from 'prop-types';
import { Link } from 'react-router';

import Tabs from 'components/Tabs';

import { LIST, MAP } from 'constants/DataTypes';

import { methodTitle, methodTab } from 'uiTheme/radium/exploreTransform';
import { PALE_BLUE } from 'uiTheme/radium/colors';
import ExtractTextForm from './components/forms/ExtractTextForm';
import ExtractListForm from './components/forms/ExtractListForm';
import ExtractMapForm from './components/forms/ExtractMapForm';
import ReplacePatternForm from './components/forms/ReplacePatternForm';
import ReplaceValuesForm from './components/forms/ReplaceValuesForm';
import ReplaceCustomForm from './components/forms/ReplaceCustomForm';
import ReplaceExactForm from './components/forms/ReplaceExactForm';
import ReplaceRangeForm from './components/forms/ReplaceRangeForm';
import SplitForm from './components/forms/SplitForm';
import './TransformView.less';

@Radium
export default class TransformView extends Component {

  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    onTransformChange: PropTypes.func.isRequired,
    loadTransformCardPreview: PropTypes.func.isRequired,
    submit: PropTypes.func.isRequired,
    subTitles: PropTypes.array,
    subTitle: PropTypes.string,
    selectSubTitle: PropTypes.object,
    changeFormType: PropTypes.func,
    loadTransformValuesPreview: PropTypes.func,
    cancel: PropTypes.func,
    sqlSize: PropTypes.number,
    location: PropTypes.object.isRequired,
    dataset: PropTypes.instanceOf(Immutable.Map),
    cardsViewState: PropTypes.instanceOf(Immutable.Map)
  };

  getFormProps() {
    return {
      dataset: this.props.dataset,
      transform: this.props.transform,
      onTransformChange: this.props.onTransformChange,
      submit: this.props.submit,
      onCancel: this.props.cancel,
      changeFormType: this.props.changeFormType,
      sqlSize: this.props.sqlSize,
      loadTransformCardPreview: this.props.loadTransformCardPreview,
      viewState: this.props.cardsViewState
    };
  }

  filterSubtitles() {
    const { subTitles, transform } = this.props;
    return subTitles.map((subtitle) => {
      return subtitle.types.map((type) => {
        if (type === transform.get('columnType')) {
          return this.renderSubHeadersTitle(subtitle);
        }
      });
    });
  }

  renderSubHeadersTitle(subtitle) {
    if (!this.props.cardsViewState.get('isInProgress') && subtitle.name === this.props.transform.get('method')) {
      return <div style={[methodTab, {backgroundColor: 'rgba(0,0,0,0.05)'}]} key={subtitle.id}>{subtitle.name}</div>;
    }
    const { location } = this.props;
    return <Link
      style={methodTab} key={subtitle.id}
      to={{ ...location, state: {...location.state, method: subtitle.name }}}>
      {subtitle.name}
    </Link>;
  }

  renderReplace() {
    const { transform } = this.props;
    const formProps = this.getFormProps();
    const method = transform.get('method');
    const formKey = `${transform.get('transformType')}.${transform.get('method')}`;

    return (
      <Tabs activeTab={method}>
        <ReplacePatternForm
          {...formProps}
          tabId='Pattern'
          formKey={formKey}
        />
        <ReplaceValuesForm
          tabId='Values'
          {...formProps}
          loadTransformValuesPreview={this.props.loadTransformValuesPreview}
          formKey={formKey}
        />
        <ReplaceCustomForm
          tabId='Custom Condition'
          {...formProps}
          loadTransformValuesPreview={this.props.loadTransformValuesPreview}
          formKey={formKey}
        />
        <ReplaceExactForm
          tabId='Exact'
          {...formProps}
          loadTransformValuesPreview={this.props.loadTransformValuesPreview}
          formKey={formKey}
          />
        <ReplaceRangeForm
          tabId='Range'
          {...formProps}
          formKey={formKey}
        />
      </Tabs>
    );
  }



  renderTab() {
    const { transform, location } = this.props;
    const columnType = transform.get('columnType');
    const initializeColumnTypeForExtract = columnType === LIST || columnType === MAP
      ? columnType
      : 'default';

    const formProps = this.getFormProps();

    return (
      <Tabs activeTab={transform.get('transformType')} >
        <Tabs tabId='extract' activeTab={initializeColumnTypeForExtract} >
          <ExtractTextForm
            tabId='default'
            {...formProps}/>
          <ExtractListForm
            tabId={LIST}
            {...formProps}/>
          <ExtractMapForm
            tabId={MAP}
            location={location}
            {...formProps}/>
        </Tabs>
        <div tabId='replace'>
          {this.renderReplace()}
        </div>
        <Tabs tabId='split' activeTab='split'>
          <SplitForm
            tabId='split'
            {...formProps}/>
        </Tabs>
        <div tabId='keeponly'>
          {this.renderReplace()}
        </div>
        <div tabId='exclude'>
          {this.renderReplace()}
        </div>
      </Tabs>
    );
  }

  render() {
    const transformType  = this.props.transform.get('transformType');
    const subtitles = ['replace', 'keeponly', 'exclude'].indexOf(transformType) !== -1 ? this.filterSubtitles() : null;
    return (
      <div className='transform' style={[styles.base]}>
        <div className='subTitles' style={styles.titles}>
          {subtitles ? <span style={methodTitle}>Method:</span> : null}
          {subtitles}
        </div>
        {this.renderTab()}
      </div>
    );
  }
}

const styles = {
  base: {
    backgroundColor: PALE_BLUE,
    display: 'flex',
    flexDirection: 'column',
    flexWrap: 'wrap',
    alignItems: 'stretch',
    position: 'relative'
  },
  emptyCard: {
    height: 180,
    width: 460,
    minWidth: 455,
    marginLeft: 10,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    position: 'relative',
    backgroundColor: '#fff',
    border: '1px solid #f2f2f2',
    cursor: 'pointer'
  },
  titles: {
    display: 'flex',
    marginTop: 2,
    marginBottom: 2
  }
};
