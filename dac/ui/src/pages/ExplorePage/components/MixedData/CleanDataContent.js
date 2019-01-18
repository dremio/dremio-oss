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
import { connect } from 'react-redux';
import { Link } from 'react-router';
import Radium from 'radium';

import PropTypes from 'prop-types';

import Tabs from 'components/Tabs';
import { getViewState } from 'selectors/resources';
import { getExploreState } from '@app/selectors/explore';
import { loadCleanData, CLEAN_DATA_VIEW_ID } from 'actions/explore/dataset/get';

import { PALE_BLUE, EXPLORE_HOVER_COLOR } from 'uiTheme/radium/colors';
import { formLabel } from 'uiTheme/radium/typography';
import { FLEX_COL_START, LINE_CENTER_CENTER, LINE_START_CENTER } from 'uiTheme/radium/flexStyle';
import { methodTitle, methodTab } from 'uiTheme/radium/exploreTransform';

import SingleTypeForm from './forms/SingleTypeForm';
import SplitTypeForm from './forms/SplitTypeForm';

@Radium
class CleanDataContent extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    detailType: PropTypes.string,
    columnName: PropTypes.string,
    location: PropTypes.object,
    changeFormType: PropTypes.func,
    submit: PropTypes.func,
    cancel: PropTypes.func,
    loadCleanData: PropTypes.func.isRequired,
    single: PropTypes.instanceOf(Immutable.List),
    split: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    const {columnName, dataset} = this.props;
    this.props.loadCleanData(columnName, dataset);
  }

  renderCleanTypeItem(cleanType) {
    if (cleanType.id === this.props.detailType) {
      return <div style={[methodTab, {backgroundColor: 'rgba(0,0,0,0.05)'}]} key={cleanType.id}>
        {cleanType.label}
      </div>;
    }
    const {location} = this.props;
    return <Link
      style={methodTab} key={cleanType.id}
      to={{...location, query: {...location.query, type: cleanType.id}}}>
      {cleanType.label}
    </Link>;
  }

  renderCleanTypes() {
    const cleanTypes = [
      {id: 'SINGLE_DATA_TYPE', label: 'Convert to Single Type'},
      {id: 'SPLIT_BY_DATA_TYPE', label: 'Split by Data Type'}
    ];
    return (
      <div style={LINE_START_CENTER}>
        <span style={methodTitle}>{la('Method:')}</span>
        {cleanTypes.map((cleanType) => this.renderCleanTypeItem(cleanType))}
      </div>
    );
  }

  render() {
    const formProps = {
      dataset: this.props.dataset,
      submit: this.props.submit,
      columnName: this.props.columnName,
      onCancel: this.props.cancel,
      viewState: this.props.viewState
    };
    return (
      <div style={styles.base}>
        {this.renderCleanTypes()}
        <Tabs activeTab={this.props.detailType}>
          <SingleTypeForm tabId='SINGLE_DATA_TYPE' {...formProps} singles={this.props.single.toJS()}/>
          <SplitTypeForm tabId='SPLIT_BY_DATA_TYPE' {...formProps} split={this.props.split.toJS()}/>
        </Tabs>
      </div>
    );
  }
}

function mapStateToProps(state) {
  const explorePageState = getExploreState(state);
  return {
    single: explorePageState.recommended.get('cleanData').get('single'),
    split: explorePageState.recommended.get('cleanData').get('split'),
    viewState: getViewState(state, CLEAN_DATA_VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadCleanData
})(CleanDataContent);

const styles = {
  base: {
    position: 'relative',
    backgroundColor: PALE_BLUE,
    ...FLEX_COL_START
  },
  btn: {
    ...LINE_CENTER_CENTER,
    marginLeft: 10,
    height: 28,
    width: 130,
    borderRadius: 2,
    ...formLabel,
    cursor: 'pointer',
    ':hover': {
      backgroundColor: EXPLORE_HOVER_COLOR
    }
  }
};

