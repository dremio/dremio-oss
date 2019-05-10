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
import ImmutablePropTypes from 'react-immutable-proptypes';

import { connect } from 'react-redux';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import isEmpty from 'lodash/isEmpty';

import { getViewState } from 'selectors/resources';
import { getExploreState } from '@app/selectors/explore';

import dataStoreUtils from 'utils/dataStoreUtils';
import exploreUtils from 'utils/explore/exploreUtils';

import {
  loadTransformCards,
  loadTransformCardPreview,
  loadTransformValuesPreview,
  LOAD_TRANSFORM_CARDS_VIEW_ID
} from 'actions/explore/recommended';
import { resetViewState } from 'actions/resources';

import { MAP, LIST } from 'constants/DataTypes';

import TransformView from './TransformView';

@pureRender
@Radium
export class Transform extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func.isRequired,
    cardValues: PropTypes.object,
    cancel: PropTypes.func,
    changeFormType: PropTypes.func.isRequired,

    // connected
    transform: ImmutablePropTypes.contains({
      transformType: PropTypes.string,
      columnType: PropTypes.string.isRequired
    }),
    sqlSize: PropTypes.number,
    cardsViewState: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object,

    // actions
    loadTransformCards: PropTypes.func.isRequired,
    loadTransformCardPreview: PropTypes.func.isRequired,
    loadTransformValuesPreview: PropTypes.func.isRequired,
    resetViewState: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.subTitles = dataStoreUtils.getSubtypeForTransformTab();
    this.loadTransformCardPreview = this.loadTransformCardPreview.bind(this);
    this.loadTransformValuesPreview = this.loadTransformValuesPreview.bind(this);
  }

  componentDidMount() {
    this.props.resetViewState(LOAD_TRANSFORM_CARDS_VIEW_ID);
    this.loadTransformCards(this.props).then((action) => {
      if (!action) {
        return;
      }

      const { transform, location } = this.props;
      if (transform.get('method') === 'Values' && action.payload.values) {
        const allUnique = action.payload.values.availableValues.every((item) => item.count === 1);
        if (allUnique) {
          this.context.router.replace({
            ...location,
            state: {
              ...location.state,
              method: 'Pattern'
            }
          });
        }
      }
    });
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.transform.equals(nextProps.transform)) {
      this.loadTransformCards(nextProps);
    }
  }

  loadTransformCardPreview(index, model) {
    const { transform } = this.props;
    const columnName = transform.get('columnName');

    const selection = exploreUtils.transformHasSelection(transform)
      ? { columnName }
      : { columnName, mapPathList: model.path ? [model.path] : undefined }; // TODO does api actually use this?
    const actionType = this.transformTypeURLMapper(transform);
    const data = {
      selection,
      rule: model
    };
    return this.props.loadTransformCardPreview(data, transform, this.props.dataset, actionType, index);
  }

  loadTransformCards(props) {
    const { transform } = props;
    const method = transform.get('method');
    if (
      exploreUtils.needSelection(method) && !exploreUtils.transformHasSelection(transform) ||
      !exploreUtils.needsToLoadCardFormValuesFromServer(transform)
    ) {
      return Promise.resolve();
    }

    const transformSelection = transform.get('selection').toJS();
    const columnName = transform.get('columnName');
    const actionType = this.transformTypeURLMapper(transform);
    const selection = !isEmpty(transformSelection) ? transformSelection : exploreUtils.getDefaultSelection(columnName);
    return props.loadTransformCards(selection, transform, this.props.dataset, actionType);
  }

  loadTransformValuesPreview(values) {
    const { transform } = this.props;
    const transformType = transform.get('transformType');
    const columnName = transform.get('columnName');

    const selection = !exploreUtils.transformHasSelection(transform)
      ? { columnName }
      : transform.get('selection') && transform.get('selection').toJS();

    const data = {
      selection,
      values
    };
    this.props.loadTransformValuesPreview(data, transform, this.props.dataset, transformType);
  }

  handleTransformChange = (newTransform) => {
    const {location} = this.props;
    this.context.router.push({...location, state: newTransform.toJS()});
  };

  transformTypeURLMapper(transform) {
    const transformType = transform.get('transformType');
    const columnType = transform.get('columnType');

    if (transformType === 'split') {
      return transformType;
    }
    switch (columnType) {
    case MAP: return `${transformType}_map`;
    case LIST: return `${transformType}_list`;
    default: return transformType;
    }
  }

  renderContent() {
    return (
      <TransformView
        dataset={this.props.dataset}
        transform={this.props.transform}
        onTransformChange={this.handleTransformChange}
        loadTransformValuesPreview={this.loadTransformValuesPreview}
        cardValues={this.props.cardValues}
        submit={this.props.submit}
        changeFormType={this.props.changeFormType}
        loadTransformCardPreview={this.loadTransformCardPreview}
        cancel={this.props.cancel}
        subTitles={this.subTitles}
        sqlSize={this.props.sqlSize}
        location={this.props.location}
        cardsViewState={this.props.cardsViewState}
      />
    );
  }

  render() {
    return (
      <div>
        { this.renderContent() }
      </div>
    );
  }
}

function mapStateToProps(state) {
  const location = state.routing.locationBeforeTransitions;
  const transform = exploreUtils.getTransformState(location);
  return {
    transform,
    sqlSize: getExploreState(state).ui.get('sqlSize'),
    cardsViewState: getViewState(state, LOAD_TRANSFORM_CARDS_VIEW_ID),
    location
  };
}

export default connect(mapStateToProps, {
  loadTransformCards,
  loadTransformCardPreview,
  loadTransformValuesPreview,
  resetViewState
})(Transform);
