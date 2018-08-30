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
import { connect } from 'react-redux';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { loadSummaryDataset } from 'actions/resources/dataset';
import { getSummaryDataset } from 'selectors/datasets';

import ReflectionList from './ReflectionList';

const VIEW_ID = 'ReflectionBlock';

@PureRender
class ReflectionBlock extends Component {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map).isRequired,
    loadSummaryDataset: PropTypes.func,
    summaryDataset: PropTypes.object,
    datasetFullPath: PropTypes.instanceOf(Immutable.List)
  };

  componentWillMount() {
    this.props.loadSummaryDataset(this.props.datasetFullPath.join('/'), VIEW_ID);
  }

  render() {
    const { jobDetails, summaryDataset } = this.props;
    const materializationFor = jobDetails.get('materializationFor');
    const datasetPathList = jobDetails.get('datasetPathList');
    const datasetQueryLinks = summaryDataset.get('links');
    const datasetLink = datasetQueryLinks ? datasetQueryLinks.get('query') : '';

    const reflectionsListData = [
      {
        reflection: {
          type: materializationFor.get('reflectionType'),
          name: materializationFor.get('reflectionName'),
          id: materializationFor.get('reflectionId')
        },
        dataset: {
          id: materializationFor.get('datasetId'),
          path: datasetPathList
        },
        datasetLink: {
          pathname: datasetLink,
          title: la('Parent dataset')
        }
      }];
    return (
      <ReflectionList reflections={reflectionsListData} jobDetails={jobDetails} />
    );
  }

}

function mapStateToProps(state, props) {
  const fullPath = props.datasetFullPath.join(',');
  return {
    summaryDataset: getSummaryDataset(state, fullPath)
  };
}

export default connect(mapStateToProps, { loadSummaryDataset })(ReflectionBlock);
