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
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import { withRouter } from 'react-router';
import { getCanTagsBeSkipped } from '@app/selectors/home';
import { Alert } from '@app/components/Alert';

const mapStateToProps = (state, { location }) => ({
  showAlert: getCanTagsBeSkipped(state, location.pathname)
});

export const TagsAlertView = ({ showAlert }) => {
  // see CollaborationHelper.getTagsForIds, variable 'maxTagRequestCount' methods for max count number
  return showAlert ? <Alert text='Tags are only shown inline for the first 200 items.' /> : null;
};
TagsAlertView.propTypes = {
  showAlert: PropTypes.bool
};

export const TagsAlert = withRouter(connect(mapStateToProps)(TagsAlertView));
