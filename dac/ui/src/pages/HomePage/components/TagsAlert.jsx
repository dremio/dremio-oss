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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { withRouter } from "react-router";
import { getCanTagsBeSkipped } from "#oss/selectors/home";
import { Tooltip } from "dremio-ui-lib";
import * as classes from "./TagsAlert.module.less";

const mapStateToProps = (state, { location }) => ({
  showAlert: getCanTagsBeSkipped(state, location.pathname),
});

export const TagsAlertView = ({ showAlert }) => {
  // see CollaborationHelper.getTagsForIds, variable 'maxTagRequestCount' methods for max count number
  return showAlert ? (
    <>
      <Tooltip
        title="Tags are only shown inline for the first 200 items."
        placement="right"
      >
        <dremio-icon name="interface/warning" class={classes["icon"]} />
      </Tooltip>
    </>
  ) : null;
};
TagsAlertView.propTypes = {
  showAlert: PropTypes.bool,
};

export const TagsAlert = withRouter(connect(mapStateToProps)(TagsAlertView));
