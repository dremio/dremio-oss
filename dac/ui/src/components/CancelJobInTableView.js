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

import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { injectIntl } from "react-intl";
import * as ButtonTypes from "@app/components/Buttons/ButtonTypes";
import { Button } from "dremio-ui-lib";
import Immutable from "immutable";
import {
  getJobProgress,
  getRunStatus,
  getExploreJobId,
} from "selectors/explore";
import { cancelJobAndShowNotification } from "@app/actions/jobs/jobs";
import { getLocation } from "selectors/routing";
import { cancelTransform } from "actions/explore/dataset/transform";
import { getExploreViewState } from "selectors/resources";
import "./CancelJobInTableView.less";

const CancelJobInTableView = (props) => {
  const [jobId, setJobId] = useState(props.jobId);
  // const [queryStatus, setQueryStatus] = useState('');
  // const [queryMessage, setQueryMessage] = useState('');

  useEffect(() => {
    setJobId(props.jobId);
  }, [props.jobId]);

  // useEffect(() => {
  //     if (props.jobProgress && jobId === props.jobId) {
  //         setQueryStatus(props.jobProgress.status);
  //         let msg = '';
  //         switch (props.jobProgress.status) {
  //             case 'ENGINE_START' : setQueryMessage ('Engine starting, please wait...');
  //             break;
  //             case 'STARTING' : setQueryMessage ('Compiling the query...');
  //             break;
  //             case 'RUNNING' : setQueryMessage ('Query running...');
  //             break;
  //             // case 'COMPLETED' : setQueryMessage ('Completed!');
  //             // break;
  //             default : props.jobProgress.status !== 'COMPLETED' ? setQueryMessage ('Collecting initial data...') : setQueryMessage('');
  //             break;
  //         }
  //     }
  // }, [props.jobProgress])

  const viewId = props.exploreViewState.get("viewId");
  return (
    <div className="view-state-cancel-wrapper-overlay">
      <div>
        {/* not doing this now, pending discussion
            <span className='view-state-wrapper-inner-span'>{queryMessage}</span> */}
        <Button
          color="secondary"
          type={ButtonTypes.SECONDARY}
          data-qa="qa-cancel-job"
          title={props.intl.formatMessage({ id: "Query.Table.Cancel" })}
          onClick={() => {
            jobId
              ? props.cancelJobAndShowNotification(jobId)
              : props.cancelTransform(viewId);
          }}
          style={{
            position: "absolute",
            left: "50%",
            transform: "translateX(-50%)",
            top: "40%",
          }}
          disableMargin
        >
          <dremio-icon
            name="sql-editor/cancel-job"
            class="view-state-cancel-job"
          />
          {`${props.intl.formatMessage({ id: "Query.Table.Cancel" })}`}
        </Button>
      </div>
    </div>
  );
};
const mapStateToProps = (state) => {
  const location = getLocation(state);
  const version = location.query && location.query.version;
  const jobId = getExploreJobId(state);
  const jobProgress = getJobProgress(state, version);
  const runStatus = getRunStatus(state).isRun;
  const exploreViewState = getExploreViewState(state);
  return {
    jobProgress,
    runStatus,
    jobId,
    exploreViewState,
  };
};

const mapDispatchToProps = {
  cancelJobAndShowNotification,
  cancelTransform,
};

CancelJobInTableView.propTypes = {
  intl: PropTypes.object,
  exploreViewState: PropTypes.instanceOf(Immutable.Map),
  jobProgress: PropTypes.object,
  jobId: PropTypes.string,
  cancelJobAndShowNotification: PropTypes.func,
  cancelTransform: PropTypes.func,
};

export default injectIntl(
  connect(mapStateToProps, mapDispatchToProps)(CancelJobInTableView)
);
