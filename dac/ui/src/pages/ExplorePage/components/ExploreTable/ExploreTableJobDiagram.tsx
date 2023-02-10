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
import { getExploreJobId } from "@app/selectors/explore";
import { getJobList } from "@app/selectors/jobs";
import { JOB_STATUS } from "./constants";
import ExploreTableJobDiagramState from "./ExploreTableJobDiagramState";
import * as classes from "./ExploreTableJobDiagram.module.less";

type ExploreTableJobDiagramProps = {
  curJobState: string;
};

function ExploreTableJobDiagram({ curJobState }: ExploreTableJobDiagramProps) {
  const jobStates = [
    JOB_STATUS.pending,
    JOB_STATUS.metadataRetrieval,
    JOB_STATUS.planning,
    JOB_STATUS.engineStart,
    JOB_STATUS.queued,
    JOB_STATUS.executionPlanning,
    JOB_STATUS.starting,
    JOB_STATUS.running,
  ];

  const hasStateCompleted = (stateIndex: number) => {
    const curStateIndex = jobStates.findIndex(
      (jobState) => jobState === curJobState
    );

    return stateIndex < curStateIndex;
  };

  return (
    <div className={classes["sqlEditor__jobDiagram"]}>
      <ExploreTableJobDiagramState
        isCurrentState={curJobState === JOB_STATUS.engineStart}
        isStateComplete={hasStateCompleted(3)}
        state="Job.State.ENGINE_START"
      />
      <div className={classes["sqlEditor__jobDiagram__link"]}>
        <div
          className={
            hasStateCompleted(3)
              ? classes["sqlEditor__jobDiagram__link-line--success"]
              : classes["sqlEditor__jobDiagram__link-line"]
          }
        ></div>
      </div>
      <ExploreTableJobDiagramState
        isCurrentState={curJobState === JOB_STATUS.queued}
        isStateComplete={hasStateCompleted(4)}
        state="Job.State.QUEUED"
      />
      <div className={classes["sqlEditor__jobDiagram__link"]}>
        <div
          className={
            hasStateCompleted(4)
              ? classes["sqlEditor__jobDiagram__link-line--success"]
              : classes["sqlEditor__jobDiagram__link-line"]
          }
        ></div>
      </div>
      <ExploreTableJobDiagramState
        isCurrentState={curJobState === JOB_STATUS.executionPlanning}
        isStateComplete={hasStateCompleted(5)}
        state="Job.State.EXECUTION_PLANNING"
      />
      <div className={classes["sqlEditor__jobDiagram__link"]}>
        <div
          className={
            hasStateCompleted(5)
              ? classes["sqlEditor__jobDiagram__link-line--success"]
              : classes["sqlEditor__jobDiagram__link-line"]
          }
        ></div>
      </div>
      <ExploreTableJobDiagramState
        isCurrentState={curJobState === JOB_STATUS.starting}
        isStateComplete={hasStateCompleted(6)}
        state="Job.State.STARTING"
      />
      <div className={classes["sqlEditor__jobDiagram__link"]}>
        <div
          className={
            hasStateCompleted(6)
              ? classes["sqlEditor__jobDiagram__link-line--success"]
              : classes["sqlEditor__jobDiagram__link-line"]
          }
        ></div>
      </div>
      <ExploreTableJobDiagramState
        isCurrentState={curJobState === JOB_STATUS.running}
        isStateComplete={hasStateCompleted(7)}
        state="Job.State.RUNNING"
      />
    </div>
  );
}

function mapStateToProps(state: any) {
  const jobId = getExploreJobId(state);
  const jobList = getJobList(state).toArray();
  const jobDetails = jobList.find(
    (jobDetails: any) => jobDetails.get("id") === jobId
  );
  const curJobState = jobDetails.get("state");

  return {
    curJobState,
  };
}

export default connect(mapStateToProps)(ExploreTableJobDiagram);
