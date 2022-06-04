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

import "./CommitDivergenceGraph.less";

type CommitDivergenceGraphProps = {
  commitsBehind: number | undefined;
  commitsAhead: number | undefined;
};

function CommitDivergenceGraph({
  commitsBehind = 0,
  commitsAhead = 0,
}: CommitDivergenceGraphProps): JSX.Element {
  if (commitsBehind === undefined || commitsAhead === undefined) return <></>;

  const total = commitsBehind + commitsAhead;

  return (
    <div className="commit-divergence">
      <span className="commit-divergence-group">
        <div className="count count-behind">{commitsBehind}</div>
        <div
          className="bar bar-behind"
          style={{
            width: !!total
              ? `${Math.floor((commitsBehind / total) * 100)}%`
              : "0%",
          }}
        ></div>
      </span>
      <span className="commit-divergence-group">
        <div className="count count-ahead">{commitsAhead}</div>
        <div
          className="bar bar-ahead"
          style={{
            width: !!total
              ? `${Math.floor((commitsAhead / total) * 100)}%`
              : "0%",
          }}
        ></div>
      </span>
    </div>
  );
}

export default CommitDivergenceGraph;
