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
/* eslint-disable */
import { getIntlContext } from "../../../../contexts/IntlContext";
import { formatDurationMetric } from "../../../../utilities/formatDuration";
import { JobStatusLabels } from "../../JobStatus";

const getPercentage = (duration: string, totalTime: number) => {
  return ((Number(duration) / totalTime) * 100).toFixed(2);
};

const getMetricWidth = (duration: string, totalTime: number) => {
  return (Number(duration) / totalTime) * 360;
};

export const JobMetrics = ({ job }: { job: any }) => {
  const durationDetails = job.durationDetails || [];
  if (durationDetails.length === 0) return null;

  const { t } = getIntlContext();
  const totalTime = job.duration;

  return (
    <div className="job-metrics">
      {durationDetails.map((phase: any) => (
        <div className="job-metrics__metric">
          <div className="job-metrics__metricLabel">
            {t(
              JobStatusLabels[phase.phaseName as keyof typeof JobStatusLabels]
            )}
          </div>
          <div className="job-metrics__metricValues">
            {formatDurationMetric(phase.phaseDuration)}
            {` (${getPercentage(phase.phaseDuration, totalTime)}%)`}
          </div>
          <div
            className="job-metrics__metricBar"
            style={{
              width: getMetricWidth(phase.phaseDuration, totalTime),
            }}
          />
        </div>
      ))}
    </div>
  );
};
