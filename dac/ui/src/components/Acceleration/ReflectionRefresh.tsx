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

import { intl } from "#oss/utils/intl";
import { Button } from "dremio-ui-lib/components";
import Radio from "#oss/components/Fields/Radio";
import FieldWithError from "#oss/components/Fields/FieldWithError";
import DurationField from "#oss/components/Fields/DurationField";
import { ScheduleRefresh } from "#oss/components/Forms/ScheduleRefresh";
import Checkbox from "#oss/components/Fields/Checkbox";
import { formDefault } from "uiTheme/radium/typography";
import { SCHEDULE_POLICIES } from "#oss/components/Forms/DataFreshnessSection";
import { withFormContext } from "#oss/pages/HomePage/components/modals/formContext";
import { isIcebergSource } from "@inject/utils/sourceUtils";
import Immutable from "immutable";

type ReflectionRefreshProps = {
  fields: {
    accelerationRefreshPeriod: any;
    accelerationGracePeriod: any;
    accelerationNeverExpire: any;
    accelerationNeverRefresh: any;
    accelerationRefreshSchedule: any;
    accelerationActivePolicyType: any;
    accelerationRefreshOnDataChanges: any;
  };
  entityType: string;
  refreshingReflections: boolean;
  isRefreshAllowed: boolean;
  minDuration: number;
  refreshAll: () => void;
  isLiveReflectionEnabled: boolean;
  formContext: Record<string, any>;
  fileFormatType: string;
};

const ReflectionRefresh = ({
  entityType,
  fields,
  refreshingReflections,
  isRefreshAllowed,
  refreshAll,
  minDuration,
  isLiveReflectionEnabled,
  formContext,
  fileFormatType,
}: ReflectionRefreshProps) => {
  const {
    accelerationRefreshPeriod,
    accelerationGracePeriod,
    accelerationNeverExpire,
    accelerationRefreshSchedule,
    accelerationActivePolicyType,
    accelerationRefreshOnDataChanges,
  } = fields;
  const isLiveReflectionEnabledForSource =
    isLiveReflectionEnabled &&
    isIcebergSource(formContext?.sourceType || "") &&
    entityType === "source";
  const isLiveReflectionEnabledForTable =
    isLiveReflectionEnabled &&
    entityType === "dataset" &&
    fileFormatType === "Iceberg";

  return (
    <div className="flex flex-col">
      {isRefreshAllowed && entityType === "dataset" && (
        <div>
          <p className="text-semibold text-base pb-1">
            {intl.formatMessage({
              id: "Reflection.Refresh.Once",
            })}
          </p>

          <Button
            disabled={refreshingReflections}
            onClick={refreshAll}
            variant="secondary"
            style={{ marginBottom: 24 }}
          >
            {intl.formatMessage({
              id: "Reflection.Refresh.Now",
            })}
          </Button>
        </div>
      )}
      <div className="mb-3">
        <div>
          <p className="text-semibold text-base pb-05">
            {intl.formatMessage({
              id: "Reflection.Refresh.Settings",
            })}
          </p>
        </div>
        <div>
          <Radio
            {...accelerationActivePolicyType}
            radioValue={SCHEDULE_POLICIES.NEVER}
            label={intl.formatMessage({
              id: "Reflection.Refresh.Never",
            })}
          />
        </div>
        <div className="flex flex-col">
          <Radio
            {...accelerationActivePolicyType}
            radioValue={SCHEDULE_POLICIES.PERIOD}
            label={
              <>
                <td>
                  <div style={styles.inputLabel}>
                    {intl.formatMessage({
                      id: "Reflection.Refresh.Every",
                    })}
                  </div>
                </td>
                <td>
                  <FieldWithError
                    errorPlacement="right"
                    {...accelerationRefreshPeriod}
                  >
                    <div style={{ display: "flex" }}>
                      <DurationField
                        {...accelerationRefreshPeriod}
                        min={minDuration}
                        style={styles.durationField}
                        disabled={
                          accelerationActivePolicyType.value !==
                          SCHEDULE_POLICIES.PERIOD
                        }
                      />
                    </div>
                  </FieldWithError>
                </td>
              </>
            }
          />
          {isLiveReflectionEnabledForSource &&
            accelerationActivePolicyType.value === SCHEDULE_POLICIES.PERIOD && (
              <Checkbox
                {...accelerationRefreshOnDataChanges}
                label={intl.formatMessage({
                  id: "Reflection.Refresh.AutoRefresh",
                })}
                className="pb-1 pl-3"
              />
            )}
        </div>
        <div className="flex flex-col">
          <Radio
            {...accelerationActivePolicyType}
            style={{ alignItems: "flex-start", marginTop: 10 }}
            radioValue={SCHEDULE_POLICIES.SCHEDULE}
            label={
              <div className="flex flex-col">
                <td>
                  <div style={styles.inputLabel}>
                    {intl.formatMessage({
                      id: "Reflection.Refresh.SetSchedule",
                    })}
                  </div>
                </td>
                {SCHEDULE_POLICIES.SCHEDULE ===
                  accelerationActivePolicyType.value && (
                  <td>
                    <FieldWithError
                      errorPlacement="right"
                      {...accelerationRefreshSchedule}
                    >
                      <div
                        style={{
                          display: "flex",
                          flexDirection: "column",
                        }}
                      >
                        <ScheduleRefresh
                          accelerationRefreshSchedule={
                            accelerationRefreshSchedule
                          }
                        />
                      </div>
                    </FieldWithError>
                  </td>
                )}
              </div>
            }
          />
          {isLiveReflectionEnabledForSource &&
            accelerationActivePolicyType.value ===
              SCHEDULE_POLICIES.SCHEDULE && (
              <Checkbox
                {...accelerationRefreshOnDataChanges}
                label={intl.formatMessage({
                  id: "Reflection.Refresh.AutoRefresh",
                })}
                className="pt-05 pl-3"
              />
            )}
        </div>
        {isLiveReflectionEnabledForTable && (
          <div>
            <Radio
              {...accelerationActivePolicyType}
              radioValue={SCHEDULE_POLICIES.REFRESH_ON_DATA_CHANGES}
              label={intl.formatMessage({
                id: "Reflection.Refresh.AutoRefresh",
              })}
              style={{ marginTop: 14 }}
            />
          </div>
        )}
      </div>
      <div>
        <p className="text-semibold text-base pb-05">
          {intl.formatMessage({
            id: "Reflection.Expire.Settings",
          })}
        </p>
      </div>
      <div>
        <div style={styles.inputLabelMargin}>
          <Checkbox
            {...accelerationNeverExpire}
            label={intl.formatMessage({
              id: "Reflection.Expire.Never",
            })}
            style={{ fontSize: 14 }}
            disabled={
              accelerationActivePolicyType.value ===
              SCHEDULE_POLICIES.REFRESH_ON_DATA_CHANGES
            }
          />
        </div>
      </div>
      <div className="flex flex-row items-center">
        <div style={styles.inputLabelMargin}>
          {intl.formatMessage({
            id: "Reflection.Expire.After",
          })}
        </div>
        <FieldWithError errorPlacement="right" {...accelerationGracePeriod}>
          <DurationField
            {...accelerationGracePeriod}
            min={minDuration}
            style={styles.durationField}
            disabled={!!accelerationNeverExpire.value}
          />
        </FieldWithError>
      </div>
    </div>
  );
};

const styles = {
  container: {
    marginTop: 6,
  },
  info: {
    maxWidth: 525,
    marginBottom: 26,
  },
  section: {
    display: "flex",
    marginBottom: 6,
    alignItems: "center",
  },
  select: {
    width: 164,
    marginTop: 3,
  },
  label: {
    fontSize: "18px",
    fontWeight: 600,
    margin: "0 0 8px 0px",
    display: "flex",
    alignItems: "center",
    color: "var(--text--primary)",
  },
  inputLabel: {
    ...formDefault,
    marginRight: 10,
    fontSize: 14,
  },
  inputLabelMargin: {
    ...formDefault,
    marginRight: 10,
    marginBottom: 6,
    fontSize: 14,
  },
  durationField: {
    width: 250,
  },
};

export default withFormContext(ReflectionRefresh);
