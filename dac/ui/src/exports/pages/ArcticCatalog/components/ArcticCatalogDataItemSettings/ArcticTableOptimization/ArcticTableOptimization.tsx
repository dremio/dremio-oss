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
import { useEffect, useState, useMemo } from "react";
import { useResourceSnapshot } from "smart-resource/react";
import { useIntl } from "react-intl";
import { Link } from "react-router";
import {
  Button,
  useModalContainer,
  ModalContainer,
  DialogContent,
  //@ts-ignore
} from "dremio-ui-lib/components";
import { FormProvider, useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { OptimizationForm } from "./components/OptimizationForm/OptimizationForm";
import { startJob } from "@app/exports/endpoints/ArcticCatalogs/Optimization/startJob";
import { optimizationScheduleResource } from "@app/exports/resources/OptimizationScheduleResource";
import { AdvancedConfig } from "./components/AdvancedConfig/AdvancedConfig";
import { JobScheduleInfo } from "@app/exports/endpoints/ArcticCatalogs/Optimization/Schedule.types";
import { ArcticJobsResource } from "@app/exports/resources/ArcticCatalogJobsResource";
import {
  getInitialValues,
  transformAdvancedConfigToString,
} from "./compactionUtils";
//@ts-ignore
import * as orgPaths from "dremio-ui-common/paths/organization.js";
import { validationSchema } from "./validationSchema";
import Message from "@app/components/Message";
import { Schedule } from "./components/Schedule/Schedule";
import { EngineStateRead } from "@app/exports/endpoints/ArcticCatalogs/Configuration/CatalogConfiguration.types";
import * as PATHS from "@app/exports/paths";
import * as classes from "./ArcticTableOptimization.module.less";

const defaultValues = {
  schedule: {
    scheduleType: "@hours",
    hourInput: 1,
    weekInput: ["1"],
    timeInput: new Date(),
    monthScheduleOption: "first",
    dayOfMonth: 1,
    ordinalOfEveryMonth: "1",
    weekdayOfEveryMonth: "0",
  },
  advancedConfig: {
    maxFileSize: 460,
    minFileSize: 192,
    minFiles: 5,
    targetFileSize: 256,
  },
};
export const ArcticTableOptimizationForm = ({
  reference,
  tableId,
  catalogId,
  configState,
}: {
  reference: string;
  tableId: string;
  catalogId: string;
  configState?: EngineStateRead;
}) => {
  const { formatMessage } = useIntl();
  const optimizeNowModal = useModalContainer();
  const [loading, setLoading] = useState<boolean>(true);
  const [scheduleId, setScheduleId] = useState<string>("");
  const [showForm, setShowForm] = useState<boolean>(false);
  const [cron, setCron] = useState<string>("");
  const [message, setMessage] = useState<
    { text: any; type: string } | undefined
  >(undefined);
  const [schedules] = useResourceSnapshot(optimizationScheduleResource);
  const [disableOptimizeNow, setDisableOptimizeNow] = useState<{
    disabled: boolean;
    tooltip: string;
  }>({ disabled: false, tooltip: "" });
  const [disableSchedule, setDisableSchedule] = useState<boolean>(false);

  useEffect(() => {
    if (
      configState === EngineStateRead.CREATING ||
      configState === EngineStateRead.UPDATING
    ) {
      setMessage({
        text: formatMessage({
          id:
            configState === EngineStateRead.CREATING
              ? "Optimization.Config.Creating"
              : "Optimization.Config.Updating",
        }),
        type: "info",
      });
      setDisableOptimizeNow({
        disabled: true,
        tooltip: formatMessage({ id: "Optimize.Disabled.Config.Tooltip" }),
      });
      setDisableSchedule(true);
    } else if (configState !== EngineStateRead.ENABLED) {
      setMessage({
        text: (
          <>
            <span className={classes["optimization-message-text"]}>
              {formatMessage({ id: "Optimization.Config.Not.Setup" })}
            </span>
            <Link
              className={classes["optimization-link"]}
              to={PATHS.arcticCatalogSettingsGeneral({
                arcticCatalogId: catalogId,
              })}
            >
              {formatMessage({ id: "Catalog.Settings" })}
            </Link>
            <span className={classes["optimization-link-divider"]}>{">"}</span>
            <Link
              className={classes["optimization-link"]}
              to={PATHS.arcticCatalogSettingsConfigurationNew({
                arcticCatalogId: catalogId,
                mode: "new",
              })}
            >
              {formatMessage({ id: "Catalog.Settings.Configurations" })}
            </Link>
            .
          </>
        ),
        type: "warning",
      });
      setDisableOptimizeNow({
        disabled: true,
        tooltip: formatMessage({ id: "Optimize.Disabled.Config.Tooltip" }),
      });
      setDisableSchedule(true);
    }
  }, [configState, catalogId, formatMessage]);

  useEffect(() => {
    ArcticJobsResource.fetch(
      catalogId,
      `reference == '${reference}' && tableId == '${tableId}' && state in (list('SETUP','QUEUED','STARTING','RUNNING'))`,
      1
    );
  }, [reference, catalogId, tableId]);

  const [data] = useResourceSnapshot(ArcticJobsResource);

  const disableOptimizeNowButton = useMemo(() => {
    const runningJob = data && data.data.length > 0;
    if (runningJob) {
      setDisableOptimizeNow({
        disabled: true,
        tooltip: formatMessage({ id: "Optimize.Disabled.Tooltip" }),
      });
    }
    return runningJob || disableOptimizeNow.disabled;
  }, [data, disableOptimizeNow.disabled, formatMessage]);

  useEffect(() => {
    optimizationScheduleResource.fetch({ catalogId, tableId, reference });
  }, [catalogId, tableId, reference]);

  const [initialValues, setInitialValues] = useState(defaultValues);

  const methods = useForm<any>({
    mode: "onChange",
    resolver: zodResolver(validationSchema),
    defaultValues: initialValues.advancedConfig,
  });

  const onSubmitForOptimizeNow = async ({
    advancedConfig: { maxFileSize, minFileSize, minFiles, targetFileSize },
  }: {
    advancedConfig: {
      maxFileSize: number;
      minFileSize: number;
      minFiles: number;
      targetFileSize: number;
    };
  }) => {
    const transformedAdvancedConfig = {
      maxFileSize: transformAdvancedConfigToString(maxFileSize),
      minFileSize: transformAdvancedConfigToString(minFileSize),
      minFiles,
      targetFileSize: transformAdvancedConfigToString(targetFileSize),
    };
    try {
      await startJob({
        catalogId,
        payload: {
          config: { reference, tableId, ...transformedAdvancedConfig },
          type: JobScheduleInfo.TypeEnum.OPTIMIZE,
        },
      });
      setMessage({
        text: `Table optimization job for ${tableId} has been created. You can view job status under job history tab.`,
        type: "success",
      });

      optimizeNowModal.close();
      setDisableOptimizeNow({
        disabled: true,
        tooltip: formatMessage({ id: "Optimize.Disabled.Tooltip" }),
      });
    } catch (e: any) {
      setMessage({ text: e?.responseBody?.errorMessage, type: "error" });
      optimizeNowModal.close();
    }
  };

  useEffect(() => {
    if (schedules && schedules.data.length > 0) {
      const { data } = schedules;
      //@ts-ignore
      const { schedule, config, id } = data[0];
      setCron(schedule);
      setScheduleId(id);
      setInitialValues(getInitialValues(schedule, config));
      setLoading(false);
    } else if (schedules) {
      setLoading(false);
    } else {
      setLoading(false);
      setScheduleId("");
      setCron("");
      setInitialValues(defaultValues);
    }
  }, [schedules]);

  return (
    <div className={classes["optimization"]}>
      {message?.text && (
        <Message
          onDismiss={() => setMessage(undefined)}
          messageType={message.type}
          message={message.text}
          style={{ position: "sticky", top: 0, marginTop: -16, maxWidth: 600 }}
        />
      )}
      <div className={classes["optimization-container"]}>
        <div className={classes["optimization-description"]}>
          <p className={classes["optimization-description-main"]}>
            {formatMessage({ id: "Arctic.Table.Optimization.Description.PT1" })}
            <Link
              alt="Table optimization documentation link"
              to="https://docs.dremio.com/cloud/sql/commands/optimize-table/"
              target="_blank"
            >
              {formatMessage({ id: "Optimize.Table" })}
            </Link>
            {formatMessage({ id: "Arctic.Table.Optimization.Description.PT2" })}
          </p>
          {/* <p>
            {formatMessage({ id: "Arctic.Table.Optimization.ViewUsage.PT1" })}
            <Link alt="Usage Settings" to={orgPaths.usage.link()}>
              {formatMessage({ id: "Organization.Usage" })}
            </Link>
            {formatMessage({ id: "Arctic.Table.Optimization.ViewUsage.PT2" })}
          </p> */}
        </div>
        <div className={classes["optimization-now"]}>
          <div className={classes["optimization-now-title"]}>
            {formatMessage({ id: "Frequency.Optimization" })}
          </div>
          <Button
            tooltip={disableOptimizeNow.tooltip}
            variant="secondary"
            onClick={() => {
              methods.setValue("advancedConfig", {
                ...initialValues.advancedConfig,
              });
              optimizeNowModal.open();
            }}
            disabled={disableOptimizeNowButton}
          >
            {formatMessage({ id: "Optimize.Once" })}
          </Button>
        </div>
        <div className={classes["optimization-schedule"]}>
          {!loading && (
            <Schedule
              cron={cron}
              scheduleId={scheduleId}
              catalogId={catalogId}
              setMessage={setMessage}
              setShowForm={setShowForm}
              showForm={showForm}
              disabled={disableSchedule}
            />
          )}
          {showForm && (
            <OptimizationForm
              tableId={tableId}
              reference={reference}
              catalogId={catalogId}
              setShowForm={setShowForm}
              initialValues={initialValues}
              cron={cron}
              setCron={setCron}
              scheduleId={scheduleId}
              setMessage={setMessage}
            />
          )}
        </div>
      </div>
      <ModalContainer {...optimizeNowModal}>
        <DialogContent
          title="Advanced Optimization Settings"
          actions={
            <>
              <Button
                disabled={methods.formState.isSubmitting}
                variant="secondary"
                onClick={optimizeNowModal.close}
              >
                {formatMessage({ id: "Common.Cancel" })}
              </Button>
              <Button
                variant="primary"
                type="button"
                pending={methods.formState.isSubmitting}
                disabled={
                  !methods.formState.isValid || methods.formState.isValidating
                }
                onClick={methods.handleSubmit(onSubmitForOptimizeNow)}
              >
                {formatMessage({ id: "Optimize.Now" })}
              </Button>
            </>
          }
        >
          <FormProvider {...methods}>
            <AdvancedConfig showTitle={false} />
          </FormProvider>
        </DialogContent>
      </ModalContainer>
    </div>
  );
};
