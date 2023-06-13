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
import { useState } from "react";
import { useIntl } from "react-intl";
import { getNextCompactionDate } from "../../compactionUtils";
import {
  Button,
  ModalContainer,
  useModalContainer,
  //@ts-ignore
} from "dremio-ui-lib/components";
import clsx from "clsx";
import { DeleteScheduleDialog } from "./components/DeleteScheduleDialog";
import { deleteSchedule } from "@app/exports/endpoints/ArcticCatalogs/Optimization/deleteSchedule";
import { optimizationScheduleResource } from "@app/exports/resources/OptimizationScheduleResource";
import * as classes from "./Schedule.module.less";

export const Schedule = ({
  setShowForm,
  showForm,
  catalogId,
  scheduleId,
  cron,
  disabled,
  setMessage,
}: {
  setShowForm: (arg: boolean) => void;
  showForm: boolean;
  catalogId: string;
  scheduleId: string;
  cron: string;
  disabled: boolean;
  setMessage: (arg: { text: string; type: string }) => void;
}) => {
  const deleteConfirmationModal = useModalContainer();
  const [submitting, setSubmitting] = useState<boolean>(false);
  const { formatMessage } = useIntl();
  const onDelete = async () => {
    setSubmitting(true);
    try {
      deleteSchedule({ catalogId, scheduleId });
      setSubmitting(false);
      optimizationScheduleResource.reset();
      deleteConfirmationModal.close();
      setMessage({
        text: "Schedule was successfully deleted.",
        type: "success",
      });
    } catch (e: any) {
      setMessage({ text: e?.responseBody?.errorMessage, type: "error" });
      deleteConfirmationModal.close();
      setSubmitting(false);
    }
  };
  return (
    <>
      <div className={classes["title"]}>
        {formatMessage({ id: "Optimize.Regularly" })}
      </div>
      <div className={classes["schedule"]}>
        {!showForm ? (
          !scheduleId ? (
            <>
              <span
                className={clsx(
                  classes["schedule-none"],
                  disabled && classes["disabled"]
                )}
              >
                {formatMessage({ id: "No.Schedule.Yet" })}
              </span>
              <Button
                disabled={disabled}
                variant="tertiary"
                onClick={() => setShowForm(true)}
              >
                {formatMessage({ id: "Set.Schedule" })}
              </Button>
            </>
          ) : (
            <>
              <span>
                {formatMessage({ id: "Next.Compaction.Job" })}
                {getNextCompactionDate(cron)}
              </span>
              <Button variant="tertiary" onClick={() => setShowForm(true)}>
                {formatMessage({ id: "Common.Edit" })}
              </Button>
              <Button variant="tertiary" onClick={deleteConfirmationModal.open}>
                {formatMessage({ id: "Common.Delete" })}
              </Button>
            </>
          )
        ) : (
          <p className={classes["next-compaction"]}>
            {formatMessage({ id: "Next.Compaction.Scheduled" })}
            {cron && getNextCompactionDate(cron)}
          </p>
        )}
      </div>
      <ModalContainer {...deleteConfirmationModal}>
        <DeleteScheduleDialog
          onCancel={deleteConfirmationModal.close}
          onSubmit={onDelete}
          pending={submitting}
        />
      </ModalContainer>
    </>
  );
};
