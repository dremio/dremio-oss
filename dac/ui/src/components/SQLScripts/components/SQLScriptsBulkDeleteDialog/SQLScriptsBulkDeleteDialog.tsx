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

import { addNotification } from "#oss/actions/notification";
import { deleteScripts } from "#oss/exports/endpoints/Scripts/deleteScripts";
import { Button, DialogContent } from "dremio-ui-lib/components";
import { useEffect, useMemo, useState } from "react";
import { useDispatch } from "react-redux";
import { usePromise } from "react-smart-promise";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import sentryUtil from "#oss/utils/sentryUtil";

export const SQLScriptsBulkDeleteDialog = (props: {
  onCancel: () => void;
  onSuccess: (errorIds: string[]) => Promise<any>;
  scripts: Record<string, any>[];
}) => {
  const { t } = getIntlContext();
  const dispatch = useDispatch();
  const { onSuccess, onCancel, scripts } = props;
  const [submittedData, setSubmittedData] = useState<{ ids: string[] } | null>(
    null,
  );

  const [, data, status] = usePromise(
    useMemo(() => {
      if (!submittedData) {
        return null;
      }
      return () => {
        return deleteScripts(submittedData);
      };
    }, [submittedData]),
  );

  const submitPending = status === "PENDING";
  const errorIds: string[] = useMemo(
    () =>
      Object.values((data as Record<string, string[]>) || {}).reduce(
        (acc, cur) => {
          return [...acc, ...cur];
        },
        [],
      ),
    [data],
  );

  useEffect(() => {
    if (submittedData && (status === "SUCCESS" || status === "ERROR")) {
      setSubmittedData(null);
      onSuccess(
        submittedData.ids.filter((cur) => !errorIds.includes(cur)),
      ).catch((e) => {
        sentryUtil.logException(e);
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status, data, submittedData]);

  useEffect(() => {
    if (status === "ERROR" || errorIds.length > 0) {
      dispatch(
        addNotification(t("Sonar.Scripts.BulkDelete.GenericError"), "error"),
      );
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [errorIds, status]);

  return (
    <DialogContent
      icon={<dremio-icon name="interface/warning" alt="Warning" />}
      title={t("Sonar.Scripts.BulkDelete.Title")}
      actions={
        <>
          <Button
            variant="secondary"
            onClick={onCancel}
            disabled={submitPending}
          >
            {t("Common.Actions.Cancel")}
          </Button>
          <Button
            variant="primary-danger"
            type="submit"
            onClick={() => {
              setSubmittedData({
                ids: scripts.map((script) => script.id),
              });
            }}
            pending={submitPending}
            success={status === "SUCCESS"}
          >
            {t("Common.Actions.Delete")}
          </Button>
        </>
      }
    >
      <div className="dremioDialog__paper">
        {t("Sonar.Scripts.BulkDelete.Message", { br: () => <br></br> })}
        <br />
        <br />
      </div>
    </DialogContent>
  );
};
