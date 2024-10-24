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

import { useCallback, useEffect, useRef } from "react";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { addNotification } from "#oss/actions/notification";
import { MSG_CLEAR_DELAY_SEC } from "#oss/constants/Constants";
import { downloadDataset } from "#oss/exports/endpoints/Datasets/downloadDataset";
import {
  StartDatasetDownloadParams,
  startDatasetDownload,
} from "#oss/exports/endpoints/Datasets/startDatasetDownload";
import { DownloadStatusResource } from "#oss/exports/resources/AsyncDownloadResource";
import { useResourceSnapshotDeep } from "#oss/exports/utilities/useDeepResourceSnapshot";
import { store } from "#oss/store/store";

const { t } = getIntlContext();

const ONGOING_DOWNLOADS = "ongoingDownloads";

const getOngoingDownloadsFromCache = (): Partial<{
  [key: string]: {
    id: string | undefined;
    format: StartDatasetDownloadParams["format"] | null;
  };
}> => {
  const ongoingDownloads = sessionStorage.getItem(ONGOING_DOWNLOADS) ?? "{}";

  try {
    return JSON.parse(ongoingDownloads);
  } catch (e) {
    console.error(
      "Error trying to parse ongoing downloads from session storage",
    );
    return {};
  }
};

const startDownload = async ({
  jobId,
  format,
  setIsDownloading,
}: {
  jobId: string;
  format: StartDatasetDownloadParams["format"];
  setIsDownloading: (isDownloading: boolean) => void;
}) => {
  setIsDownloading(true);

  try {
    const startResponse = await startDatasetDownload({
      jobId,
      format,
    });

    DownloadStatusResource.start(() => [
      {
        jobId,
        downloadId: startResponse.id,
      },
    ]);

    return startResponse;
  } catch (e: any) {
    store.dispatch(
      addNotification(
        e.responseBody?.errorMessage,
        "error",
        MSG_CLEAR_DELAY_SEC,
      ),
    );

    setIsDownloading(false);
  }
};

const useCompleteDownload = ({
  jobId,
  setIsDownloading,
}: {
  jobId: string;
  setIsDownloading: (isDownloading: boolean) => void;
}) =>
  useCallback(
    async (
      downloadJobId: string | undefined,
      format: StartDatasetDownloadParams["format"] | null,
    ) => {
      if (!downloadJobId || !format) {
        return;
      }

      try {
        DownloadStatusResource.reset();

        const downloadResponse = await downloadDataset({
          jobId,
          downloadId: downloadJobId,
        });

        const downloadUrl = window.URL.createObjectURL(downloadResponse);
        const linkEl = document.createElement("a");
        linkEl.href = downloadUrl;
        linkEl.download = `${jobId}.${format}`.toLocaleLowerCase();
        linkEl.click();

        // delay revoking the ObjectURL for Firefox
        setTimeout(() => {
          window.URL.revokeObjectURL(downloadUrl);
        }, 250);

        store.dispatch(
          addNotification(
            `${jobId} ${t("Sonar.SqlRunner.Download.Complete", {
              type:
                format === "PARQUET"
                  ? format.charAt(0) + format.slice(1).toLowerCase()
                  : format,
            })}`,
            "success",
            MSG_CLEAR_DELAY_SEC,
          ),
        );
      } catch (e: any) {
        store.dispatch(
          addNotification(
            e.responseBody?.errorMessage,
            "error",
            MSG_CLEAR_DELAY_SEC,
          ),
        );
      }

      const ongoingDownloads = getOngoingDownloadsFromCache();
      delete ongoingDownloads[jobId];
      sessionStorage.setItem(
        ONGOING_DOWNLOADS,
        JSON.stringify(ongoingDownloads),
      );

      setIsDownloading(false);
    },
    [jobId, setIsDownloading],
  );

const useFailedDownload = ({
  jobId,
  setIsDownloading,
}: {
  jobId: string;
  setIsDownloading: (isDownloading: boolean) => void;
}) => {
  const [pollSnapshot] = useResourceSnapshotDeep(DownloadStatusResource);

  return useCallback(() => {
    DownloadStatusResource.reset();

    store.dispatch(
      addNotification(
        pollSnapshot?.errors[0].title ?? `Error downloading ${jobId}`,
        "error",
        MSG_CLEAR_DELAY_SEC,
      ),
    );

    setIsDownloading(false);
  }, [jobId, setIsDownloading, pollSnapshot?.errors]);
};

/**
 * @description Takes a jobId and asynchronously downloads job results
 * @returns A function used to start a download
 */
export const useAsyncDownload = ({
  jobId,
  setIsDownloading,
}: {
  jobId: string;
  setIsDownloading: (isDownloading: boolean) => void;
}) => {
  // keeps track of the current download job if any
  const downloadJob = useRef<{
    id: string | undefined;
    format: StartDatasetDownloadParams["format"] | null;
  }>({ id: undefined, format: null });

  const [pollSnapshot, pollError] = useResourceSnapshotDeep(
    DownloadStatusResource,
  );

  // continue / discontinue downloads on tab change
  useEffect(() => {
    // start polling if a download was in progress
    const ongoingDownloads = getOngoingDownloadsFromCache();
    const currentDownload = ongoingDownloads[jobId];

    if (currentDownload?.id) {
      setIsDownloading(true);

      DownloadStatusResource.start(() => [
        {
          jobId,
          downloadId: currentDownload.id,
        },
      ]);

      downloadJob.current = { ...currentDownload };
    }

    // stop polling and save the download in session storage if in progress
    return () => {
      DownloadStatusResource.reset();

      if (downloadJob.current.id) {
        const ongoingDownloads = getOngoingDownloadsFromCache();

        sessionStorage.setItem(
          ONGOING_DOWNLOADS,
          JSON.stringify({
            ...ongoingDownloads,
            [jobId]: downloadJob.current,
          }),
        );

        downloadJob.current = { id: undefined, format: null };
      }

      setIsDownloading(false);
    };
  }, [jobId, setIsDownloading]);

  const completeDownload = useCompleteDownload({ jobId, setIsDownloading });
  const handleFailedDownload = useFailedDownload({ jobId, setIsDownloading });

  // handle download jobs that have reached a terminal state
  useEffect(() => {
    const downloadStatus = pollSnapshot?.status;

    if (downloadStatus === "COMPLETED") {
      completeDownload(downloadJob.current.id, downloadJob.current.format);
      downloadJob.current = { id: undefined, format: null };
    }

    if (downloadStatus === "FAILED") {
      handleFailedDownload();
      downloadJob.current = { id: undefined, format: null };
    }
  }, [pollSnapshot, completeDownload, handleFailedDownload]);

  // handle any errors with the /status API
  useEffect(() => {
    if (pollError) {
      DownloadStatusResource.reset();

      store.dispatch(
        addNotification(
          pollError.responseBody?.errorMessage ??
            "Error getting download status",
          "error",
          MSG_CLEAR_DELAY_SEC,
        ),
      );

      setIsDownloading(false);
      downloadJob.current = { id: undefined, format: null };
    }
  }, [pollError, setIsDownloading]);

  return async (format: StartDatasetDownloadParams["format"]) => {
    const startResponse = await startDownload({
      jobId,
      format,
      setIsDownloading,
    });

    downloadJob.current = { id: startResponse?.id, format };
  };
};
