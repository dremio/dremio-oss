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

import { useCallback, useEffect, useRef, useState } from "react";
import * as monaco from "monaco-editor";
import { type SQLError } from "../../../../../../sql/errorDetection/types/SQLError";
import { runErrorDetectionOnWorker } from "../../../../../../sql/worker/client/SQLParsingWorkerClient";
import { type RunErrorDetectionData } from "../../../../../../sql/worker/server/SQLParsingWorker.worker";
import { monacoLogger } from "../../../utilities/monacoLogger";

/**
 * Applies error decorations to a given editor instance. Will also alculate
 * real-time errors if live error detection is enabled and returns a callback
 * wrapping an onChange function to update error decorations on content updates.
 */
export const useSqlErrorDecorations = ({
  editorInstance,
  isLiveDetectionEnabled,
  onChange,
  serverSqlErrors,
}: Partial<{
  editorInstance: monaco.editor.IStandaloneCodeEditor;
  isLiveDetectionEnabled: boolean;
  onChange: (val: string) => unknown;
  serverSqlErrors: SQLError[];
}>) => {
  const modelVersionRef = useRef<number>(-1);
  const [liveErrors, setLiveErrors] = useState<SQLError[]>([]);

  const errorToDecorations = (
    sqlError: SQLError,
  ): monaco.editor.IModelDeltaDecoration => ({
    range: sqlError.range,
    options: {
      hoverMessage: {
        // needs to be encoded or Monaco will filter out certain error messages
        value: encodeURI(sqlError.message as string),
      },
      stickiness:
        monaco.editor.TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges,
      className: "squiggly-error",
      linesDecorationsClassName: "dremio-error-line flex justify-center",
      overviewRuler: {
        // color should match the @monaco-error variable in color-schema.scss
        color: "rgba(255,18,18,0.7)",
        position: monaco.editor.OverviewRulerLane.Left,
      },
    },
  });

  useEffect(() => {
    if (!editorInstance || (!serverSqlErrors?.length && !liveErrors.length)) {
      return;
    }

    const decorationsCollection = editorInstance.createDecorationsCollection(
      serverSqlErrors?.length
        ? serverSqlErrors.map(errorToDecorations)
        : liveErrors.map(errorToDecorations),
    );

    // need to clear decorations before applying new ones or stale decorations will persist
    return () => decorationsCollection.clear();
  }, [editorInstance, liveErrors, serverSqlErrors]);

  const getLiveErrors = useCallback(
    async (modelVersion: number) => {
      modelVersionRef.current = modelVersion;

      const editorModel = editorInstance?.getModel();

      if (!editorModel) {
        return [];
      }

      const data: RunErrorDetectionData = {
        linesContent: editorModel.getLinesContent(),
      };

      // if this returns false, that means there is a newer pending error detection request;
      // we should cancel this one to prioritize the newer one instead
      const isCancellationRequested = () =>
        modelVersion < modelVersionRef.current;

      const syntaxErrors: SQLError[] | null = await runErrorDetectionOnWorker(
        data,
        isCancellationRequested,
      );

      // do not use model after this point because it may have been mutated already with new changes
      if (!syntaxErrors) {
        monacoLogger.debug(
          `Error detection request cancelled for model version: ${modelVersion}`,
        );

        return [];
      }

      return syntaxErrors;
    },
    [editorInstance],
  );

  return useCallback(
    async (val: string) => {
      onChange?.(val);

      if (!isLiveDetectionEnabled) {
        return;
      }

      if (liveErrors.length) {
        // clear any existing live errors before starting the async request to resynchronize them
        setLiveErrors([]);
      }

      const getModelVersion = () => editorInstance?.getModel()?.getVersionId();
      const modelVersion = getModelVersion();

      if (!modelVersion) {
        return;
      }

      const newLiveErrors = await getLiveErrors(modelVersion);

      if (modelVersion === getModelVersion()) {
        if (!newLiveErrors.length && !liveErrors.length) {
          return;
        }

        setLiveErrors(newLiveErrors);
      }
    },
    [
      editorInstance,
      isLiveDetectionEnabled,
      liveErrors.length,
      getLiveErrors,
      onChange,
    ],
  );
};
