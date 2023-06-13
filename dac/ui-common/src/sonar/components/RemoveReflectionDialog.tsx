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
import { DialogContent, Button } from "dremio-ui-lib/components";
import { getIntlContext } from "../../contexts/IntlContext";

type RemoveReflectionDialogProps = {
  name: string;
  onAccept: () => Promise<void> | void;
  onCancel: () => void;
  pending: boolean;
  error?: any;
  renderError?: any;
};

export const RemoveReflectionDialog = (props: RemoveReflectionDialogProps) => {
  const { t } = getIntlContext();
  return (
    <DialogContent
      actions={
        <div className="dremio-button-group">
          <Button
            variant="secondary"
            onClick={() => props.onCancel()}
            disabled={props.pending}
          >
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={() => props.onAccept()}
            disabled={props.pending || !!props.error}
            pending={props.pending}
          >
            Remove
          </Button>
        </div>
      }
      title={t("Sonar.Reflection.Remove")}
    >
      {props.renderError?.()}
      <p
        className="dremio-prose"
        style={{ marginBottom: "var(--dremio--spacing--2)" }}
      >
        {t("Sonar.Reflection.Remove.Prompt", { reflectionName: props.name })}
      </p>
    </DialogContent>
  );
};
