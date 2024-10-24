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
import { useEffect, useRef, useState } from "react";
import {
  ModalContainer,
  DialogContent,
  Button,
  IconButton,
  Input,
  Label,
} from "dremio-ui-lib/components";
import { connect } from "react-redux";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  replaceScript,
  type Script,
  type UpdatedScript,
} from "dremio-ui-common/sonar/scripts/endpoints/replaceScript.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import { useForm } from "react-hook-form";
import Message from "#oss/components/Message";
import { fetchAllAndMineScripts } from "#oss/components/SQLScripts/sqlScriptsUtils";
import { fetchScripts, setActiveScript } from "#oss/actions/resources/scripts";
import { store } from "#oss/store/store";

import * as classes from "./SQLScriptRenameDialog.module.less";
import {
  stripTemporaryPrefix,
  isTemporaryScriptName,
} from "dremio-ui-common/sonar/SqlRunnerSession/utilities/temporaryTabs.js";

type SQLScriptRenameDialogProps = {
  isOpen: boolean;
  onCancel: () => void;
  script: Script;
  fetchScripts: any;
};

const SQLScriptRenameDialog = (
  props: SQLScriptRenameDialogProps,
): React.ReactElement => {
  const { t } = getIntlContext();
  const {
    isOpen,
    onCancel,
    script: { content, context, id, name, jobIds },
    fetchScripts,
  } = props;
  const [error, setError] = useState(undefined);
  const methods = useForm<any>({
    mode: "onChange",
    defaultValues: { scriptName: name },
  });
  const isTemporaryScript = isTemporaryScriptName(name);
  if (isTemporaryScript) {
    setTimeout(() => {
      methods.setValue("scriptName", stripTemporaryPrefix(name), {
        shouldDirty: true,
        shouldTouch: true,
      });
    });
  }

  const {
    formState: { isDirty, isSubmitting },
    register,
  } = methods;

  const inputRef = useRef<HTMLInputElement | null>(null);

  const onSubmit = async ({ scriptName }: { scriptName: string }) => {
    try {
      const payload: UpdatedScript = {
        name: scriptName,
        content,
        context,
        description: "",
        jobIds,
      };
      const script = await replaceScript(id, payload);
      store.dispatch(setActiveScript({ script }));
      ScriptsResource.fetch();
      fetchAllAndMineScripts(fetchScripts, "");
      onCancel();
    } catch (e: any) {
      setError(e?.responseBody?.errorMessage);
    }
  };

  const { ref: assignInputRef, ...scriptNameField } = register("scriptName");

  useEffect(() => {
    if (!inputRef.current) return;
    inputRef.current.select();
  }, []);

  return (
    <ModalContainer open={() => {}} isOpen={isOpen} close={onCancel}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>
        <DialogContent
          title={isTemporaryScript ? "Save" : t("Script.Rename")}
          className={classes["dialog"]}
          error={
            error ? <Message messageType="error" message={error} /> : undefined
          }
          toolbar={
            <IconButton onClick={onCancel} aria-label="Close">
              <dremio-icon
                name="interface/close-big"
                alt=""
                style={{ cursor: "pointer" }}
              />
            </IconButton>
          }
          actions={
            <>
              <Button
                onClick={onCancel}
                variant="secondary"
                className="mr-05"
                disabled={isSubmitting}
              >
                {t("Common.Actions.Cancel")}
              </Button>
              <Button
                variant="primary"
                type="submit"
                disabled={!isDirty}
                pending={isSubmitting}
              >
                {t("Common.Actions.Save")}
              </Button>
            </>
          }
        >
          <div className={classes["dialog__description"]}>
            <Label value={t("Script.Name")} classes={{ root: "mb-1" }} />
            <Input
              {...scriptNameField}
              ref={(cur) => {
                assignInputRef(cur);
                inputRef.current = cur;
              }}
            />
          </div>
        </DialogContent>
      </form>
    </ModalContainer>
  );
};

export default connect(null, { fetchScripts })(SQLScriptRenameDialog);
