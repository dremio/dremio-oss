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
import { useIntl } from "react-intl";
//@ts-ignore
import { Button } from "dremio-ui-lib/components";
import * as classes from "./FormSubmit.module.less";
import { useFormContext } from "react-hook-form";

export const FormSubmit = ({
  onCancel,
  disabled,
}: {
  onCancel: () => void;
  disabled: boolean;
}) => {
  const { formatMessage } = useIntl();
  const {
    formState: { isValid, isValidating, isSubmitting },
  } = useFormContext();

  return (
    <div className={classes["form-actions"]}>
      <Button
        type="submit"
        variant="primary"
        disabled={!isValid || isValidating || disabled}
        pending={isSubmitting}
      >
        {formatMessage({ id: "Common.Save" })}
      </Button>
      <Button
        variant="secondary"
        onClick={onCancel}
        disabled={isSubmitting || disabled}
      >
        {formatMessage({ id: "Common.Cancel" })}
      </Button>
    </div>
  );
};
