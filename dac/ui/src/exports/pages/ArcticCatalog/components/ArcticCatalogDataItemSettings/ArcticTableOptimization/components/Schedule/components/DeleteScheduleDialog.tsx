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
import { Button, DialogContent } from "dremio-ui-lib/components";

export const DeleteScheduleDialog = ({
  onCancel,
  onSubmit,
  pending,
}: {
  onCancel: () => void;
  onSubmit: () => void;
  pending: boolean;
}) => {
  const { formatMessage } = useIntl();
  return (
    <DialogContent
      title={
        <>
          <dremio-icon name="interface/warning" /> Delete Schedule?
        </>
      }
      actions={
        <>
          <Button variant="secondary" onClick={onCancel} disabled={pending}>
            {formatMessage({ id: "Common.Cancel" })}
          </Button>
          <Button
            variant="primary-danger"
            type="button"
            onClick={onSubmit}
            pending={pending}
          >
            {formatMessage({ id: "Common.Delete" })}
          </Button>
        </>
      }
    >
      <div style={{ minHeight: "175px" }} className="dremio-prose">
        Deleting the schedule will stop any future optimization runs that are
        tied to it
      </div>
    </DialogContent>
  );
};
