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

import { Button, DialogContent } from "dremio-ui-lib/components";
import { type FC } from "react";

export const TemporaryTabConfirmDeleteDialog: FC<{
  onCancel: () => void;
  onDiscard: () => void;
  onSave: () => void;
}> = (props) => {
  return (
    <DialogContent
      title="Save script before closing?"
      actions={
        <div className="dremio-button-group">
          <Button variant="secondary" onClick={() => props.onCancel()}>
            Cancel
          </Button>
          <Button variant="secondary" onClick={() => props.onDiscard()}>
            Discard
          </Button>
          <Button variant="primary" onClick={() => props.onSave()}>
            Save
          </Button>
        </div>
      }
    >
      <div className="dremio-prose">
        <p>
          This tab has not been saved as a script and will be deleted when it is
          closed.
        </p>
        <p>Do you want to save the contents of this tab as a script?</p>
      </div>
    </DialogContent>
  );
};
