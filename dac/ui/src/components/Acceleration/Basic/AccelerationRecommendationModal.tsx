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

import { intl } from "#oss/utils/intl";
import {
  Button,
  DialogContent,
  IconButton,
  ModalContainer,
} from "dremio-ui-lib/components";

type Props = {
  isOpen: boolean;
  handleCloseDialog: () => void;
  fetchRecommendations: (type: "agg" | "raw") => void;
};

const AccelerationRecommendationModal = ({
  isOpen,
  handleCloseDialog,
  fetchRecommendations,
}: Props) => {
  return (
    <ModalContainer
      open={() => {}}
      isOpen={isOpen}
      close={handleCloseDialog}
      style={{ width: 600 }}
    >
      <DialogContent
        title={intl.formatMessage({
          id: "Reflection.Details.Generate.Title",
        })}
        actions={
          <>
            <Button
              onClick={handleCloseDialog}
              variant="secondary"
              className="margin-right"
            >
              {intl.formatMessage({ id: "Common.Cancel" })}
            </Button>
            <Button
              variant="primary"
              onClick={() => {
                fetchRecommendations("agg");
                handleCloseDialog();
              }}
            >
              {intl.formatMessage({ id: "Common.Continue" })}
            </Button>
          </>
        }
        toolbar={
          <IconButton aria-label="Close" onClick={handleCloseDialog}>
            <dremio-icon name="interface/close-big" />
          </IconButton>
        }
      >
        <div className="dremio-prose" style={{ height: 116 }}>
          {intl.formatMessage({
            id: "Reflection.Details.Generate.Modal.Message",
          })}
        </div>
      </DialogContent>
    </ModalContainer>
  );
};

export default AccelerationRecommendationModal;
