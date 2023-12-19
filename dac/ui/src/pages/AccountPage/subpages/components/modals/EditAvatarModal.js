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
import { Component } from "react";
import PropTypes from "prop-types";
import Avatars from "components/Avatar/Avatars";

import Modal from "components/Modals/Modal";
import { formLabel } from "uiTheme/radium/typography";
import { secondary } from "uiTheme/radium/buttons";
import { FieldWithError, TextField } from "components/Fields";
import { ModalForm, FormBody, modalFormProps } from "components/Forms";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";
import clsx from "clsx";

export default class EditAvatarModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    generalUserAction: PropTypes.func,
    pathname: PropTypes.string,
    query: PropTypes.object,
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { isOpen, hide } = this.props;
    return (
      <Modal
        title={laDeprecated("Edit Avatar")}
        size="small"
        isOpen={isOpen}
        hide={hide}
      >
        <ModalForm {...modalFormProps(this.props)}>
          <FormBody style={styles.main}>
            <div style={formLabel}>{laDeprecated("Browse")}</div>
            <Avatars />
            <div style={styles.uploadField}>
              <div style={formLabel}>Or upload your own image</div>
              <div style={{ display: "flex" }}>
                <FieldWithError>
                  <TextField style={{ cursor: "pointer" }} />
                </FieldWithError>
                <button
                  key="browse"
                  style={styles.button}
                  className={clsx(
                    classes["buttonPsuedoClasses"],
                    classes["secondaryButtonPsuedoClasses"]
                  )}
                >
                  <span>Browse</span>
                </button>
              </div>
            </div>
          </FormBody>
        </ModalForm>
      </Modal>
    );
  }
}

const styles = {
  main: {
    width: 540,
    margin: "0 auto",
  },
  uploadField: {
    marginTop: 20,
  },
  button: {
    ...secondary,
    marginLeft: -10,
  },
};
