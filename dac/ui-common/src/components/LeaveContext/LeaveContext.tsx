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
import { getIntlContext } from "../../contexts/IntlContext";
import {
  Button,
  DialogContent,
  ModalContainer,
} from "dremio-ui-lib/components";
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

export const leaveContext = createContext({
  setNextLocation: (nextLocation: any) => {},
  stay: () => {},
  leave: () => {},
  isAllowed: (nextLocation: any) => {},
  isOpen: false,
});

export const useLeaveModal = ({ router, route, isDirty }: any) => {
  const leaveModal = useContext(leaveContext);
  useEffect(() => {
    if (!isDirty) return;

    // setRouteLeaveHook returns a remove function, run it during effect cleanup
    return router.setRouteLeaveHook(route, (nextLocation: any) => {
      const allowed = leaveModal.isAllowed(nextLocation);
      leaveModal.setNextLocation(nextLocation);
      return allowed;
    });
  }, [leaveModal, isDirty]);
};

type LeaveContextProviderProps = {
  children: JSX.Element;
  setNextLocation: (nextLocation: any) => void;
};

export const LeaveContextProvider = (props: LeaveContextProviderProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const nextRequestedLocation = useRef<any>(null);
  const acceptedKey = useRef(null);
  const { t } = getIntlContext();

  const leave = () => {
    setIsOpen(false);
    acceptedKey.current = nextRequestedLocation.current;
    props.setNextLocation(nextRequestedLocation.current); //Use browser history.push in consuming code
    nextRequestedLocation.current = null;
    acceptedKey.current = null;
  };

  const contextValue = useMemo(
    () => ({
      isOpen,
      isAllowed: (nextLocation: any) => {
        //@ts-ignore
        return nextLocation.pathname === acceptedKey.current?.pathname;
      },
      setNextLocation: (nextLocation: any) => {
        if (nextRequestedLocation.current) {
          return;
        }
        nextRequestedLocation.current = nextLocation;
        setIsOpen(true);
      },
      stay: () => {
        setIsOpen(false);
        nextRequestedLocation.current = null;
      },
      leave,
    }),
    [isOpen],
  );

  return (
    <leaveContext.Provider value={contextValue}>
      {props.children}
      <ModalContainer isOpen={isOpen} open={() => {}} close={contextValue.stay}>
        <DialogContent
          title={t("Common.RouteLeaveDialog.Title")}
          actions={
            <div className="dremio-button-group">
              <Button variant="primary" onClick={contextValue.leave}>
                {t("Common.RouteLeaveDialog.Actions.Leave")}
              </Button>
              <Button variant="secondary" onClick={contextValue.stay}>
                {t("Common.RouteLeaveDialog.Actions.Stay")}
              </Button>
            </div>
          }
        >
          <>{t("Common.RouteLeaveDialog.Message")}</>
        </DialogContent>
      </ModalContainer>
    </leaveContext.Provider>
  );
};
