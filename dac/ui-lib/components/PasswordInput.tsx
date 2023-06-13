//@ts-nocheck
import { forwardRef, useState } from "react";
import { Input } from "./Input";
export const PasswordInput = forwardRef((props, ref) => {
  const { ...rest } = props;
  const [revealed, setRevealed] = useState(false);
  const handleReveal = (e) => {
    e.stopPropagation();
    e.preventDefault();
    setRevealed((x) => !x);
  };
  return (
    <Input
      ref={ref}
      {...rest}
      type={revealed ? "text" : "password"}
      suffix={
        <>
          {revealed ? (
            <button
              className="dremio-icon-button m-0"
              onClick={handleReveal}
              type="button"
            >
              <dremio-icon name="sql-editor/panel-hide"></dremio-icon>
            </button>
          ) : (
            <button
              className="dremio-icon-button m-0"
              onClick={handleReveal}
              type="button"
            >
              <dremio-icon name="sql-editor/panel-show"></dremio-icon>
            </button>
          )}
        </>
      }
    />
  );
});
