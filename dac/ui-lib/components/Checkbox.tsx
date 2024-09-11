import * as React from "react";

type CheckboxProps = {
  className?: string;
  label?: string | JSX.Element;
  labelClassName?: string;
  onClick?: any;
  value?: string;
  checked?: boolean;
  style?: Record<string, any>;
};

export const Checkbox = (props: CheckboxProps) => {
  const { label, className, ...inputProps } = props;
  return (
    <label className={className}>
      <input {...inputProps} className="form-control" type="checkbox" />
      {label}
    </label>
  );
};
