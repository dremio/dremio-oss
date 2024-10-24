//@ts-nocheck
import clsx from "clsx";
import * as React from "react";
import { createPortal } from "react-dom";
import { CSSTransition } from "react-transition-group";
import {
  type Placement,
  useFloating,
  useInteractions,
  useHover,
  useFocus,
  useRole,
  flip,
  autoUpdate,
  offset,
  shift,
  arrow,
  safePolygon,
  useClick,
  useDismiss,
} from "@floating-ui/react";
import mergeRefs from "react-merge-refs";

type PopoverProps = {
  /**
   * Render prop function allows you to customize where the tooltip content is rendered,
   * otherwise it defaults to a sibling of the hover target.
   */
  children: JSX.Element | ((popoverContent: JSX.Element) => JSX.Element);
  className?: string;
  content:
    | (JSX.Element | string)
    | ((opts: { close: () => void }) => JSX.Element | string);
  placement: Placement;
  portal?: boolean;
  delay?: number;
  dismissable?: boolean;
  /**
   * Called when the popover is closed
   */
  onClose?: () => void;

  /**
   * Called when the popover is opened
   */
  onOpen?: () => void;
  showArrow?: boolean;
  mode: "click" | "hover";

  role:
    | "dialog"
    | "alertdialog"
    | "tooltip"
    | "menu"
    | "listbox"
    | "grid"
    | "tree";
};

export const Popover = (props: PopoverProps) => {
  const arrowElRef = React.useRef(null);
  const [open, setOpen] = React.useState(false);
  const {
    children,
    className,
    content,
    delay = 125,
    dismissable,
    mode,
    placement,
    portal = false,
    role,
    showArrow = false,
    onClose,
    onOpen,
  } = props;

  const handleOpenChange = (isOpen: boolean) => {
    setOpen(isOpen);

    if (!isOpen) {
      onClose?.();
    }

    if (isOpen) {
      onOpen?.();
    }
  };

  const { x, y, context, refs, strategy, middlewareData } = useFloating({
    middleware: [
      offset(8),
      flip(),
      shift({ padding: 8 }),
      arrow({ element: arrowElRef }),
    ],
    onOpenChange: handleOpenChange,
    open,
    placement,
    strategy: "absolute",
    whileElementsMounted: autoUpdate,
  });

  const hoverHooks = [
    useHover(context, {
      restMs: delay,
      handleClose: safePolygon({
        buffer: 2,
      }),
    }),
    useFocus(context),
  ];

  const clickHooks = [useClick(context), dismissable && useDismiss(context)];

  const { getReferenceProps, getFloatingProps } = useInteractions([
    ...(mode === "hover" ? hoverHooks : []),
    ...(mode === "click" ? clickHooks : []),
    useRole(context, { role }),
  ]);

  const ref = React.useMemo(
    () => mergeRefs([refs.setReference, (children as any).ref]),
    [refs.setReference, children],
  );

  const staticSide = (
    {
      top: "bottom",
      right: "left",
      bottom: "top",
      left: "right",
    } as const
  )[placement.split("-")[0]];

  const tooltipContent = (
    <CSSTransition
      appear
      classNames="popover"
      in={open}
      //@ts-ignore
      addEndListener={(node, done) =>
        node.addEventListener("transitionend", done, false) as any
      }
      mountOnEnter
      unmountOnExit
    >
      <div
        className={clsx("popover", `popover--${staticSide}`, className)}
        {...getFloatingProps({
          ref: refs.setFloating,
          style: {
            position: strategy,
            top: y ?? 0,
            left: x ?? 0,
          },
        })}
      >
        {typeof content === "function"
          ? content({ close: () => setOpen(false) })
          : content}
        {showArrow && (
          <div
            className="popover__arrow"
            ref={arrowElRef}
            style={{
              left: middlewareData.arrow?.x,
              top: middlewareData.arrow?.y,
              [staticSide as any]: "calc(var(popover--arrow--size) * -0.5)",
            }}
          />
        )}
      </div>
    </CSSTransition>
  );

  if (typeof children === "function") {
    const childrenResult = children(tooltipContent);
    return React.cloneElement(
      children(tooltipContent),
      getReferenceProps({ ref, ...childrenResult.props }),
    );
  }

  return (
    <>
      {React.cloneElement(
        children,
        getReferenceProps({ ref, ...children.props }),
      )}
      {portal ? createPortal(tooltipContent, document.body!) : tooltipContent}
    </>
  );
};
