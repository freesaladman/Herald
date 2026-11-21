import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";
import {
  onToasts,
  toastClicked,
  toastHover,
  toastResize,
  type ToastCard,
} from "../api";

const ellipsis: CSSProperties = {
  overflow: "hidden",
  whiteSpace: "nowrap",
  textOverflow: "ellipsis",
};

const stackStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  justifyContent: "flex-end",
  gap: 4,
  width: "100%",
};

const cardStyle: CSSProperties = {
  display: "flex",
  alignItems: "stretch",
  height: 56,
  background: "var(--bg)",
  border: "1px solid var(--border)",
  borderRadius: 6,
  overflow: "hidden",
  cursor: "pointer",
  color: "var(--text)",
};

const pill: CSSProperties = {
  flex: "none",
  padding: "0 6px",
  borderRadius: 8,
  background: "var(--hover)",
  border: "1px solid var(--border)",
  fontSize: 11,
  color: "var(--muted)",
};

function Card({ card }: { card: ToastCard }) {
  return (
    <div
      role="button"
      data-testid="toast-card"
      style={cardStyle}
      onClick={() => void toastClicked()}
    >
      <div
        style={{
          width: 4,
          flex: "none",
          background: `var(--sev-${card.severity})`,
        }}
      />
      <div
        style={{
          flex: 1,
          minWidth: 0,
          padding: "0 10px",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ ...ellipsis, flex: 1, fontWeight: 600 }}>
            {card.title}
          </span>
          {card.test && <span style={pill}>TEST</span>}
          {card.count > 1 && <span style={pill}>{card.count}</span>}
        </div>
        {card.subtitle && (
          <div style={{ ...ellipsis, color: "var(--muted)", fontSize: 12 }}>
            {card.subtitle}
          </div>
        )}
      </div>
    </div>
  );
}

export function Toast() {
  const [cards, setCards] = useState<ToastCard[]>([]);
  const stackRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const unlisten = onToasts((next) => setCards(next));
    return () => {
      void unlisten.then((fn) => fn());
    };
  }, []);

  useLayoutEffect(() => {
    const el = stackRef.current;
    if (!el) return;
    const report = () =>
      void toastResize(cards.length === 0 ? 0 : el.offsetHeight);
    report();
    const observer = new ResizeObserver(report);
    observer.observe(el);
    return () => observer.disconnect();
  }, [cards]);

  return (
    <div
      className="toast-root"
      onMouseEnter={() => void toastHover(true)}
      onMouseLeave={() => void toastHover(false)}
    >
      <div ref={stackRef} style={stackStyle}>
        {cards.map((c) => (
          <Card key={c.cardId} card={c} />
        ))}
      </div>
    </div>
  );
}
