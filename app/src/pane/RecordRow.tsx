import type { CSSProperties } from "react";
import { activateRecord, markViewed, type Event } from "../api";

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

export function relativeTime(iso: string, now: number = Date.now()): string {
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return "";
  const diff = Math.max(0, now - then);
  if (diff < MINUTE) return "now";
  if (diff < HOUR) return `${Math.floor(diff / MINUTE)}m`;
  if (diff < DAY) return `${Math.floor(diff / HOUR)}h`;
  // Calendar-day difference in local time, so "yesterday" means the previous date.
  const startOf = (t: number) => {
    const d = new Date(t);
    return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  };
  const days = Math.round((startOf(now) - startOf(then)) / DAY);
  return days <= 1 ? "yesterday" : `${days}d`;
}

export function subtitleParts(event: Event, now?: number): string[] {
  const subject = event.pr
    ? `!${event.pr.id}`
    : event.branch
      ? event.branch.shortName
      : event.run
        ? event.run.definition
        : "";
  return [
    event.repo?.name ?? "",
    subject,
    event.actor.displayName,
    relativeTime(event.occurredUtc, now),
  ].filter((p) => p !== "");
}

const ellipsis: CSSProperties = {
  overflow: "hidden",
  whiteSpace: "nowrap",
  textOverflow: "ellipsis",
};

const rowButton: CSSProperties = {
  display: "flex",
  alignItems: "stretch",
  flex: 1,
  minWidth: 0,
  minHeight: 36,
  padding: 0,
  font: "inherit",
  color: "var(--text)",
  textAlign: "left",
  background: "transparent",
  border: 0,
  cursor: "pointer",
};

const dismissButton: CSSProperties = {
  width: 28,
  padding: 0,
  font: "inherit",
  color: "var(--muted)",
  background: "transparent",
  border: 0,
  cursor: "pointer",
};

interface Props {
  event: Event;
  onError: (e: unknown) => void;
}

export function RecordRow({ event, onError }: Props) {
  return (
    <div
      className="row-wrap"
      style={{ display: "flex", borderBottom: "1px solid var(--border)" }}
    >
      <button
        type="button"
        style={rowButton}
        onClick={() => activateRecord(event.id).catch(onError)}
        data-testid="record-row"
      >
        <span
          aria-hidden="true"
          style={{
            width: 3,
            flexShrink: 0,
            background: `var(--sev-${event.severity})`,
          }}
        />
        <span style={{ flex: 1, minWidth: 0, padding: "3px 8px" }}>
          <span
            style={{
              ...ellipsis,
              display: "flex",
              alignItems: "center",
              gap: 6,
            }}
          >
            <span style={ellipsis}>{event.title}</span>
            {event.test && <span className="badge">TEST</span>}
          </span>
          <span
            style={{
              ...ellipsis,
              display: "block",
              fontSize: 11,
              color: "var(--muted)",
            }}
          >
            {subtitleParts(event).join(" · ")}
          </span>
        </span>
      </button>
      <button
        type="button"
        style={dismissButton}
        aria-label={`Dismiss ${event.title}`}
        onClick={(e) => {
          e.stopPropagation();
          markViewed([event.id]).catch(onError);
        }}
      >
        ×
      </button>
    </div>
  );
}
