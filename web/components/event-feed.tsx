"use client";

import { useMemo, useState } from "react";

import type { TimelineCategory, TimelineEvent } from "@/lib/types";
import { formatAgo, formatClock } from "@/lib/format";

interface EventFeedProps {
  events: TimelineEvent[];
  onClear: () => void;
}

const FILTERS: Array<{ id: "all" | TimelineCategory; label: string }> = [
  { id: "all", label: "all" },
  { id: "election", label: "election" },
  { id: "replication", label: "replication" },
  { id: "client", label: "client" },
  { id: "lifecycle", label: "lifecycle" },
];

const CATEGORY_GLYPH: Record<TimelineCategory, string> = {
  election: "★",
  replication: "⇄",
  client: "▶",
  lifecycle: "●",
};

export function EventFeed({ events, onClear }: EventFeedProps) {
  const [filter, setFilter] = useState<"all" | TimelineCategory>("all");

  const counts = useMemo(() => {
    const result: Record<string, number> = { all: events.length };
    for (const event of events) {
      result[event.category] = (result[event.category] ?? 0) + 1;
    }
    return result;
  }, [events]);

  const visible = useMemo(
    () =>
      (filter === "all"
        ? events
        : events.filter((event) => event.category === filter)
      ).slice(0, 120),
    [events, filter],
  );

  return (
    <aside className="panel event-panel" aria-labelledby="events-title">
      <div className="panel-head event-head">
        <div>
          <h2 id="events-title" className="panel-title">
            Consensus timeline
          </h2>
          <p className="panel-subtitle">
            Node events plus state diffs, newest first.
          </p>
        </div>
        <button
          type="button"
          className="btn btn-ghost btn-tiny"
          onClick={onClear}
          disabled={events.length === 0}
        >
          clear
        </button>
      </div>

      <div className="event-filters" role="tablist" aria-label="Event filters">
        {FILTERS.map((item) => (
          <button
            key={item.id}
            type="button"
            role="tab"
            aria-selected={filter === item.id}
            className={`filter-chip ${filter === item.id ? "active" : ""} cat-${item.id}`}
            onClick={() => setFilter(item.id)}
          >
            {item.label}
            <span className="filter-count">{counts[item.id] ?? 0}</span>
          </button>
        ))}
      </div>

      {visible.length === 0 ? (
        <div className="empty-state event-empty">
          <strong>
            {filter === "all" ? "Waiting for action." : `No ${filter} events yet.`}
          </strong>
          <span>
            {filter === "election"
              ? "No election events yet. This cluster runs with fixed roles, so N1 stays leader until its process stops."
              : "Write an entry, kill a follower, or restart a process to see the timeline fill up."}
          </span>
        </div>
      ) : (
        <ol className="event-list">
          {visible.map((event) => (
            <li
              key={event.id}
              className={`event-row cat-${event.category}`}
            >
              <span className="event-rail" aria-hidden>
                {CATEGORY_GLYPH[event.category]}
              </span>
              <div className="event-main">
                <div className="event-topline">
                  <span className="event-node">N{event.nodeId}</span>
                  <span className="event-type">{event.type}</span>
                  {event.term !== undefined && (
                    <span className="event-term">term {event.term}</span>
                  )}
                  <span className="event-time" title={formatAgo(event.time)}>
                    {formatClock(event.time)}
                  </span>
                </div>
                <p className="event-title">{event.title}</p>
                {event.detail && (
                  <p className="event-detail mono">{event.detail}</p>
                )}
              </div>
            </li>
          ))}
        </ol>
      )}

      <div className="event-foot">
        <span>Showing {visible.length} of {events.length}</span>
        <span className="dot-sep">·</span>
        <span>derived from /state every 750 ms</span>
      </div>
    </aside>
  );
}
