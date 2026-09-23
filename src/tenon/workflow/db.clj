(ns tenon.workflow.db
  "The storage layer abstracted into the Storage protocol")

(defprotocol Storage
  (init-db! [this]
    "Ensures the required schema and tables exist in the storage. Returns this.")

  (insert-workflow!
    [this id wf-def arguments-edn-str parent-workflow-id metadata-edn-str]
    "Inserts a workflow row, unless one already exists for this exact
     (wf_def, arguments) pair, in which case nothing is inserted.")

  (append-event! [this workflow-id expected-latest-event-id state payload-edn-str]
    "Appends an event to workflow-id, but only if the id of its latest event
     is still expected-latest-event-id (nil: it has no events yet) - a
     compare-and-set, so of several concurrent writers that read the same
     latest event, only one appends. Returns the new event's id, or nil if
     the latest event had changed and nothing was appended.")

  (current-time [this]
    "The storage's own current UTC time, as a java.time.LocalDateTime - the
     clock that also supplies workflow_events.changed_at, so the two can be
     compared without mixing in the JVM's clock.")

  (get-workflow [this id])

  (latest-event [this workflow-id]
    "The most recent workflow_events row of workflow-id, or nil. Its id is
     what append-event! expects, and its changed_at is a UTC
     java.time.LocalDateTime.")

  (find-by-wf-def-and-arguments [this wf-def arguments-edn-str])

  (top-level-workflows
    [this]
    [this filters]
    "Workflow rows (every column), each annotated with created_at (its STARTED event's
     timestamp) and state (its latest event's state), newest first.
     filters is an optional map of {:state s :wf-def w :top-level-only?
     :limit n :before {:created-at ... :id ...}}.

     :state and :wf-def, when given, restrict to rows matching exactly;
     omitting a key (or the whole map) leaves that dimension unfiltered.
     :top-level-only? defaults to true (only parent_workflow_id IS NULL
     rows, i.e. hides sub-workflows nested under another invocation); pass
     false to include every workflow regardless of nesting.

     :limit caps the number of rows returned - pagination is timestamp
     seek-based, not OFFSET-based (which gets slower, and can skip/repeat
     rows under concurrent writes, as the offset grows): pass the last
     row of the previous page as :before (its :created_at and :id, the
     tiebreaker for rows sharing a created_at, both required together)
     to fetch the rows immediately after it in the newest-first order.
     Callers asking for n rows and wanting to know whether a further page
     exists should request :limit (inc n) and check whether they got
     more than n back, trimming to n before display.")

  (top-level-wf-defs [this top-level-only?]
    "Distinct wf_def values, alphabetical - the option list for a
     workflow-name filter dropdown. top-level-only? true restricts to
     wf_defs seen among top-level workflows; false includes every
     workflow regardless of nesting.")

  (full-timeline [this workflow-id]
    "Every workflow_events row belonging to workflow-id itself and every
     workflow nested under it at any depth (sub-workflows, their own
     sub-workflows, and so on), interleaved into a single chronological
     sequence. Each row also carries workflow_id, wf_def, and depth (0 for
     workflow-id itself, 1 for a direct sub-workflow, and so on), so it
     can be attributed to whichever invocation it actually came from and
     indented to show its nesting level."))
