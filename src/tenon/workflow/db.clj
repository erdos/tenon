(ns tenon.workflow.db
  "The storage layer abstracted into the Storage protocol.

   Every workflow is a single row of the workflow table holding its current
   state. Rows are only ever INSERTed or UPDATEd with compare-and-set
   semantics - on (invocation_id, state), where invocation_id changes on
   every restart - and a trigger copies each replaced state into
   workflow_history. The only history row written directly is REUSED
   (record-reuse!): a caller got an earlier invocation's result instead of
   running it. Every history row carries the parent_invocation_id it was
   recorded under - for REUSED rows, the reusing caller's.

   Timestamps are UTC milliseconds since the epoch, supplied by the
   storage's own clock - never the JVM's, so they can be compared without
   mixing in a second clock.")

(defprotocol Storage
  (init-db! [this]
    "Ensures the required schema and tables exist in the storage. Returns this.")

  (insert-workflow!
    [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    "Inserts a STARTED workflow row, unless one already exists for this exact
     (wf_def, params) pair, in which case nothing is inserted. Its lease
     expires timeout-ms after now (nil: never). Returns the new row's
     invocation_id, or nil if nothing was inserted.")

  (finish! [this invocation-id state result-edn-str]
    "Moves invocation-id from STARTED to state (DONE or ERROR) with
     result-edn-str as its result - but only if it is still STARTED under
     that same invocation_id, i.e. it was neither finished, timed out nor
     restarted meanwhile. Returns true if the row was updated.")

  (time-out! [this invocation-id result-edn-str]
    "Like finish! with state ERROR, but only if the lease of invocation-id
     has also expired. Returns true if the row was updated.")

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    "Moves invocation-id from DONE or ERROR back to STARTED under a new,
     larger invocation_id, clearing its result, replacing its metadata and
     leasing it for timeout-ms (nil: never expires). Returns the new
     invocation_id, or nil if invocation-id is no longer DONE or ERROR
     (e.g. a concurrent restart got there first).")

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    "Logs that a caller running under parent-invocation-id (nil: top-level)
     with metadata-edn-str got the stored result (or failure) of
     invocation-id instead of running it again: appends a REUSED row to the
     history of the workflow invocation-id belongs to - which may also be
     an earlier invocation_id of it - without changing the workflow row.")

  (get-workflow [this invocation-id]
    "The workflow row invocation-id belongs to, or nil. invocation-id may
     also be an earlier invocation_id of a since restarted workflow - its
     current row is returned then, whose invocation_id differs. The row
     also carries expired: true if its lease has run out (by the storage's
     clock), so time-out! would succeed.")

  (find-by-wf-def-and-params [this wf-def params-edn-str])

  (top-level-workflows
    [this]
    [this filters]
    "Workflow rows (every column but idempotence_key), each annotated with
     created_at (when its first invocation started), newest invocation
     first. filters is an
     optional map of {:state s :wf-def w :top-level-only? t :limit n
     :before invocation-id}.

     :state and :wf-def, when given, restrict to rows matching exactly;
     omitting a key (or the whole map) leaves that dimension unfiltered.
     :top-level-only? defaults to true (only parent_invocation_id IS NULL
     rows, i.e. hides sub-workflows nested under another invocation); pass
     false to include every workflow regardless of nesting.

     :limit caps the number of rows returned - pagination is seek-based,
     not OFFSET-based (which gets slower, and can skip/repeat rows under
     concurrent writes, as the offset grows): pass the invocation_id of the
     last row of the previous page as :before to fetch the rows
     immediately after it. Callers asking for n rows and wanting to know
     whether a further page exists should request :limit (inc n) and check
     whether they got more than n back, trimming to n before display.")

  (top-level-wf-defs [this top-level-only?]
    "Distinct wf_def values, alphabetical - the option list for a
     workflow-name filter dropdown. top-level-only? true restricts to
     wf_defs seen among top-level workflows; false includes every
     workflow regardless of nesting.")

  (full-timeline [this invocation-id]
    "Every state (past ones from workflow_history and the current one) of
     the workflow invocation-id belongs to and of every workflow nested
     under any of its invocations at any depth, interleaved into a single
     chronological sequence. Each row carries invocation_id (the
     invocation it was recorded under), current_invocation_id, wf_def,
     state, state_changed_at, data (metadata for STARTED and REUSED, the
     result otherwise) and depth (0 for the workflow itself, 1 for a direct
     sub-workflow, and so on). A workflow whose result an invocation reused
     counts as its sub-workflow too, with its whole history included."))
