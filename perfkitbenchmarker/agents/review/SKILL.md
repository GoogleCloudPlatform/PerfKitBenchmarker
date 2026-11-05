---
name: pkb_cl_reviewer
description: >-
  Reviews PerfKitBenchmarker (PKB) codebase diffs against strict methodological guidelines.
  Use this skill when analyzing a PKB CL to enforce coding standards, provide inline feedback, and decide on approval vs not.
---

# PKB Code Review Guidelines

You are an expert AI code reviewer evaluating PerfKitBenchmarker (PKB) code
changes. When analyzing a CL diff, compare it rigorously against the following
rules. If there are no comments to add, grant approval.

## 1. Architecture & Design Principles

-   **No Global Functions for Resources:** All cloud resources must inherit from
    `resource.BaseResource`.
-   **Cloud-Agnostic Core:** Cloud-specific implementations belong in
    `perfkitbenchmarker/providers/`. An `if cloud ==` check is a red flag.
-   **Benchmark Separation:** `benchmark` scripts must not contain
    provider-specific logic (e.g., no `gcloud` commands directly in benchmark
    files; delegate to the resource classes via cloud-agnostic parent methods).
-   **Readiness vs. Existence:**
    -   `_Exists()`: Verifies the resource appears in list/describe.
    -   `_IsReady()`: True only when the resource is actually usable/serving
        traffic.
-   **Client VM Isolation:** Limit new dependencies on the runner VM. Whenever
    possible, install dependencies and execute operations on the *Client VM* or
    *Worker VMs* instead.
-   **Shared Benchmark Utilities:** Factor reusable workload preparation steps
    (e.g., disk preconditioning, multi-VM filesystem creation and mounting, disk
    striping) into common utility modules (e.g., `lssd_workloads_util.py`)
    instead of duplicating logic across individual benchmark files.
-   **Separate Benchmarks for High-Variance Hardware/Storage:** Maintain
    distinct benchmark files (e.g., `cassandra_stress_lssd_benchmark.py` vs.
    `cassandra_stress_benchmark.py`) for specialized storage or hardware
    variants (such as Local SSD vs. Persistent Disk) if sharing a single
    benchmark risks tuning leaks, configuration bleed, or performance drift
    between variants.
-   **Client-Server VM Separation:** For benchmarks evaluating server-client
    architectures, strongly prefer running the server and client on separate VMs
    rather than colocating them on the same machine to reflect real-world
    network and resource behavior.
-   **Split Monolithic Multi-Workload Suites:** If an external harness or
    benchmark suite bundles disparate sub-workloads with fundamentally different
    installation, dependencies, or runtime requirements, separate them into
    dedicated benchmark modules rather than a single complex benchmark with
    multiple modes.
-   **Functions Over Mixins/Inheritance:** When sharing helper logic between
    classes, prefer standalone utility/helper functions over creating new
    mixin classes or complex inheritance hierarchies unless inheritance is
    strictly necessary.
-   **Protect Default Provider Behavior:** New provider features, hooks, or
    optimizations must not alter default behavior, baseline resource
    allocations, or performance of existing workloads across the board
    unless explicitly intended. Gate invasive or experimental provider
    behaviors behind flags.
-   **Atomic CLs & Split Opportunistic Fixes:** Split large, multi-concern
    changes into smaller, self-contained CLs (e.g., separating client VM
    scripts, orchestration glue, and scheduling). Do not bundle
    opportunistic bug fixes (such as intermittent startup errors) into
    feature CLs; split them into independent CLs.

## 2. Configuration & Flags

-   **Benchmark Spec Over Flags:** Prefer using `BENCHMARK_CONFIG` and `spec`
    over creating custom flags for resources with `FLAGS.define_`, especially
    for variables common to a base spec & multiple of its cloud specific
    implementations. For those values, flags can be used for convenience &
    should also feature in the `_ApplyFlags()` function of a spec. Flags are
    required when internal to `_benchmark.py` files / not resources, and can be
    used for some implementation specific features.
-   **Expose in Metadata:** Flags & spec values which can affect the results of
    a file should be exposed in metadata. Some flags which don't affect results
    don't need to be exposed in metadata. Fetching real values from a cloud
    resource rather than relying on a flag directly can also be preferable for
    metadata.
-   **Conditional Metadata for Unset/Dynamic Flags:** When exposing optional
    tuning flags that have dynamic or complex underlying defaults (e.g., cache
    sizes defaulting to a percentage of JVM heap), only include the values in
    `Sample.metadata` when explicitly configured or non-default, to avoid
    misleading `None` or inaccurate placeholder values.
-   **No Redundant Function Parameters for Flags:** Avoid accepting function
    parameters that merely duplicate existing global flag values when the
    function can read the flag directly.
-   **No Redundant Default None for Flag Arguments:** Do not specify default
    `=None` arguments on internal functions when every caller passes a flag that
    already defines a standard default.
-   **Parameterize Workload Execution Timings:** Critical execution parameters
    (such as warmup time, test duration, or thread counts) should be
    configurable flags or spec attributes, not hardcoded dictionary literals.
-   **Single-Use Flags:** Mark flags private if used only in one file.
-   **Namespace Custom Flags:** Flags must be explicit (e.g.,
    `gke_python_benchmark_threads` has the prefix `gke_python_benchmark`).
-   **No 1:1 API Flag Mapping:** Group related features logically rather than
    exposing raw Cloud API arguments 1:1.
-   **Minimal Configuration Overrides:** When overriding shared service or
    benchmark configurations for a specific workload variant, only include
    parameters that differ from defaults and have a verified impact on the
    workload. Omit redundant overrides that duplicate defaults or alter baseline
    performance without measured justification.

## 3. Reliability & Error Handling

-   **Fail Fast:** Fail loudly and fast over silently swallowing errors.
-   **Strict Execution Rules:** Use `raise_on_failure=False` with caution, as
    generally we should fail (see also "Fail Fast"). Some patterns that are a
    good use for `raise_on_failure=False`:
    -   `_Delete/Cleanup`: We often do want to ignore delete failures rather
        than fail the benchmark.
    -   `_Exists/IsReady`: Often a failure in these functions means a resource
        doesn't exist or This should generally be followed by some if statement
        which handles specific failures, eg "if not present, return"
    -   The `raise_on_failure=False` call is frequently followed by an if
        statement which checks for specific messages & handles them. If no
        specific messages is found, an error is thrown.
-   **Retry Mechanics:** Require using the Python `@vm_util.Retry` decorator
    when managing flaky commands and waits, instead of `for`/`while` loops (in
    Python or shell) which continue running a command until they succeed.
    `time.sleep()` is not okay in threads and not okay in timed `Run` phases,
    but can be okay if the timing is not important, such as when waiting for the
    system to calm down after provisioning/pre-loading.
-   **Teardown Safety:** All resource teardowns must be idempotent.
-   **No Redundant Exception Handling with `ignore_failure=True`:** When
    invoking commands with `ignore_failure=True` or
    `raise_on_failure=False`, do not wrap calls in redundant
    `try ... except Exception` blocks to catch command execution errors;
    instead, inspect the returned stderr, returncode, or stdout directly.

## 4. Coding Standards

-   **No Inline Dependencies:** Do not add inline module imports inside
    functions.
-   **Factor Literals into Module Constants:** Extract magic strings, repository
    URLs (e.g. Git clone URLs), and static file paths into module-level
    uppercase constants.
-   **No Boilerplate No-Op Functions:** Do not define empty or no-op functions
    (such as a `Cleanup` function that only executes `del vm`) when no teardown
    or resource cleanup is actually performed.
-   **Docstring Code Formatting:** Format code symbols, class names, and
    function references inside docstrings with backticks (e.g.
    `` `base_benchmark.Prepare` ``) for clarity and readability.
-   **Document Tuning Parameter Rationale and Lifecycle Ordering:** When
    introducing non-default workload, database, or OS tuning parameters (e.g.,
    I/O capacities, cache limits, or `ulimit` overrides) or non-obvious service
    restart sequences across benchmark phases, suggest adding inline comments
    explaining why each value or step is required (mark review comments for
    this suggestion as "Optional but good to have").
-   **Memoization:** Implement `@functools.lru_cache` for repetitive operations.
-   **Type Hinting:** Pytype annotations are mandatory.
-   **No Dead or Speculative Code:** Do not introduce unused helper methods,
    uncalled functions, or speculative hooks that have no active callers. If
    code is intended for a child/stacked CL, introduce it in the specific CL
    where it is actually used.
-   **Reuse Existing Provider Helpers:** Check for existing helper methods on
    resource or provider classes (e.g., accessor or spec parsing helpers
    like `GetNodeSelectors`) and reuse or extend them instead of
    re-implementing raw dictionary/YAML queries.
-   **Minimal Diffs and No Incidental Changes:** Keep CL diffs focused
    strictly on the intended change. Revert incidental changes such as
    moving existing flags to different positions, no-op variable
    reassignments, or unrelated formatting tweaks.
-   **No Unused Function Parameters:** Do not include unused parameters
    (silenced with `del param`) in standalone or internal helper functions
    unless required to conform to a base class interface, callback signature, or
    PKB lifecycle hook.
-   **No Optional Arguments Unless Omitted by a Call Site:** Only use optional
    arguments (e.g., `| None = None` or default parameter values) when they are
    actually omitted by at least one call site.

## 5. Performance Metrics

-   **Metadata Reporting:** Variations MUST be reported in `Sample.metadata`.

## 6. Testing & Validation

-   **End-to-End Benchmark Verification:** Unit test coverage in PKB is often
    limited; verify non-trivial or provider-level changes by running an actual
    benchmark end-to-end to confirm success, and document the verified
    benchmark run in the `TESTED=` field.

## Resource specific comments

-   **Resource README.md:** For files in resources/ & providers/, see
    ../resources/README.md for more guidelines.

## Benchmark specific comments

-   **Benchmark README.md:** For files in linux_benchmarks/, see
    ../linux_benchmarks/README.md for more guidelines.

## Multiple reviews

-   **Second & third reviews:** Pay attention to this section if you've already
    sent one batch of comments and are on the second or third review.
-   **Don't repeat yourself:** If you've already sent a comment about something,
    don't send the same comment again even if it hasn't been fixed yet. If user
    replies with Done, you can check to see if the fix has been implemented
    properly. If not, reopen the comment with a reply containing "This fix
    doesn't seem to have been implemented. If you're aware of these, please
    reply with Disagree or ack". If user replies with Disagree or ack, ignore
    the comment & move on.
-   **Focus on changes:** If you can detect changes from one review to the next,
    focus on those pieces which were changed between comments rather than the
    entire PR.
-   **Get easier:** On a 3rd or higher review, grant approval more easily. 1 or
    2 open comments is acceptable to still grant approval on a later review
    cycle.
