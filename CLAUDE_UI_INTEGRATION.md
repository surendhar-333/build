# Claude Opus task: integrate the interactive CLI UI

## Objective

Add the prototype terminal UI to an existing long-running work script in this repository. Preserve the
script's business logic and outputs; improve only how progress, logs, and user input are presented.

Read `AGENT_CONTEXT.md` first. Then inspect the repository and identify the actual orchestration entry point
before editing anything. Do not assume a Bash entry point exists just because the prototype uses Bash.

## Source artifacts

- `LoadingUiApp.cs` is the Windows executable wrapper. It locates Git Bash, extracts embedded resources to a
  unique temporary directory, launches the PowerShell UI, preserves the child exit code, and cleans up.
- `build-app.ps1` compiles the wrapper and embeds the PowerShell launcher plus a Bash workload as resources.

The current artifact names are prototype defaults:

- `run-with-ui.ps1`
- `demo-interactive-ui.sh`
- `LoadingUI.Resources.run-with-ui.ps1`
- `LoadingUI.Resources.demo-interactive-ui.sh`

Replace the demo workload with the real work entry point, or retain a generic drag-and-drop/external-script
mode if embedding the work script would make maintenance harder. Keep resource names synchronized between
`LoadingUiApp.cs` and `build-app.ps1`.

## UI protocol

The UI treats specially formatted stdout lines as control messages. All other stdout and stderr lines remain
normal logs.

### Determinate progress

```bash
printf '%s\n' '::progress::10::Loading configuration'
printf '%s\n' '::progress::55::Processing records'
printf '%s\n' '::progress::100::Complete'
```

Format:

```text
::progress::<integer 0-100>::<short status text>
```

### Text input

```bash
printf '%s\n' '::ui-prompt::{"type":"text","label":"Project name","default":"payments-api","step":1,"total":2}'
IFS= read -r project_name
```

### Single-choice input

```bash
printf '%s\n' '::ui-prompt::{"type":"select","label":"Environment","options":["Development","Staging","Production"],"defaultIndex":1,"step":2,"total":2}'
IFS= read -r environment
```

The PowerShell host pauses dashboard rendering, displays the prompt as a modal, writes the selected answer to
the child process stdin, and then resumes progress and log rendering.

## Integration rules

1. Locate every existing user prompt and classify it as text, secret, choice, multi-choice, confirmation, or
   file selection.
2. Convert only supported text and single-choice prompts initially. Keep unsupported prompts working until
   their UI component is implemented and tested.
3. Prefer one pre-run setup wizard for values known before execution. Use mid-run modals only for questions
   that genuinely depend on runtime results.
4. End setup with a review-and-confirm screen when more than three values are collected.
5. Never print passwords, tokens, connection strings, account numbers, or other secrets to stdout, stderr,
   summaries, or retained logs. Add a masked password prompt before migrating secret inputs.
6. Preserve the original script's exit codes, signal handling, validation, defaults, retries, and side effects.
7. Keep stdout machine-readable when it is redirected or running in CI. Disable the interactive UI when the
   host is not a TTY and fall back to plain logs/prompts.
8. Do not estimate fake percentages. If total work is unknown, show an indeterminate spinner and meaningful
   stage text.
9. Keep Windows PowerShell 5.1 compatibility unless the repository explicitly standardizes on PowerShell 7.
   For 5.1, keep `.ps1` source ASCII-safe and generate Unicode box/spinner characters from character codes.
10. Git Bash is currently an external runtime dependency. Detect it through `BASH_EXE`, common Git for
    Windows locations, and finally `PATH`. Return a clear error if it is unavailable.

## Recommended implementation sequence

1. Read `AGENT_CONTEXT.md` and inventory candidate work scripts.
2. Document which script will be wrapped and list its current prompts and major execution stages.
3. Add the PowerShell runtime UI (or adapt the existing `run-with-ui.ps1`) without changing workload logic.
4. Replace raw prompt lines with the protocol incrementally.
5. Update `LoadingUiApp.cs` and `build-app.ps1` to embed the correct runtime files.
6. Build the executable and test it from a directory containing only the executable.
7. Test success, workload failure, user cancellation, invalid input, narrow terminal width, missing Bash, and
   cleanup of extracted temporary files.
8. Update `AGENT_CONTEXT.md` with the chosen entry point, build command, run command, and remaining limits.

## Acceptance criteria

- Double-clicking the executable opens the setup UI and launches the real work script.
- Progress/status appears on the left and live logs remain readable on the right.
- Interactive questions appear one at a time without corrupting the dashboard.
- Arrow-key choice selection and text defaults work.
- No secret value is displayed or persisted.
- Cancelling terminates the child process and restores the terminal cursor/screen.
- The executable returns the real workload exit code.
- The bundled executable works when copied away from the source checkout, with Git Bash installed.
- Non-interactive/CI execution has a plain-text fallback.

## Prompt to continue with Claude Opus

```text
Read AGENT_CONTEXT.md and CLAUDE_UI_INTEGRATION.md completely. Inspect the repository to locate the real
long-running work entry point and its current user prompts. Integrate the CLI progress/log/input UI using the
provided protocol while preserving all business logic, validation, side effects, and exit codes. Do not
invent a new workflow if an existing orchestration script already exists. Implement incrementally, test the
failure and cancellation paths, build the bundled executable, and update AGENT_CONTEXT.md with exact usage.
```
