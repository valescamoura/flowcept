# Agent Loop Provenance Skill

This directory contains the Codex skill used by the Flowcept Codex adapter to capture the Declared Provenance Layer (DPL) for PROV-Agent-Loop experiments.

The Flowcept Codex adapter can always capture the Observed Provenance Layer (OPL) from Codex session JSONL logs. OPL includes structural information such as prompts, assistant responses, model calls, tool calls, command inputs, outputs, timestamps, status, errors, and token usage.

DPL requires the assistant to explicitly emit provenance annotations for semantic concepts that cannot be reliably inferred from raw logs, such as objectives, plans, plan steps, loop iterations, evaluations, criteria/results, thoughts, observations, beliefs, decisions, mandates, memories, and lessons learned.

## Files

- `AGENT_LOOP_PROVENANCE_SKILL.md`: source text for the Codex skill. It is intentionally not named `SKILL.md` because this repository ignores `SKILL.md` files.
- `agents/openai.yaml`: optional skill agent metadata.
- `references/flowcept-prov-agent-loop.md`: detailed Flowcept/PROV-Agent-Loop mapping used by the skill.

## Installing For Codex

Codex expects installed skills to contain a file named `SKILL.md`. To install this skill locally, copy this directory into your Codex skills directory and rename the source file to `SKILL.md` in the installed copy:

```bash
mkdir -p ~/.codex/skills/agent-loop-provenance
cp AGENT_LOOP_PROVENANCE_SKILL.md ~/.codex/skills/agent-loop-provenance/SKILL.md
cp -R agents references ~/.codex/skills/agent-loop-provenance/
```

Start a new Codex session after installing or updating the skill.

## Using With The Codex Adapter

Configure Flowcept with the Codex adapter and point it to the Codex session JSONL:

```yaml
adapters:
  codex:
    kind: codex
    file_path: /path/to/codex/session.jsonl
    watch_interval_sec: 1
    recursive: true
    include_developer_messages: true
    include_reasoning: true
    declared_provenance_enabled: true
```

Set `declared_provenance_enabled: true` for DPL runs. Set it to `false` for OPL-only runs.

In the Codex session that performs the experiment, enable or invoke the `agent-loop-provenance` skill before asking the assistant to configure and run the workflow. The adapter will parse the skill's `<flowcept_event>...</flowcept_event>` annotations from the Codex JSONL and map them to Flowcept workflow/task/entity records.
