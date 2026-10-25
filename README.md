# Reason Harbor adapter

Run [Reason](https://reasonmachines.ai) as an agent in
[Harbor](https://harborframework.com). Harbor starts the task container and
grades the result. The adapter installs the published Reason CLI in that
container, checks its checksum, and starts it as a worker; the agent runs as a
normal Reason Session. Your API key stays on your machine; the container only
receives a one-use enrollment ticket.

## Install

```bash
uv pip install reason-machines-harbor
```

This installs Harbor 0.22.0. For cloud environments, add the matching Harbor
extra, for example `uv pip install "harbor[modal]==0.22.0"`.

## Run

Get a [Reason API key](https://reasonmachines.ai/customize?tab=api) with
`run`, `sessions:read`, `deployment:read`, `devices:read`, `devices:write`,
`devices:use` and `org:write`, and enable the model under
[Models](https://reasonmachines.ai/customize?tab=models).

```bash
export REASON_API_KEY="YOUR_REASON_API_KEY"
DOCKER_DEFAULT_PLATFORM=linux/amd64 harbor run \
  --dataset datacurve/deep-swe-1-1 --n-tasks 1 \
  --agent reasonmachines_harbor:ReasonAgent \
  --model openai/gpt-6-luna \
  --ak reasoning_effort=xhigh \
  --allow-agent-host api.reasonmachines.com \
  --allow-agent-host relay.reasonmachines.com \
  --env reasonmachines_harbor.environment:ReasonDockerEnvironment
```

The worker connects out to the Reason API and relay, so both
`--allow-agent-host` flags are required, even for offline tasks.

| Agent kwarg (`--ak`) | Default | Meaning |
|---|---|---|
| `reasoning_effort` | `medium` | `minimal`, `low`, `medium`, `high`, `xhigh` or `max` |
| `mode` | `ephemeral` | `ephemeral`: a new single-use workspace per trial, with memory and skills on. `standard`: your own workspace |
| `runtime_profile` | `runtime/reason-agent-1.2` in ephemeral mode; the server default in standard mode | A harness listed by `GET /v3/deployment`. The default is the frozen Reason Agent 1.2 release: it never changes, and its trials install the CLI and agent tools it pins (CLI 0.2.151). Name an earlier release to rerun it, for example `runtime/reason-v7-2026-10`; `runtime/reason-c2-tools-v7` runs the deployment's current default instead |

Ephemeral mode needs an owner or admin key of a workspace with paid model
access. To bill the model to your own provider account, connect its API key
under [Models](https://reasonmachines.ai/customize?tab=models) and pass the
model as `byok:<provider>:<model>`, for example `byok:openai:gpt-6-luna`.

To run the full benchmarks and reproduce our results, see
[reason-machines/benchmark](https://github.com/reason-machines/benchmark).

## Source

[reason-machines/reason-machines-harbor](https://github.com/reason-machines/reason-machines-harbor)
is a read-only copy of the adapter in the Reason monorepo, where it is
developed and tested. Releases are tagged `vX.Y.Z`.
