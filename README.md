# Reason Harbor adapter

Run [Reason](https://reasonmachines.ai) as an agent in
[Harbor](https://harborframework.com). Harbor starts the task container and
grades the result. The adapter installs the published Reason CLI in that
container, after checking its published checksum, and starts it as a worker.
The agent runs as a normal Reason Session. Your API key stays on your machine;
the container only receives a one-use enrollment ticket.

## Install

```bash
uv pip install reason-machines-harbor
```

This also installs Harbor 0.22.0. For cloud environments such as Modal or
E2B, also install the matching Harbor extra, for example
`uv pip install "harbor[modal]==0.22.0"`.

## Run

Get a [Reason API key](https://reasonmachines.ai/customize?tab=api) with
`run`, `sessions:read`, `deployment:read`, `devices:read`, `devices:write` and
`devices:use`, and enable the model under
[Models](https://reasonmachines.ai/customize?tab=models).

```bash
export REASON_API_KEY="YOUR_REASON_API_KEY"
DOCKER_DEFAULT_PLATFORM=linux/amd64 .venv/bin/harbor run \
  --dataset datacurve/deep-swe-1-1 --n-tasks 1 \
  --agent reasonmachines_harbor:ReasonAgent \
  --model openai/gpt-6-luna \
  --ak reasoning_effort=xhigh \
  --allow-agent-host api.reasonmachines.com \
  --allow-agent-host relay.reasonmachines.com \
  --env reasonmachines_harbor.environment:ReasonDockerEnvironment
```

Both `--allow-agent-host` flags are required: the worker connects out to the
Reason API and relay, even for tasks that otherwise run without internet. Use
`reason-machines-harbor` 0.3.2 or later for these hostnames.

| Agent kwarg (`--ak`) | Default | Meaning |
|---|---|---|
| `reasoning_effort` | `medium` | `minimal`, `low`, `medium`, `high`, `xhigh` or `max` |
| `mode` | `isolated` | `isolated`: memory, skills and integrations off. `ephemeral`: a new single-use workspace per trial, with memory and skills on (needs enabling for your workspace). `standard`: your own workspace |

To run the full benchmarks and reproduce our results, see
[reason-machines/benchmark](https://github.com/reason-machines/benchmark).

## Source

This repository is a read-only copy of `packages/reason-machines-harbor` in
the Reason monorepo, where the adapter is developed and tested. `sync.sh`
refreshes it from a monorepo checkout. Each release is tagged `vX.Y.Z`.
