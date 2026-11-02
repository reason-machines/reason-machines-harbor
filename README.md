# Reason Harbor adapter

Run [Reason](https://reasonmachines.ai) as an agent in
[Harbor](https://harborframework.com). Harbor starts the task container and
grades the result. The adapter installs the published Reason CLI in that
container, checks its checksum, and starts it as a worker; the agent runs as a
normal Reason Session. Your API key stays on your machine; the container only
receives a one-use enrollment ticket.

## Install

```bash
uv tool install "harbor==0.25.0" --with "reason-machines-harbor>=0.3.44"
```

The adapter works with Harbor 0.22 to 0.25 and Python 3.12 or later. For cloud
environments, add the matching Harbor extra, for example
`"harbor[e2b,modal]==0.25.0"`. To reproduce our published results, use the
Harbor version the [benchmark scripts](https://github.com/reason-machines/benchmark) pin.

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
`--allow-agent-host` flags are required, even for offline tasks. Drop
`--n-tasks 1` to run the whole dataset. On e2b, use
`--env reasonmachines_harbor.environment:ReasonE2BEnvironment`: it keeps the
task image's environment variables, PATH included, which e2b otherwise drops.

Each trial's `agent/` folder holds the Session as the model saw it
(`trajectory.json`, ATIF), its link, status and cost (`session.json`) and
what it ran on (`receipt.json`, `deployment.json`). API errors are
reported by code, for example `missing_scope` or
`ephemeral_workspace_parent_payment_required`.

| Agent kwarg (`--ak`) | Default | Meaning |
|---|---|---|
| `reasoning_effort` | `medium` | `minimal`, `low`, `medium`, `high`, `xhigh` or `max` |
| `mode` | `ephemeral` | `ephemeral`: a new single-use workspace per trial, with memory and skills on. `standard`: your own workspace |
| `runtime_profile` | `runtime/reason-agent-1.3` in ephemeral mode; the server default in standard mode | A harness listed by `GET /v3/deployment`. The default is the frozen Reason Agent 1.3 release: it never changes, and its trials install the CLI and agent tools it pins (CLI 0.2.625, agent-tools-2026-10-27). Name an earlier release to rerun it, for example `runtime/reason-agent-1.2`; `runtime/reason-c2-tools-v7` runs the deployment's current default instead |

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

## License

[Apache-2.0](LICENSE).
