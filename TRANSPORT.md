# Pilot Custom API transport (implementation in progress)

Manager is a Pilot-Server `IServerApiFactory` extension. Its public boundary is
`IBimBridgeTaskApi`, `IBimBridgeWorkerApi`, `IBimBridgeDiagnosticsApi` and the new,
additive `IBimBridgeArtifactApi`. It does not expose an ASP.NET REST API.

The original PSIMpy HTTP transport used `/tasks/...` paths. No implementation of
an adapter for those paths exists in the three source repositories. Existing
HTTP callers remain supported for environments with an external adapter, but
that adapter is not a dependency of the new architecture. No HTTP server is added.

## Direct connection, including Colab

`BimBridge.from_pilot(server_database, login, password)` uses the official
`Ascon.Pilot.Server.Api` package's `HttpPilotClient`, matching the Manager's
existing ProtocolTestClient authentication and Custom API mechanism.

The Python process starts a **local child process**. A JSON-lines stdin/stdout
adapter bridges Python to .NET. It has no listening socket and no independent
network API. Credentials are passed on stdin, never as command-line arguments
or persisted in download state. Only `HttpPilotClient` connects to Pilot-Server.

The source package includes the adapter source. Building requires .NET SDK 9
and NuGet access. `build_pilot_adapter()` builds into a versioned user cache;
alternatively supply a prebuilt `PilotAdapter.dll` with its adjacent runtime
dependencies using `adapter=...`. The .NET 9 runtime is required to execute it.
Python's base installation has no mandatory pythonnet dependency.

```python
from getpass import getpass
from bim_bridge import BimBridge

# In Colab, install .NET SDK 9 in the notebook environment first. This does
# not require installing or starting the Pilot desktop application.
bim = BimBridge.from_pilot(
    "https://your-pilot-server/database",
    login=input("Pilot login: "),
    password=getpass("Pilot password: "),
)
print(bim.health())
task = bim.task(saved_task_id)
task.wait()
task.download("result.bin")
bim.close()
```

The adapter has been compiled locally. Authentication against a live Pilot and
execution in Linux/Google Colab have not been exercised; Windows-specific
dependencies of the vendor transport must be validated in that environment.
No connection to the user's working Pilot is part of local tests.

## Immutable result download

`GetResultMetadataAsync(taskId)` identifies content with TaskId, TotalLength,
TotalChunks, ChunkSizeBytes and Sha256. `ReadResultChunkAsync(taskId,index)` is
deterministic. No HTTP Range/206/ETag is required for direct Pilot transport.

`Task.download()` stores `<destination>.inprogress` and
`<destination>.download.json`. Each saved prefix ends on a complete chunk. Data
is flushed before its state is atomically committed. On the next invocation,
fresh metadata must match the persisted identity. The saved prefix hash is
verified, any uncommitted tail is truncated, and reads start at the next chunk.
The full SHA-256 and size are verified before atomic destination publication.
Gzip decoding happens only after validating the original artifact bytes.

A changed identity or damaged prefix produces an explicit error and never
silently appends data. Use a new destination to intentionally fetch a new
artifact. Downloads assume one writer per destination.

The legacy HTTP transport remains separate and can optionally resume with
standard Range semantics when an existing external endpoint supports them.
It is not used by `from_pilot`.

## Public contract source synchronization

`tools/sync_contracts.py --check` verifies that Manager, worker and the local
adapter have byte-identical public protocol source copies. Run without
`--check` after modifying canonical files in Manager's Contracts project.
