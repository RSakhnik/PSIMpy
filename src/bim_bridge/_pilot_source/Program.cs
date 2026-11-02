// Local stdin/stdout adapter, NOT an HTTP server. Only HttpPilotClient performs
// network I/O. Credentials arrive through stdin, never command-line arguments.
using Ascon.Pilot.Common.DataProtection;
using Ascon.Pilot.DataClasses;
using Ascon.Pilot.Server.Api;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Pilot.BimBridge.Contracts;

Console.InputEncoding = System.Text.Encoding.UTF8;
Console.OutputEncoding = new System.Text.UTF8Encoding(false);
var output = Console.Out;
Console.SetOut(Console.Error); // third-party diagnostic output must not corrupt protocol stdout
HttpPilotClient client = null;
IBimBridgeTaskApi tasks = null;
IBimBridgeDiagnosticsApi diagnostics = null;
string line;
while ((line = Console.ReadLine()) != null)
{
    try
    {
        var request = JObject.Parse(line);
        var method = request.Value<string>("Method");
        object result;
        if (method == "Connect")
        {
            if (client != null) throw new InvalidOperationException("Already connected.");
            var credentials = ConnectionCredentials.GetConnectionCredentials(request.Value<string>("ServerDatabase"),
                request.Value<string>("Login"), request.Value<string>("Password").ConvertToSecureString());
            client = new HttpPilotClient(credentials.GetConnectionString());
            client.Connect(false);
            await client.GetAuthenticationAsyncApi().LoginWithIdentityAsync(credentials.DatabaseName, credentials.Username,
                credentials.ProtectedPassword, request.Value<int?>("EnterpriseCode") ?? 103,
                new SelfIdentity {Id="PSIMpy.PilotAdapter"});
            var callback = new Callback();
            tasks = client.GetCustomApi<IBimBridgeTaskApi, IBimBridgeClientCallback>(callback);
            diagnostics = client.GetCustomApi<IBimBridgeDiagnosticsApi, IBimBridgeClientCallback>(callback);
            result = new {Connected=true};
        }
        else
        {
            if (tasks == null) throw new InvalidOperationException("Connect first.");
            switch (method)
            {
                case "GetHealth": result=await diagnostics.GetHealthAsync();break;
                case "GetWorkers": result=await diagnostics.GetWorkersAsync();break;
                case "GetStatistics": result=await diagnostics.GetStatisticsAsync();break;
                case "SubmitTask": result=await tasks.SubmitTaskAsync(request["Request"].ToObject<SubmitBimTaskRequestDto>());break;
                case "GetTaskStatus": result=await tasks.GetTaskStatusAsync(Guid.Parse(request.Value<string>("TaskId")));break;
                case "CancelTask": result=await tasks.CancelTaskAsync(Guid.Parse(request.Value<string>("TaskId")));break;
                case "GetResultMetadata": result=await tasks.GetResultMetadataAsync(Guid.Parse(request.Value<string>("TaskId")));break;
                case "ReadResultChunk": result=await tasks.ReadResultChunkAsync(Guid.Parse(request.Value<string>("TaskId")),request.Value<int>("ChunkIndex"));break;
                case "Close": client.Dispose();return;
                default: throw new InvalidOperationException("Unsupported adapter method.");
            }
        }
        await output.WriteLineAsync(JsonConvert.SerializeObject(result));
        await output.FlushAsync();
    }
    catch (Exception)
    {
        // Exception messages from authentication/transport may contain secrets.
        await output.WriteLineAsync("{\"Error\":{\"Code\":\"PILOT_TRANSPORT_ERROR\",\"Message\":\"Pilot Custom API call failed\",\"IsRetryable\":true}}");
        await output.FlushAsync();
    }
}
client?.Dispose();
sealed class Callback : IBimBridgeClientCallback {public void OnNotification(BimBridgeNotificationDto value) {}}
