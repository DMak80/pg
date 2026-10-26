using System.Diagnostics;
using PgWorker.Backups;
using PgWorker.Etcd.S3;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Операция verify tool-контейнера (arch/19 §5, spec t26 §3.4): одна
/// активная на контейнер (общий OperationRegistry). Фаза downloading — list +
/// скачивание full/&lt;id&gt;/ 1:1 .NET S3-клиентом (ReadAll-семантика, Р4 —
/// S3-контекст приходит POST-командой, секреты только в POST) в staging-том;
/// сбой скачивания — failed с фазой downloading (МАРКЕР ТРАНЗИЕНТА: воркер
/// держит PENDING и ретраит тиками, контейнер сносится и создаётся заново).
/// Фаза verifying — pg_verifybackup (manifest SHA256, вкл. pg_wal/); ≠ 0 —
/// failed с фазой verifying (PERMANENT — данные плохие). Чистка staging —
/// при любом исходе; терминальный снимок остаётся в реестре (следующий POST
/// — новая операция, TryBegin перезапишет терминальный).</summary>
public sealed class VerifyOperation : IReceiverOperation
{
    private readonly StartVerifyRequest _request;
    private readonly IProcessRunner _runner;
    private readonly OperationRegistry _registry;
    private readonly CancellationTokenSource _cts = new();

    public VerifyOperation(
        StartVerifyRequest request,
        IProcessRunner runner,
        OperationRegistry registry)
    {
        _request = request;
        _runner = runner;
        _registry = registry;
    }

    public OperationSnapshot? Snapshot => _registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;

        // prefix команды — <C>/<X>: cluster/shard — разбор по первому '/'.
        var prefix = _request.S3.Prefix.Trim('/');
        var slash = prefix.IndexOf('/');
        var cluster = slash > 0 ? prefix[..slash] : prefix;
        var shard = slash > 0 ? prefix[(slash + 1)..] : string.Empty;
        var s3Base = $"{prefix}/full/{_request.BackupId}/";
        var stagingDir = Path.Combine(ToolPaths.Staging, "full", _request.BackupId);
        IBackupS3? s3 = null;
        string? caBundlePath = null;
        try
        {
            // CA https (PEM в POST-команде, §7): материализация в temp, 0600.
            if (_request.S3.CaPem is { Length: > 0 } caPem)
            {
                caBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
                var caOptions = OperatingSystem.IsWindows()
                    ? new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None }
                    : new FileStreamOptions
                    {
                        Mode = FileMode.Create,
                        Access = FileAccess.Write,
                        Share = FileShare.None,
                        UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
                    };
                await using (var caStream = new FileStream(caBundlePath, caOptions))
                using (var writer = new StreamWriter(caStream))
                {
                    await writer.WriteAsync(caPem + "\n");
                }
            }

            // ReadAll-семантика: перебор нод до найденного (полный заливается
            // на первую живую ноду момента заливки — verify ищет по всем).
            s3 = new PoolBackupS3(
                _request.S3.Endpoints, _request.S3.Endpoints,
                _request.S3.AccessKey, _request.S3.SecretKey, _request.S3.Bucket,
                caBundlePath, requestTimeout: WalReceiverCore.PutAttemptTimeout);

            // Фаза downloading: list + скачивание 1:1 (mkdir по относ. пути).
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, OperationPhases.Downloading));
            var listed = await s3.ListPrefixAsync(s3Base, ct: token);
            if (!listed.IsSuccess)
            {
                Fail(OperationPhases.Downloading, OpText.OneLine(listed.Error!.Message));
                return;
            }
            Directory.CreateDirectory(stagingDir);
            foreach (var obj in listed.Value)
            {
                var rel = obj.Key[s3Base.Length..];
                var dest = Path.Combine(stagingDir, rel.Replace('/', Path.DirectorySeparatorChar));
                var destDir = Path.GetDirectoryName(dest);
                if (!string.IsNullOrEmpty(destDir))
                    Directory.CreateDirectory(destDir);
                var got = await s3.GetObjectToFileAsync(cluster, shard, obj.Key, dest, token);
                if (!got.IsSuccess)
                {
                    Fail(OperationPhases.Downloading, OpText.OneLine(got.Error!.Message));
                    return;
                }
            }

            // Фаза verifying: pg_verifybackup -D <staging> (child через runner).
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, OperationPhases.Verifying));
            var psi = new ProcessStartInfo { FileName = "pg_verifybackup" };
            psi.ArgumentList.Add("-D");
            psi.ArgumentList.Add(stagingDir);
            var (exitCode, _, stderr) = await _runner.RunAsync(psi, token);
            if (exitCode != 0)
            {
                // PERMANENT (фаза verifying): данные плохие — ретраи не лечат.
                Fail(OperationPhases.Verifying, OpText.FirstLine(stderr, "pg_verifybackup failed"));
                return;
            }
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Succeeded, OperationPhases.Verifying));
        }
        catch (OperationCanceledException)
        {
            // Cancel API: failed cancelled (контейнер снесёт воркер).
            Fail(CurrentPhase(), "cancelled");
        }
        catch (Exception e)
        {
            Fail(CurrentPhase(), $"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            // Чистка staging при любом исходе; CA-файл; S3-клиент.
            try
            {
                if (Directory.Exists(stagingDir))
                    Directory.Delete(stagingDir, recursive: true);
            }
            catch { /* best-effort */ }
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (s3 is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — runner
    /// убивает дерево текущего child-процесса (pg_verifybackup).</summary>
    public Task CancelAsync()
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    private string CurrentPhase()
        => _registry.Current?.Phase ?? OperationPhases.Downloading;

    private void Fail(string phase, string error)
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, OperationStates.Failed, phase, Error: error));
}
