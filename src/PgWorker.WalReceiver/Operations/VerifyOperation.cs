using System.Diagnostics;
using PgWorker.Backups;
using PgWorker.Etcd.S3;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Операция verify tool-контейнера (arch/19 §5, spec t26 §3.4): одна
/// активная на контейнер (общий OperationRegistry). Фаза downloading —
/// union-лист full/&lt;id&gt;/ по всем endpoints + скачивание 1:1 .NET S3-клиентом
/// (Р4 — S3-контекст приходит POST-командой, секреты только в POST) в
/// staging-том;
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
    private readonly string _stagingRoot;
    private readonly Func<string, IBackupS3>? _targetFactory; // тестовый шов (шаг 9)
    private readonly CancellationTokenSource _cts = new();

    public VerifyOperation(
        StartVerifyRequest request,
        IProcessRunner runner,
        OperationRegistry registry,
        string? stagingRoot = null,
        Func<string, IBackupS3>? targetFactory = null)
    {
        _request = request;
        _runner = runner;
        _registry = registry;
        _stagingRoot = stagingRoot ?? ToolPaths.Staging;
        _targetFactory = targetFactory;
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
        var stagingDir = Path.Combine(_stagingRoot, "full", _request.BackupId);
        // Тип — PoolBackupS3 (не IBackupS3): union-листинг набора — метод пула.
        PoolBackupS3? s3 = null;
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

            // Пул-клиент (write=read=endpoints команды): скачивание ключа —
            // перебор read-списка до найденного (GetObjectToFileAsync).
            s3 = new PoolBackupS3(
                _request.S3.Endpoints, _request.S3.Endpoints,
                _request.S3.AccessKey, _request.S3.SecretKey, _request.S3.Bucket,
                caBundlePath, requestTimeout: WalReceiverCore.PutAttemptTimeout,
                targetFactory: _targetFactory);

            // Фаза downloading: union-лист + скачивание 1:1 (mkdir по
            // относ. пути). Листинг — по ВСЕМ endpoints с дедупом: полный
            // заливается на первую живую ноду момента заливки — соло-лист
            // другой ноды (отказ посреди заливки/лаг mesh) неполон и дал бы
            // недокачанный набор → ложный permanent verify-FAILED.
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, OperationPhases.Downloading));
            var listed = await s3.ListPrefixUnionAsync(s3Base, ct: token);
            if (!listed.IsSuccess)
            {
                Fail(OperationPhases.Downloading, OpText.OneLine(listed.Error!.Message));
                return;
            }
            Directory.CreateDirectory(stagingDir);
            foreach (var obj in listed.Value)
            {
                // rel — путь файла под staging (относительно s3Base); key — ключ
                // скачивания ОТНОСИТЕЛЬНО {cluster}/{shard}/ (контракт
                // GetObjectToFileAsync сам сочиняет {cluster}/{shard}/{key}):
                // полный obj.Key давал задвоение c1/s1/c1/s1/... (ключа нет —
                // downloading-transient навсегда), rel давал бы укороченный ключ.
                var rel = obj.Key[s3Base.Length..];
                var key = obj.Key[(_request.S3.Prefix.Length + 1)..];
                var dest = Path.Combine(stagingDir, rel.Replace('/', Path.DirectorySeparatorChar));
                var destDir = Path.GetDirectoryName(dest);
                if (!string.IsNullOrEmpty(destDir))
                    Directory.CreateDirectory(destDir);
                var got = await s3.GetObjectToFileAsync(cluster, shard, key, dest, token);
                if (!got.IsSuccess)
                {
                    Fail(OperationPhases.Downloading, OpText.OneLine(got.Error!.Message));
                    return;
                }
            }

            // Фаза verifying: pg_verifybackup <каталог набора> (child через
            // runner; каталог — ПОЗИЦИОННЫЙ аргумент, флага -D у утилиты нет).
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, OperationPhases.Verifying));
            var psi = new ProcessStartInfo { FileName = "pg_verifybackup" };
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
