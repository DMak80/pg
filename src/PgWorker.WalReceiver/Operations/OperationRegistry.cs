using System.Threading;

namespace PgWorker.WalReceiver.Operations;

/// <summary>
/// Реестр одной активной операции приёмника (spec §3.2): нодовый режим —
/// полный бэкап, tool-режим — одна операция на контейнер. Снимки IMMUTABLE,
/// публикация — атомарная замена ссылки (паттерн LiveFacts); TryBegin —
/// 409-гвард второй активной операции.
/// </summary>
public sealed class OperationRegistry
{
    private OperationSnapshot? _current; // доступ только через Volatile

    /// Текущий снимок (null — активной операции нет).
    public OperationSnapshot? Current => Volatile.Read(ref _current);

    /// CAS-гвард старта: true — снимок принят (было null/терминальное);
    /// false — активная операция уже есть (running/uploading) — вызывающий
    /// отвечает 409. Принятый снимок публикуется до возврата.
    public bool TryBegin(OperationSnapshot initial)
    {
        var cur = Volatile.Read(ref _current);
        if (cur is { State: "running" or "uploading" }) return false;
        return ReferenceEquals(Interlocked.CompareExchange(ref _current, initial, cur), cur);
    }

    /// Публикация нового снимка (атомарная замена; вызывается только владельцем операции).
    public void Update(OperationSnapshot next) => Volatile.Write(ref _current, next);

}
