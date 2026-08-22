namespace AdminPanel.Core;

// Стендовая топология /cluster/nodes/<node> → IP (arch/02 §2.3; в проде префикса нет).
public sealed record StandNode(string Name, string? Address);
