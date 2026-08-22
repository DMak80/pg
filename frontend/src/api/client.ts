// Ошибка API: HTTP-статус + разобранные ProblemDetails (+ Retry-After для 429).
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly title?: string,
    readonly detail?: string,
    readonly retryAfterSeconds?: number,
  ) {
    super(detail ?? title ?? `HTTP ${status}`);
    this.name = 'ApiError';
  }
}

// Опции запроса: только то, что нужно каркасу (метод + JSON-тело).
export interface ApiFetchInit {
  method?: 'GET' | 'POST';
  body?: unknown;
}

// Разбор тела ошибки: ProblemDetails (title/detail) и заголовок Retry-After.
async function toApiError(response: Response): Promise<ApiError> {
  let title: string | undefined;
  let detail: string | undefined;
  try {
    const problem = (await response.json()) as { title?: string; detail?: string };
    title = problem.title;
    detail = problem.detail;
  } catch {
    // Тело не JSON (пустое/HTML) — поля остаются undefined.
  }

  const retryAfter = response.headers.get('Retry-After');
  const retryAfterSeconds =
    retryAfter !== null && /^\d+$/.test(retryAfter) ? Number(retryAfter) : undefined;
  return new ApiError(response.status, title, detail, retryAfterSeconds);
}

// Guard-реакция: 401 от любого вызова (кроме страницы логина) → /login с возвратом (spec §3.11).
function redirectOnUnauthorized(): void {
  if (window.location.pathname === '/login') return;
  const from = encodeURIComponent(window.location.pathname + window.location.search);
  window.location.replace(`/login?from=${from}`);
}

// Единственная точка HTTP фронта: относительные пути, cookie same-origin (spec §3.13).
export async function apiFetch<T>(path: string, init?: ApiFetchInit): Promise<T> {
  const hasBody = init?.body !== undefined;
  const response = await fetch(path, {
    method: init?.method ?? 'GET',
    credentials: 'same-origin',
    headers: hasBody
      ? { Accept: 'application/json', 'Content-Type': 'application/json' }
      : { Accept: 'application/json' },
    body: hasBody ? JSON.stringify(init?.body) : undefined,
  });

  if (response.status === 401) redirectOnUnauthorized();
  if (!response.ok) throw await toApiError(response);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}
