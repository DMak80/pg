#!/bin/bash
# Полный прогон всех тестовых серий монорепо (канон — arch/operations/05-run-all-tests.md):
# PgWorker.E2eTests — ПАРАЛЛЕЛЬНО всем остальным сериям; остальные — ПОСЛЕДОВАТЕЛЬНО
# между собой (юниты → интеграционные → E2E остальных воркеров).
# Скрипт самодостаточен: проекты автодетектятся по src/tests/*/*.csproj — править
# его под каждый запуск не нужно. Итоговый exit 0 — только если все серии зелёные.
#
# Использование:
#   scripts/run-all-tests.sh            # полный прогон
#   scripts/run-all-tests.sh --dry-run  # показать план, ничего не запускать
# Совместимо с системным bash (macOS 3.2+).

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_ID="$(date -u +%Y%m%d-%H%M%S)"
RUN_DIR="/tmp/pgw-testruns-${RUN_ID}"
DRY_RUN="${1:-}"

# Автодетект: все тестовые проекты из src/tests (каталог без csproj — фантом,
# отсеивается glob'ом). Утилита подготовки (PgWorker.E2ePrepare) и тестовая
# инфраструктура (Shared.TestInfra) в прогон не входят.
UNIT_PROJECTS=""
INTEGRATION_PROJECTS=""
E2E_PROJECTS=""
for csproj in "$REPO_ROOT"/src/tests/*/*.csproj; do
  name=$(basename "$csproj" .csproj)
  case "$name" in
    PgWorker.E2ePrepare | Shared.TestInfra) continue ;;
    *UnitTests)        UNIT_PROJECTS="$UNIT_PROJECTS $name" ;;
    *IntegrationTests) INTEGRATION_PROJECTS="$INTEGRATION_PROJECTS $name" ;;
    *E2eTests)         E2E_PROJECTS="$E2E_PROJECTS $name" ;;
  esac
done

# Последовательный поток (параллельно фону PgWorker.E2eTests): юниты →
# интеграционные → E2E остальных воркеров. Порядок — алфавитный (стабильный).
SEQUENTIAL=$(echo "$UNIT_PROJECTS $INTEGRATION_PROJECTS $(echo "$E2E_PROJECTS" | tr ' ' '\n' | grep -v '^PgWorker.E2eTests$' | tr '\n' ' ')" | tr -s ' ' | sed 's/^ //;s/ $//')

echo "[phase] run-dir: $RUN_DIR"
echo "[phase] plan: parallel=PgWorker.E2eTests"
echo "[phase] plan: sequential=[$SEQUENTIAL]"
if [ "$DRY_RUN" = "--dry-run" ]; then
  echo "[phase] dry-run: план показан, ничего не запущено"
  exit 0
fi

mkdir -p "$RUN_DIR"
SUMMARY="$RUN_DIR/summary.tsv"
printf 'project\texit\tduration\tresult\n' > "$SUMMARY"

# Одна серия: лог ЦЕЛИКОМ в файл (канон: вывод команд не теряется), факт —
# строка в summary. Возврат — код dotnet test.
run_project() {
  proj="$1"; log="$RUN_DIR/$1.log"
  echo "[phase] start $proj"
  start=$(date +%s)
  DOTNET_CLI_UI_LANGUAGE=en dotnet test "$REPO_ROOT/src/tests/$proj" \
    -c Release --no-build --nologo > "$log" 2>&1
  code=$?
  end=$(date +%s); dur=$((end - start))
  result=$(grep -E 'Passed!  -|Failed!  -' "$log" | tail -1 | cut -c1-160)
  printf '%s\t%s\t%ss\t%s\n' "$proj" "$code" "${dur}s" "${result:-<no summary line>}" >> "$SUMMARY"
  echo "[phase] done  $proj exit=$code ${dur}s"
  return $code
}

FAILURES=0

# 0) Внешние образы docker-серий — из локального registry (ранбук 02-registry-images).
if [ -x "$REPO_ROOT/dev-stand/images/pull-images.sh" ]; then
  echo "[phase] pull-images (registry-first)"
  "$REPO_ROOT/dev-stand/images/pull-images.sh" > "$RUN_DIR/pull-images.log" 2>&1 \
    || echo "[phase] WARN pull-images failed — см. $RUN_DIR/pull-images.log"
fi

# Предполёт: полный прогон не идёт поверх поднятого dev-станда (канон 04-e2e-runs).
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qE '^(as-etcd|adminpanel)'; then
  echo "[phase] ABORT: поднят dev-стенд (as-etcd/adminpanel в docker ps) — прогон поверх стенда запрещён"
  exit 2
fi

# 1) Сборка Release — ОТДЕЛЬНОЙ командой до всех серий (свежесть бинаря видна выводом).
echo "[phase] build Release (dotnet build src/PgWorker.slnx)"
DOTNET_CLI_UI_LANGUAGE=en dotnet build "$REPO_ROOT/src/PgWorker.slnx" -c Release --nologo \
  > "$RUN_DIR/build.log" 2>&1
if [ $? -ne 0 ]; then
  echo "[phase] ABORT: build failed — см. $RUN_DIR/build.log"
  tail -5 "$RUN_DIR/build.log"
  exit 2
fi

# 2) Статическая подготовка PgWorker E2E (штамп готовности; без неё серия падает пачкой).
echo "[phase] e2e-prepare (PgWorker.E2ePrepare)"
DOTNET_CLI_UI_LANGUAGE=en dotnet run --project "$REPO_ROOT/src/tests/PgWorker.E2ePrepare" -c Release \
  > "$RUN_DIR/e2e-prepare.log" 2>&1
if [ $? -ne 0 ]; then
  echo "[phase] ABORT: e2e-prepare failed — см. $RUN_DIR/e2e-prepare.log"
  exit 2
fi

# 3) PgWorker.E2eTests — фоновый поток (самая длинная серия, ~20–28 мин).
run_project "PgWorker.E2eTests" &
PARALLEL_PID=$!

# 4) Последовательный поток: юниты → интеграционные → E2E остальных
#    (параллельно фону из шага 3).
for proj in $SEQUENTIAL; do
  run_project "$proj" || FAILURES=$((FAILURES + 1))
done

# 5) Дождаться фоновую серию и забрать её исход.
echo "[phase] waiting for parallel PgWorker.E2eTests"
if wait "$PARALLEL_PID"; then :; else FAILURES=$((FAILURES + 1)); fi

# 6) Страховочный отчёт об остатках docker — БЕЗ удаления: зачистка own-only
#    (teardown серий); чужие контейнеры/стенд не трогаются (разбор — по отчёту).
LEFT=$(docker ps -a --format '{{.Names}}' 2>/dev/null | grep -cE '^(pgw-|kfw-)')
echo "[phase] docker leftovers (pgw-*/kfw-*, только отчёт, НЕ удаляются): ${LEFT:-0}"

# Сводка.
echo
echo "===== SUMMARY ($RUN_ID) ====="
cat "$SUMMARY"
echo "logs: $RUN_DIR"
if [ "$FAILURES" -ne 0 ]; then
  echo "RESULT: FAILED ($FAILURES series red)"
  exit 1
fi
echo "RESULT: OK (all series green)"
