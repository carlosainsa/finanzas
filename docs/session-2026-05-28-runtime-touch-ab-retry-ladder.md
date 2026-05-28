# Sesion 2026-05-28: Runtime Touch A/B Retry Ladder

**Fecha:** 2026-05-28 (jueves)  
**Hora finalizacion:** ~01:48 UTC  
**Estado:** Completado - Pendiente decision mañana  
**Tags:** `runtime-touch`, `ab-retry-ladder`, `batch-05`, `harvey-weinstein`, `dry-run`

---

## TL;DR para manana

1. Se investigo el exit code `20` → **NO es error** (es convencion de `data_lake_only`)
2. Se ejecuto el A/B retry ladder con batch-05 → **Solo encontro 2 assets** (Harvey Weinstein Yes/No)
3. El sistema dice: **NO ejecutar A/B todavia** → necesita captura fresca
4. **Comando listo abajo** (seccion "Proximo paso")

---

## Contexto: Por que estamos aqui

- Run anterior: `runtime-touch-discovery-loop-20260518T014847Z`
- Resultado anterior: `next_action = EXPAND_MARKET_DISCOVERY`
- Expasion ya ejecutada: `runtime-touch-discovery-expanded-20260527T222242Z`
- Batch seleccionado: **batch-05** (5 batches procesados, early stop en batch-05)

---

## Investigacion: Exit Code 20

**Pregunta:** Por que `research_exit_code.txt` dice `20` si todo parece OK?

**Respuesta:**
- En `scripts/run_real_dry_run_research.sh` linea 712-756, el modo `data_lake_only` escribe intencionalmente `20`
- Significa: "Datos exportados a DuckDB, research loop completo omitido intencionalmente"
- Se usa para evitar OOM (exit code 137) en pasos de seleccion
- **No bloquea** → todos los scripts aceptan exit code 20 como condicion no fatal

---

## Ejecucion: A/B Retry Ladder

**Comando ejecutado:**
```bash
EXECUTION_MODE=dry_run \
  scripts/run_runtime_touch_ab_retry_ladder.sh \
  --fresh-duckdb .tmp/real-dry-run-data-lake/runtime-touch-discovery-loop-20260527T222242Z-batch-05-fresh/research.duckdb \
  --fresh-report-root .tmp/real-dry-run-data-lake/runtime-touch-discovery-loop-20260527T222242Z-batch-05-fresh/reports/runtime-touch-discovery-loop-20260527T222242Z-batch-05-fresh \
  --duration-seconds 3600 \
  --fresh-capture-seconds 1800 \
  --min-assets 2 \
  --profile-a execution_probe_v11 \
  --profile-b execution_probe_v12
```

**Resultado del ladder:**

| Intento | Configuracion | Assets | Estado |
|---------|--------------|--------|--------|
| 1 | strict_signalable (density >= 0.05) | **0** | insufficient |
| 2 | wider_universe (limit 20) | **0** | insufficient |
| 3 | lower_signalable_density (>= 0.02) | **0** | insufficient |
| 4 | lower_signalable_snapshots (>= 1) | **0** | insufficient |
| 5 | runtime_hybrid_backfill | **2** | ready |

**Assets encontrados (solo con backfill):**
- Harvey Weinstein - Yes: `24327803960645909378149041810697343640752122608192367041827900158592826352552`
- Harvey Weinstein - No: `86488478623677188352872801318507143761188967461168408688159600382919967378486`

---

## Veredicto del Sistema

| Campo | Valor |
|-------|-------|
| `can_run_selected_ab` | `false` |
| `recommendation` | `COLLECT_FRESH_RUNTIME_SAMPLE_OR_CHANGE_MARKET_TIMING` |
| `route_result` | `DEPTH_STRICT_UNIVERSE_TOO_NARROW` |
| `requires_allow_gate_bypass` | `true` |

**Que significa:** Los datos de batch-05 tienen ~3 horas de antiguedad. El universo estricto (sin backfill) esta vacio ahora. El sistema pide datos frescos antes de correr A/B.

---

## Proximo paso (comando listo para copiar y pegar)

### Opcion A: Captura fresca (RECOMENDADA por el sistema)

```bash
cd /home/carlos_ainsa_gmail_com/finanzas

EXECUTION_MODE=dry_run \
  scripts/run_runtime_touch_ab_cycle.sh \
  --duration-seconds 3600 \
  --profile-a execution_probe_v11 \
  --profile-b execution_probe_v12 \
  --min-assets 2 \
  --runtime-touch-hybrid-backfill
```

**Nota:** Esto capturara datos nuevos durante 1 hora y luego comparara v11 vs v12.

### Opcion B: Bypass manual con datos existentes (NO recomendada)

Solo si se quiere forzar A/B con datos viejos:

```bash
cd /home/carlos_ainsa_gmail_com/finanzas

EXECUTION_PROBE_RUNTIME_TOUCH_HYBRID_BACKFILL=1 \
  EXECUTION_PROBE_UNIVERSE_LIMIT=20 \
  EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_DENSITY=0.02 \
  EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_SNAPSHOTS=3 \
  scripts/run_runtime_touch_ab_cycle.sh \
  --skip-fresh-capture \
  --fresh-duckdb .tmp/real-dry-run-data-lake/runtime-touch-discovery-loop-20260527T222242Z-batch-05-fresh/research.duckdb \
  --duration-seconds 3600 \
  --profile-a execution_probe_v11 \
  --profile-b execution_probe_v12 \
  --min-assets 2 \
  --runtime-touch-hybrid-backfill
```

---

## Archivos relevantes

| Archivo | Descripcion |
|---------|-------------|
| `.tmp/operational/runtime-touch-ab-retry-ladder-20260528T014753Z/runtime_touch_ab_retry_ladder/runtime_touch_ab_retry_ladder.json` | Reporte completo del ladder |
| `.tmp/operational/runtime-touch-discovery-expanded-20260527T222242Z/runtime_touch_discovery_loop_summary.json` | Resumen de la expansion |
| `.tmp/real-dry-run-data-lake/runtime-touch-discovery-loop-20260527T222242Z-batch-05-fresh/research.duckdb` | DuckDB con datos del batch-05 |
| `docs/session-2026-05-28-runtime-touch-ab-retry-ladder.md` | Este archivo |

---

## Notas para manana

- [ ] Decidir: captura fresca (Opcion A) o bypass manual (Opcion B)
- [ ] Si captura fresca: reservar ~1 hora de ejecucion
- [ ] Revisar si Harvey Weinstein sigue siendo un mercado activo/signalable
- [ ] Considerar si hay otros mercados en mejor estado para A/B

---

*Documento generado automaticamente al finalizar la sesion. No editar manualmente sin contexto.*
