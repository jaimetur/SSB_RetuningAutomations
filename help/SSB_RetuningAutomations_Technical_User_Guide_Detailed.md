# Technical User Guide (Detallada) — SSB_RetuningAutomations

## 1) Visión global de la herramienta

SSB_RetuningAutomations es una plataforma de automatización para proyectos de retuning SSB que puede ejecutarse en modo GUI o CLI y orquesta cinco módulos funcionales:

- **Módulo 0**: Update Network Frequencies.
- **Módulo 1**: Configuration Audit & Logs Parser.
- **Módulo 2**: Consistency Check (Pre/Post manual).
- **Módulo 3**: Consistency Check Bulk (autodetección Pre/Post por mercado).
- **Módulo 4**: Final Clean-Up.

La ejecución principal vive en `src/SSB_RetuningAutomations.py`, donde se gestionan argumentos CLI, GUI, persistencia de configuración, resolución de inputs (carpetas/ZIP), ejecución por módulo y versionado de artefactos.

---

## 2) Arquitectura técnica del repositorio

### 2.1 Núcleo de orquestación
- `src/SSB_RetuningAutomations.py`: punto de entrada, parseo CLI/GUI, enrutamiento a módulos, ejecución batch, bulk y versionado.

### 2.2 Módulos de negocio
- `src/modules/ConfigurationAudit/ConfigurationAudit.py`: parseo de logs y construcción de libro de auditoría (Excel + PPT).
- `src/modules/ConfigurationAudit/ca_summary_excel.py`: ensamblado de `SummaryAudit` y dataframes de discrepancias.
- `src/modules/ConsistencyChecks/ConsistencyChecks.py`: carga PRE/POST, comparación de relaciones, discrepancias y exportación de salidas.
- `src/modules/ProfilesAudit/ProfilesAudit.py`: auditoría de perfiles (integrada en módulo 1).
- `src/modules/CleanUp/FinalCleanUp.py`: clean-up final (base implementada para extensión).

### 2.3 Capa común y utilidades
- `src/modules/Common/*.py`: lógica de comandos de corrección y funciones comunes.
- `src/utils/*.py`: IO, parseo, frecuencia, Excel, pivots, ordenación, infraestructura y tiempos.

---

## 3) Entradas, salidas y contenido por módulo

## 3.1 Módulo 0 — Update Network Frequencies

### Entrada
- Carpeta de input (puede contener subcarpetas/ZIP ya soportados por la capa IO).
- Logs con tabla `NRFrequency` y columna `arfcnValueNRDl`.

### Proceso
1. Recorre logs y detecta bloques `NRFrequency`.
2. Extrae valores numéricos de `arfcnValueNRDl`.
3. Elimina duplicados y ordena frecuencias.
4. Actualiza la configuración persistida de "Network frequencies" para GUI/CLI.

### Salida
- No genera Excel/PPT.
- Actualiza el valor persistido de frecuencias de red para filtros y selección en ejecuciones posteriores.

---

## 3.2 Módulo 1 — Configuration Audit & Logs Parser

### Entradas
- Carpeta input con logs (`.log`, `.logs`, `.txt`) o ZIPs resolubles por utilidades.
- Parámetros de frecuencia:
  - `n77_ssb_pre`
  - `n77_ssb_post`
  - `n77b_ssb`
  - listas permitidas de SSB/ARFCN pre/post.
- Flags:
  - `profiles_audit`
  - `frequency_audit`
  - `export_correction_cmd`
  - `fast_excel_export`.

### Proceso
1. Parsea archivos y extrae tablas MO por bloques `SubNetwork`.
2. Genera una hoja por tabla detectada.
3. Construye `SummaryAudit` + pivots/resúmenes auxiliares.
4. Ejecuta auditoría de perfiles si está habilitada.
5. Exporta comandos de corrección CA si se solicita.
6. Genera PPT resumen.

### Salidas
- Carpeta `ConfigurationAudit_<timestamp>_v<version>/`.
- Archivo Excel `ConfigurationAudit_<timestamp>_v<version>.xlsx`:
  - Hojas por cada tabla MO parseada.
  - `SummaryAudit`.
  - Hojas de discrepancias NR/LTE de parámetros.
  - Hojas de resumen/pivot por frecuencias y relaciones.
- Archivo PPT `ConfigurationAudit_<timestamp>_v<version>.pptx`.
- Carpeta opcional `Correction_Cmd_CA/` con comandos AMOS.

### Contenido semántico principal
- **SummaryAudit** contiene filas con:
  - `Category`, `SubCategory`, `Metric`, `Value`, `ExtraInfo`,
  - y campos de contexto de ejecución (stage, módulo, etc. según flujo).
- `Value` suele representar conteo de nodos/celdas/relaciones impactadas.
- `ExtraInfo` contiene lista de NodeIds o detalle sintético de discrepancias.

---

## 3.3 Módulo 2 — Consistency Check (Pre/Post)

### Entradas
- `input_pre` y `input_post` (o estructura equivalente resuelta).
- Frecuencias `n77_ssb_pre` y `n77_ssb_post`.
- Referencia opcional a `ConfigurationAudit` PRE y POST para enriquecer clasificación de targets.
- Lista opcional de filtros de frecuencia (`cc_freq_filters`).

### Proceso
1. Carga tablas de relación (`GUtranCellRelation`, `NRCellRelation`).
2. Normaliza columnas/keys y selecciona snapshots más recientes por fecha.
3. Calcula:
   - relaciones nuevas,
   - relaciones perdidas,
   - discrepancias de parámetros,
   - discrepancias de frecuencia,
   - resumen por par de frecuencias PRE/POST.
4. Enriquece con clasificación de destino `SSB-Pre`, `SSB-Post`, `Unknown`.
5. Exporta excel principal + excel de discrepancias y comandos de corrección.

### Salidas
- `CellRelation_<timestamp>_v<version>.xlsx` (visión integral de relaciones).
- `ConsistencyChecks_CellRelation_<timestamp>_v<version>.xlsx` con:
  - `Summary`
  - `SummaryAuditComparisson` (si hay SummaryAudit PRE/POST)
  - `Summary_CellRelation`
  - bloques GU: `GU_relations`, `GU_param_disc`, `GU_freq_disc`, `GU_freq_disc_unknown`, `GU_missing`, `GU_new`
  - bloques NR: `NR_relations`, `NR_param_disc`, `NR_freq_disc`, `NR_freq_disc_unknown`, `NR_missing`, `NR_new`
  - opcionales `GU_all`, `NR_all`.
- `Correction_Cmd_CC/` con comandos por tipo (new/missing/discrepancies).

---

## 3.4 Módulo 3 — Consistency Check Bulk

### Entradas
- Carpeta raíz con subcarpetas tipo `yyyymmdd_hhmm_step0` (opcionalmente anidadas por mercado).

### Proceso
1. Detecta candidatos PRE/POST por fecha/hora más apropiadas.
2. Excluye carpetas por blacklist (`ignore`, `old`, `bad`, `partial`, `incomplete`, `discard`, etc.).
3. Ejecuta Módulo 2 por cada mercado detectado.

### Salidas
- Misma estructura de salidas del módulo 2, por mercado.
- Fichero de trazabilidad `FoldersCompared.txt`.

---

## 3.5 Módulo 4 — Final Clean-Up

### Entradas
- Carpeta de trabajo final de retune.

### Proceso
- Ejecuta políticas de limpieza finales (estructura preparada para ampliar reglas).

### Salidas
- Directorio versionado de clean-up según implementación activa.

---

## 4) Módulo 1 en detalle: Summary Audit

## 4.1 Filosofía de evaluación
`build_summary_audit()` construye una tabla de chequeos de alto nivel por categorías. El flujo:
1. Excluye nodos `UNSYNCHRONIZED` según `MeContext`.
2. Evalúa tablas NR, LTE, ENDC, Externals, TermPoints, cardinalidades y perfiles.
3. Registra cada chequeo como fila (`Category/SubCategory/Metric/Value/ExtraInfo`).

## 4.2 Catálogo de chequeos del SummaryAudit

### A) MeContext Audit
- Total de nodos únicos.
- Nodos UNSYNCHRONIZED (excluidos del resto de auditorías).

### B) NR Frequency Audit / NR Frequency Inconsistencies
**Tablas fuente**: `NRCellDU`, `NRFrequency`, `NRFreqRelation`, `NRSectorCarrier`, `NRCellRelation`, `ExternalNRCellCU`, `TermPointToGNodeB`, `TermPointToGNB`.

Chequeos principales:
- Detección de nodos NR con SSB N77 (banda 646600–660000).
- Clasificación de nodos NR LowMidBand / mmWave / mixtos.
- Nodos cuyas SSB N77 están totalmente en listas permitidas PRE o POST.
- Nodos con SSB N77 fuera de listas permitidas.
- Presencia old/new SSB por nodo (solo old, solo new, ambos).
- Nodos con NRFreqRelationId con formato no esperado (autocreados fuera convención).
- Relaciones NR a old/new SSB.
- Externos NR y termpoints apuntando a old/new/unknown.

**Disparo típico**:
- Cada check se activa si tabla y columnas mínimas existen.
- Si faltan columnas, se añade fila de estado `N/A`.
- Si tabla vacía o no encontrada, se añade fila informativa `Table not found or empty`.

**Interpretación**:
- `Value > 0` en inconsistencias indica desviación real que requiere investigación.
- `ExtraInfo` suele listar nodos afectados para targeting operativo.

### C) LTE Frequency Audit / LTE Frequency Inconsistencies
**Tablas fuente**: `GUtranSyncSignalFrequency`, `GUtranFreqRelation`, `GUtranCellRelation`, `ExternalGUtranCell`, `TermPointToENodeB`.

Chequeos principales:
- Nodos LTE con old/new SSB.
- Nodos con ambos old/new o old sin new.
- SSB fuera del set esperado pre/post.
- Relaciones LTE a old/new y discrepancias de parámetros por cell relation.
- Externos LTE OUT_OF_SERVICE para old/new.

### D) ENDC Audit / ENDC Inconsistencies
**Tablas fuente**: `EndcDistrProfile`, `FreqPrioNR`.

Chequeos principales:
- `gUtranFreqRef` y `mandatoryGUtranFreqRef` con combinaciones old/new + N77B.
- Nodos que no contienen combinación esperada de frecuencias.
- En `FreqPrioNR`: old sin new, ambos presentes, y mismatch de parámetros por celda.

### E) Cardinalities Audit / Inconsistencies
Chequeos de cardinalidad por tabla relación (por nodo y/o por celda) para detectar sobreaprovisionamiento o huecos respecto a límites esperados.

### F) Profiles Audit (si habilitado)
- Compara perfiles PRE/POST por MO de perfiles soportados.
- Detecta discrepancias de parámetros entre variantes old/new.
- Añade resultados al SummaryAudit y hojas auxiliares de detalle.

## 4.3 Significado operativo de las filas del SummaryAudit
- **Category**: dominio técnico auditado (NR/LTE/ENDC/MeContext/etc.).
- **SubCategory**: tipo de análisis (Audit/Inconsistencies/Profiles).
- **Metric**: regla concreta evaluada.
- **Value**:
  - Entero: cantidad de nodos/relaciones/celdas afectadas.
  - `N/A`: no evaluable por ausencia de columnas.
  - Texto: estado o error capturado.
- **ExtraInfo**: lista de nodos o detalle acotado para troubleshooting.

---

## 5) Módulo Consistency Check en detalle

## 5.1 Cómo detecta discrepancias de parámetros
1. Selecciona relaciones comunes PRE y POST por clave compuesta:
   - GU: típicamente `NodeId`, `EUtranCellFDDId`, `GUtranCellRelationId`.
   - NR: típicamente `NodeId`, `NRCellCUId`, `NRCellRelationId`.
2. Excluye columnas de control (keys, frecuencia, Pre/Post, Date).
3. Compara valor a valor en columnas compartidas.
4. Marca `ParamDiff=True` si existe al menos una columna distinta.
5. En GU ignora `timeOfCreation` y `mobilityStatusNR` para evitar falsos positivos.

## 5.2 Cómo detecta discrepancias de frecuencia
1. Extrae frecuencia base de las referencias de relación (`extract_gu_freq_base` / `extract_nr_freq_base`).
2. Regla de discrepancia:
   - si PRE tenía `freq_before` o `freq_after`, y POST **no** queda en `freq_after`, marca `FreqDiff=True`.
3. Clasifica la discrepancia como:
   - `FreqDiff_SSBPost` (objetivo identificado como SSB-Post),
   - `FreqDiff_Unknown` (no se puede asociar a target conocido).

## 5.3 Cómo detecta discrepancias de vecindades
Se separan en tres grupos:
- **New relations**: claves presentes en POST y ausentes en PRE.
- **Missing relations**: claves presentes en PRE y ausentes en POST.
- **Discrepancies**: misma clave en PRE/POST pero con diferencias paramétricas o de frecuencia.

## 5.4 Filtrado por nodos no retuneados
Si existe SummaryAudit POST, el módulo obtiene listas de nodos PRE/POST y puede excluir discrepancias cuyo destino apunte a nodos que no completaron retune, reduciendo ruido operativo.

## 5.5 Contenido de cada hoja del output ConsistencyChecks
- **Summary**: KPIs por tabla (volumen PRE/POST, discrepancias, nuevas/perdidas, archivos fuente).
- **SummaryAuditComparisson**: diff de métricas de SummaryAudit PRE vs POST (sin `ExtraInfo` para mantener comparativa limpia).
- **Summary_CellRelation**: KPI por par `Freq_Pre/Freq_Post` y por tecnología.
- **GU_relations / NR_relations**: universo de relaciones con enriquecimiento de target y command snippets.
- **GU_param_disc / NR_param_disc**: relaciones comunes con diferencia paramétrica.
- **GU_freq_disc / NR_freq_disc**: discrepancias de frecuencia a target SSB-Post.
- **GU_freq_disc_unknown / NR_freq_disc_unknown**: discrepancias con target no clasificable.
- **GU_missing / NR_missing**: relaciones eliminadas respecto PRE.
- **GU_new / NR_new**: relaciones añadidas en POST.
- **GU_all / NR_all**: dump consolidado opcional para análisis extendido.

---

## 6) Requisitos de entrada y buenas prácticas de operación

- Mantener exportes de logs por mercado en estructura consistente (especialmente para bulk).
- Validar que PRE/POST tengan la misma granularidad de tablas y naming coherente.
- Configurar correctamente listas permitidas de SSB/ARFCN para minimizar falsos positivos.
- Revisar primero `Summary` y `Summary_CellRelation`, después ir a hojas de detalle.
- Consumir `Correction_Cmd_CA` y `Correction_Cmd_CC` como propuesta de remediación, no como ejecución ciega.

---

## 7) Limitaciones conocidas y consideraciones

- El motor depende de la calidad y estructura de logs: columnas faltantes degradan chequeos a `N/A`.
- Algunas reglas dependen de convenciones de naming en referencias (NR/GU relation refs).
- El módulo Final Clean-Up está preparado para extender políticas específicas por operación.

---

## 8) Referencia rápida de módulos

| Módulo | Entrada principal | Salida principal | Objetivo |
|---|---|---|---|
| 0 Update Network Frequencies | Carpeta logs | Config persistida | Actualizar lista de frecuencias de red |
| 1 Configuration Audit | Carpeta logs/ZIP | Excel + PPT + comandos CA | Auditar configuración y perfiles |
| 2 Consistency Check | Carpeta PRE y POST | 2 Excel + comandos CC | Comparar relaciones pre/post |
| 3 Consistency Bulk | Carpeta raíz multi-mercado | Salidas módulo 2 por mercado | Ejecutar comparación masiva |
| 4 Final Clean-Up | Carpeta final | Carpeta clean-up | Limpieza final operativa |

