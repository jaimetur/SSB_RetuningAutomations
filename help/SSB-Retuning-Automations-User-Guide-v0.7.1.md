# Technical User Guide — SSB Retuning Automations (v0.7.1)

## 1) Tool Overview

SSB Retuning Automations is an automation platform for SSB retuning projects. It supports GUI and CLI execution and orchestrates five functional modules:

- **Module 0**: Update Network Frequencies.
- **Module 1**: Configuration Audit & Logs Parser.
- **Module 2**: Consistency Check (manual PRE/POST comparison).
- **Module 3**: Bulk Consistency Check (automatic PRE/POST detection by market).
- **Module 4**: Final Clean-Up.

The main orchestration entry point is `src/SSB_RetuningAutomations.py`, where CLI/GUI arguments, configuration persistence, input resolution (folders/ZIP), module execution, and artifact versioning are handled.

---

## 2) Repository Technical Architecture

### 2.1 Orchestration Core
- `src/SSB_RetuningAutomations.py`: application entry point, CLI/GUI parsing, module routing, batch execution, and output versioning.

### 2.2 Business Modules
- `src/modules/ConfigurationAudit/ConfigurationAudit.py`: log parsing and audit workbook generation (Excel + PPT).
- `src/modules/ConfigurationAudit/ca_summary_excel.py`: `SummaryAudit` assembly and discrepancy dataframe generation.
- `src/modules/ConsistencyChecks/ConsistencyChecks.py`: PRE/POST loading, relation comparison, discrepancy detection, and exports.
- `src/modules/ProfilesAudit/ProfilesAudit.py`: profile audit logic (integrated into module 1).
- `src/modules/CleanUp/FinalCleanUp.py`: final cleanup stage (scaffold available for extension).

### 2.3 Shared Utilities
- `src/modules/Common/*.py`: correction command builders/exporters and shared module logic.
- `src/utils/*.py`: IO, parsing, frequencies, Excel helpers, pivots, sorting, infrastructure, and datetime utilities.

---

## 3) Inputs, Processing, and Outputs by Module

## 3.1 Module 0 — Update Network Frequencies

### Input
- Input folder (supports nested folders/ZIP as resolved by IO utilities).
- Logs containing table `NRFrequency` and column `arfcnValueNRDl`.

### Processing
1. Scan logs and detect `NRFrequency` blocks.
2. Extract numeric values from `arfcnValueNRDl`.
3. Remove duplicates and sort frequency values.
4. Persist updated network frequency values for GUI/CLI reuse.

### Output
- No Excel/PPT generation.
- Updated persisted network frequency configuration.

---

## 3.2 Module 1 — Configuration Audit & Logs Parser

### Inputs
- Input folder with logs (`.log`, `.logs`, `.txt`) or supported ZIP files.
- Frequency parameters:
  - `n77_ssb_pre`
  - `n77_ssb_post`
  - `n77b_ssb`
  - allowed SSB/ARFCN lists (pre/post).
- Flags:
  - `profiles_audit`
  - `frequency_audit`
  - `export_correction_cmd`
  - `fast_excel_export`

### Processing
1. Parse files and extract MO tables by `SubNetwork` blocks.
2. Generate one worksheet per detected MO table.
3. Build `SummaryAudit` and auxiliary summary/pivot data.
4. Run profile audit if enabled.
5. Export CA correction commands if requested.
6. Generate a summary PPT.

### Outputs
- Folder: `ConfigurationAudit_<timestamp>_v<version>/`
- Excel: `ConfigurationAudit_<timestamp>_v<version>.xlsx`
- PowerPoint: `ConfigurationAudit_<timestamp>_v<version>.pptx`
- Optional folder: `Correction_Cmd_CA/` (AMOS correction commands).

---

## 3.3 Module 2 — Consistency Check (PRE/POST)

### Inputs
- `input_pre` and `input_post` folders.
- Frequencies `n77_ssb_pre` and `n77_ssb_post`.
- Optional PRE/POST ConfigurationAudit references for target classification.
- Optional frequency filters (`cc_freq_filters`).

### Processing
1. Load relation tables (`GUtranCellRelation`, `NRCellRelation`).
2. Normalize keys/columns and select latest snapshots.
3. Detect:
   - new relations,
   - missing relations,
   - parameter discrepancies,
   - frequency discrepancies,
   - PRE/POST frequency summary.
4. Classify destination targets as `SSB-Pre`, `SSB-Post`, or `Unknown`.
5. Export detailed Excel outputs and correction commands.

### Outputs
- `CellRelation_<timestamp>_v<version>.xlsx`
- `ConsistencyChecks_CellRelation_<timestamp>_v<version>.xlsx`
- `Correction_Cmd_CC/` with commands grouped by discrepancy type.

---

## 3.4 Module 3 — Bulk Consistency Check

### Input
- Root folder containing subfolders with naming convention: `yyyymmdd_hhmm_step0` (optionally grouped by market).

### Processing
1. Detect best PRE/POST candidates by timestamp.
2. Exclude blacklisted folders (e.g., `ignore`, `old`, `bad`, `partial`, `incomplete`, `discard`).
3. Execute module 2 for each detected market.

### Outputs
- Same output structure as module 2, generated per market.
- Traceability file: `FoldersCompared.txt`.

---

## 3.5 Module 4 — Final Clean-Up

### Input
- Final retuning working folder.

### Processing
- Apply final cleanup policies (framework ready for additional rules).

### Output
- Versioned cleanup output directory, depending on active implementation.

---

## 4) SummaryAudit Deep Dive (Module 1)

### 4.1 Evaluation Model
`build_summary_audit()` composes high-level checks grouped by categories:
1. Exclude `UNSYNCHRONIZED` nodes according to `MeContext`.
2. Evaluate NR, LTE, ENDC, External, TermPoint, cardinality, and profile checks.
3. Register each check row as `Category/SubCategory/Metric/Value/ExtraInfo`.

### 4.2 Main SummaryAudit Categories

- **MeContext Audit**: total unique nodes and unsynchronized node exclusion.
- **NR Frequency Audit / Inconsistencies**: N77 distribution, allowed-list compliance, relation quality, externals/termpoints target consistency.
- **LTE Frequency Audit / Inconsistencies**: old/new LTE SSB usage, expected set compliance, relation parameter inconsistencies, externals in out-of-service scenarios.
- **ENDC Audit / Inconsistencies**: old/new/N77B combination validation for `EndcDistrProfile` and `FreqPrioNR`.
- **Cardinalities Audit / Inconsistencies**: cardinality checks by node/cell for relation tables.
- **Profiles Audit** (optional): parameter drift checks on supported profile MOs.

### 4.3 Operational Meaning of Columns
- **Category**: audited technical domain.
- **SubCategory**: audit mode (Audit/Inconsistencies/Profiles).
- **Metric**: concrete rule being evaluated.
- **Value**:
  - integer count of impacted nodes/relations/cells,
  - `N/A` when missing data prevents evaluation,
  - status/error text if applicable.
- **ExtraInfo**: condensed context (typically impacted NodeIds).

---

## 5) Consistency Check Deep Dive

### 5.1 Parameter Discrepancy Detection
1. Select common PRE/POST relations by composite key:
   - GU: `NodeId`, `EUtranCellFDDId`, `GUtranCellRelationId`
   - NR: `NodeId`, `NRCellCUId`, `NRCellRelationId`
2. Exclude non-business comparison columns (keys, date markers, pre/post markers, frequency helpers).
3. Compare shared columns value-by-value.
4. Set `ParamDiff=True` when at least one compared field differs.
5. For GU, ignore noisy fields such as `timeOfCreation` and `mobilityStatusNR`.

### 5.2 Frequency Discrepancy Detection
1. Extract relation base frequencies with parser helpers.
2. Mark `FreqDiff=True` when PRE was in `freq_before` or `freq_after`, but POST is not in target `freq_after`.
3. Classify as:
   - `FreqDiff_SSBPost`
   - `FreqDiff_Unknown`

### 5.3 Neighborhood Discrepancy Groups
- **New relations**: present in POST, missing in PRE.
- **Missing relations**: present in PRE, missing in POST.
- **Discrepancies**: present in both snapshots but with parameter/frequency differences.

### 5.4 Filtering Non-Retuned Nodes
When POST `SummaryAudit` is available, the module can reduce operational noise by filtering discrepancies that point to nodes that did not complete retuning.

---

## 6) Execution Modes and Versioning

- **GUI mode**: run without CLI arguments.
- **CLI mode**: run with explicit module and options.
- Generated artifacts include a versioned suffix: `<timestamp>_v<TOOL_VERSION>`.
- This guarantees traceability and avoids collisions between runs.

---

## 7) Practical Recommendations

- Run **Configuration Audit** before Consistency Checks whenever possible.
- Validate frequency inputs (`n77_ssb_pre`, `n77_ssb_post`, `n77b_ssb`) before batch execution.
- Use Bulk mode only with a controlled folder naming convention.
- Review `Summary` sheets first, then deep-dive into discrepancy tabs and generated correction commands.
