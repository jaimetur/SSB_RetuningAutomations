# 🗓️ CHANGELOG
[Planned Roadmap](https://github.com/jaimetur/SSB_RetuningAutomations/blob/main/ROADMAP.md) for the following releases
[Changelog](https://github.com/jaimetur/SSB_RetuningAutomations/blob/main/CHANGELOG.md) for the past releases

---

## Release: v0.7.2
### Release Date: 2026-02-15
  
- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
  - Added admin DB backup import/export panel and update changelog.
  - Added MeContext workflow and priority columns for ConfigurationAudit.

- #### 🚀 Enhancements:
  - Moved documentation panels to end and update label text.
  - Improved guide version migration and add version updater GUI.
  - Refined panel scroll and replace selection buttons with scope combos.
  - Polished documentation panel behavior and add PDF user guide output.
  - Improved get_resource_path() function to accept a base_dr parameter.

- #### 🐛 Bug fixes:
  - Fixed templates_pptx folder.
  - Fixed panel scrolling and selection size metadata in user/admin menus

- #### 📚 Documentation: 
  - Improved docs links and admin/user history panel actions.
  - Updated README and related docs with all the latest changes.

---

## Release: v0.7.1
### Release Date: 2026-02-13
  
- #### 🚨 Breaking Changes:
  - Renamed the web layer from `webapp` to `web_interface` and updated related paths, scripts and references across the project.

- #### 🌟 New Features:
  - Added queued execution flow in the Web Interface to process jobs asynchronously.
  - Added a shared Inputs Repository in the Web Interface to upload once and reuse inputs in subsequent executions.
  - Added `--output` override in CLI and integrated the same output-root behavior in the Web Interface.
  - Improved Administrator panel controls for queue execution resources (%CPU, %RAM and max threads).
  - Added sortable columns for executions and inputs panels.
  - Adjust output folder naming and keep only zipped artifacts.
  - Added log delete and panel toggles for user/admin dashboards.
  - Reset input paths to empty by default and after each run.
  - Split API and web access logs with admin-only access audit view.
  - User Guide Automatically generated from Markdown files.
  - Added Documentation panels in user and admin web dashboards with direct links to User Guides in `.md`, `.docx`, and `.pptx`.
  - Added administrator database backup controls to export and import the Web Interface SQLite database from the Admin panel.

- #### 🚀 Enhancements:
  - Reworked Web Interface UX with improved collapsible panels and modal dialogs (including explicit accept actions) for clearer user flows.
  - Improved Inputs Repository workflow with better permissions/error feedback and smarter selection behavior.
  - Added PRE/POST-aware filtering for Module 2 repository selection to reduce invalid input picks.
  - Centralized execution artifacts under `data/outputs` and aligned run-size calculations to that outputs-only model.
  - Hardened Docker startup scripts/deploy flow to better handle stale container conflicts and dual deployment modes.
  - Improved batch confirmations, modal UX, and zip input storage.
  - Set default table sorting and auto-refresh run statuses.
  - Clean temp queue folders and refactor inputs repository send flow.
  - Validate input target mapping by module before send.
  - Style invalid target modal as error.
  - Highlight canceled runs with a dedicated background color.
  - Show execution start time in admin execution log selector.
  - Standardized log timestamp formatting in Web Interface logs to omit milliseconds for better readability and audit consistency.

- #### 🐛 Bug fixes:
  - Fixed missing login routes in the Web Interface.
  - Fixed Administrator panel loading issues.
  - Fixed `sqlite3.Row` handling in `compute_runs_size` for frontend pages.
  - Fixed input repository workflow edge cases detected during frontend administration flows.
  - Fixed input folder zip layout and reliable run confirmation/status.
  - Fixed output artifact capture and separate batch output roots.
  - Fixed output-root usage and robustly detect per-run output.
  - Fixed parallel run isolation and add stop action for executions.
  - Fixed queued run output finalization and live log selection.
  - Ensure queue_task folders are always promoted to final output.
  - Fixed active run tracking in executions logs panel.
  - Added explicit Web Access audit events for manual logout and inactivity logout, including inactivity duration details.
    
- #### 📚 Documentation: 
  - Added FastAPI API discovery documentation for the Web Interface backend.
  - Standardized terminology from "Web Frontend" to "Web Interface" across documentation.
  - Updated docs and references after renaming `webapp.py` to `web_interface.py`.
  - Updated README and related docs with all the latest changes.

---

## Release: v0.7.0
### Release Date: 2026-02-11
  
- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
  - Added a new webapp frontend (using docker) to use the tool from a browser.
  - Web Interface now supports uploading MO files/ZIPs as inputs and downloading exports from the Latest Runs panel.
  - New docker image with the whole application (GUI + CLI) available on DockerHub.

- #### 🚀 Enhancements:
  - Disabled the `Consistency Check (Bulk mode)` module in the Web Interface due to incompatibility.

- #### 🐛 Bug fixes:
  - Minor bug fixing.
    
- #### 📚 Documentation: 
  - Updated documentation with the latest changes.

---

## Release: v0.6.5
### Release Date: 2026-02-10
  
- #### 🚨 Breaking Changes:

- #### 🌟 New Features:

- #### 🚀 Enhancements:
  - Added an extra NRCellRelation correction command to set acaMode (e.g., AUTO) at the end of the generated script.
  - Renamed MeContext enriched audit columns to the short header names (yellow/green) instead of the long ‘from table’ descriptions.

- #### 🐛 Bug fixes:
  - Minor bug fixing.
    
- #### 📚 Documentation: 
  - Updated documentation with latest changes.

---

## Release: v0.6.4
### Release Date: 2026-02-06

- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
    - **ConfigurationAudit:** SummaryAudit: For NR and LTE cell rows, the ExtraInfo field now contains a list of NODES instead of cells.
    - **ConfigurationAudit:** SummaryAudit: EndcDistrProfile: Added 3 new rows for mandatoryGUtranFreqRef (following the gUtranFreqRef format) and enable support for additional frequencies.
    - **ConfigurationAudit:** LTE Param Mismatching: endcB1MeasPriority is no longer be handled as a "mismatch." Instead, report it as 2 separate cases and include a SummaryAudit with a list of nodes.
    - **ConfigurationAudit:** Implemented MeContext loading, exclude UNSYNCHRONIZED across all audits, and enrich the MeContext worksheet.
    - **ConfigurationAudit:** Included MeContext in SummaryAudit and implemented a double-check by excluding UNSYNCHRONIZED inside the builder.
    - **ConfigurationAudit:** MeContext sheet enriched with additional columns (as per requirements).
    - **ConfigurationAudit:** Added new SummaryAudit profile checks for the new MOs CaCellProfile and CaCellProfileUeCfg. These checks mirror the existing TrStSaCellProfile / TrStSaCellProfileUeCfg logic, producing the same Profiles Inconsistencies and Profiles Discrepancies rows. Updated the profiles table collection/parsing so these MOs are included in the audit inputs and appear in the SummaryAudit output in the expected order.

- #### 🚀 Enhancements:
    - Execution Log now shows all MO parsed and its parsing time.

- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.6.3
### Release Date: 2026-02-03

  
- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
    - Now `ConfigurationAudit` and `Final Clean-up` modules supports multiple Input folders from GUI (using Add button or selecting several subfolders within the same folder) and also from CLI (using `--inputs` argument instead of `--input`) to process all of them in batch mode.
    - Now `ConfigurationAudit` and `Final Clean-up` modules supports multi-select valid `Step0` subfolders.
    - `NRCellDU` now updates `ssbFrequency` column when the value is 0 and `ssbFrequencyAutoSelected` is not 0. 
    - New Flag on GUI (NR/LTE Frequency Audits" and CLI (`--frequency-audit`) to Include/Exclude `NRFrequency` and `GUtranSyncSignalFrequency` Categories on SummaryAudit and PPT.
    - Included a new Selectable dialog to select wich folders do you want to re-Run `ConfigurationAudit` module when a previous Audit (with the same version) have been found (This new dialog only appears in batch mode).
    - Included Tool logo on GUI launcher dialog.

- #### 🚀 Enhancements:
    - `ConfigurationAudit` module now detects Market name properly even if it is not delimited by `_` but if it appears after `Step0_` (i.e: 20260114_0728_Step0_Mkt188 → Market Mkt188).
    - Enhanced Correction_Cmd columns in `ConsistencyCheck` module to add the fix commands of header/footer of avery node text file:
      - confb+
      - gs+
      - lt all
      - alt
    - Enhanced GU_Relations and NR_Relations sheets from `ConsistencyCheck` module with additional columns to align with `ConfigurationAudit` module:
      - ExternalGNodeBFunction / ExternalGNBCUCPFunction
      - ExternalGUtranCell / ExternalNRCellCU
      - GNodeB_SSB_Target
      - Correction_Cmd
    - Changed timestamp of output files for PRE in `ConsistencyCheck` module to match with the original folder. 
    - `CoonsistencyCheck` module, now ignore columns ["timeOfCreation", "mobilityStatusNR"] from Param Discrepancies checks.
    - Improved Auto-detection of PRE/POST folder in `ConsistencyCheck (bulk)` mode, to supports several levels of subfolders.
    - Enhanced main launcher dialog layout.

- #### 🐛 Bug fixes:
    - Fix bug in `ConsistencyCheck` module where the SSB-Unknown mask was not being applied to `Summary_CellRelation` sheet so the stats show in this table did not match with the real discrepancies shown in `_disc` table. 
    - Fix bug in `ConsistencyCheck` module where the Freq_Pre/Freq_Post was not being properly extracted when the SSB freq was something like `autoXXXXX_YYY` in `Summary_CellRelation` sheet.
    - Other fixes in `ConsistencyCheck` module to align stats of Summary table with real discrepancies (filtered by SSB-Post in other tables).
    - Fix bug on when using Multi-subfolder selection on `ConsistencyCheck (bulk)` module. It has no sense to use this feature for this module. Disabled.
    - Fix auto-detect Step0 folder to detect folders with `Step0` in the folder name and valid logs files inside (in .txt/.log/.zip formats).
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.6.2
### Release Date: 2026-02-01

- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
    - New module `0. Update Network Frequencies` to update the Frequency List from the Network. For this module to work you need to provide an Input Folder with a valid log for the MO NRFrequency.

- #### 🚀 Enhancements:
    - Updated Correction Commands for MO `NRCellRelation`.
    - Updated Correction Commands for MO `GUtranCellRelation`.
    - Updated Correction Commands for MO `ExternalGUtranCell`.
    - Moved `GNodeB_SSB_Target` column beside `SSB needs update` columnd in MO table `TermpointToGNB`.

- #### 🐛 Bug fixes:
    - Fixed a pandas error in `ConsistencyCheck` module by avoiding boolean evaluation of DataFrames when selecting cached SummaryAudit data (replaced df1 or df2 with an explicit None check).
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.6.1
### Release Date: 2026-01-30

- #### 🚨 Breaking Changes:
    - New flag `--fast-excel` (also available on GUI) to enable/disable a new Excel writer engine (xlsxwriter) to speed up Excel exports (saving to Excel takes approximately half the time compared to the default engine). (NOTE: This engine has limited formatting support and does not support applying styles such as different row colors in Excel).
    - New flag `--profiles-audit` (also available on GUI) to enable/sisable `ProfilesAudit` during a `ConfigurationAudit` execution.

- #### 🌟 New Features:
    - When running `ConfigurationAudit`, if the tool finds a previously generated `ConfigurationAudit` folder in the input directory created with the same tool version, it prompts the user to decide whether to run the audit again. (Note: in batch mode, the tool automatically skips folders that already contain a `ConfigurationAudit` generated with the same tool version.)
    - Integrated a new Excel writer engine (xlsxwriter) to speed up Excel exports (saving to Excel takes approximately half the time compared to the default engine). You can enable it via the new `--fast-excel` CLI flag or from the GUI. Important: this engine has limited formatting support and does not support applying styles such as different row colors in Excel.

- #### 🚀 Enhancements:
    - Unified style_headers_autofilter_and_autofit to apply styles to sheets from `ConfigurationAudit` and `ConsistencyCheck`.

- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.6.0
### Release Date: 2026-01-28

- #### 🚨 Breaking Changes:
    - All Correction Commands files(except relations missing/new/discrepancies) are now generated by `ConfigurationAudit` module instead of `ConsistencyCheck` module.
    - Correction Commands generated by `ConfigurationAudit` are now created within folder `Correction_Cmd_CA` and in ZIP format by default.
    - Correction Commands generated by `ConsistencyCheck` are now created within folder `Correction_Cmd_CC` and in ZIP format by default.
    - New flag `--export-correction-cmd` (also available on GUI) to enable the export of Correction Commands during `ConfigurationAudit`module. (NOTE: This flag is disabled by default on CLI and need to be included if you want to export the Correction Command files).

- #### 🌟 New Features:
    - Merge Excel sheets for the same MO instead add (1), (2)… in 'ConfigurationAudit' module.
    - Added a hyperlink in cell A1 on every sheet from `ConfigurationAudit` Excel to jump back to `SummaryAudit`.
    - Added a hyperlink in cell A1 on every sheet from `ConsistencyCheck` Excel to jump back to `Summary_CellRelation`.
    - Now the execution log is also available in output folder for an easier way to identify which log belong to each execution.

- #### 🚀 Enhancements:
    - **ConsistencyChecks module:**
      - ConsistencyChecks exports now all Correction Commands to `Correction_Cmd_CC` folder instead of `Correction_Cmd`. 
      - In ConsistencyChecks, export ONLY neighbor-relation commands (because they require comparing 2 audits):
        - NR_new / NR_missing / NR_disc
        - GU_new / GU_missing / GU_disc 
      - Stop exporting External/Termpoints from ConsistencyChecks (to prevent duplicates). External/Termpoints moved to ConfigurationAudit export. 
      - Avoid to execute `ConfigurationAudit` module if any previous Configuration Audit have been found in the selected folder (applies for both, PRE and POST folders).
      - If no previous Configuration Audit is found in input folders, then execute it but pass the dataframe generated in memory to `ConsistencyCheck` module instead of forze it to read the Excel file from disk (slow). 
      - `SummaryAuditComparisson` sheet now includes a new column `Value_Diff` with the difference between `Value_Pre`and `Value_Post` columns.
      - `Summary_CellRelation` now distinguish between `Param_Discrepancies` and `Frequency_Discrepancies` and `SSB-Unknown`(those relations with Freq_Pre=Freq_Post but nodes not found in retuned list).
      - `Summary_CellRelation` now highlight those rows where Freq_Pre or Freq_Post is one of the Frequencies affecte (SSB-Pre or SSB-Post).
      - Now sheets GU_disc and NR_disc are divided into two sheets called GU_param_disc/GU_freq_disc for GU and NR_param_disc/NR_freq_disc for NR to distinguish between Param/Frequency discrepancies.
      - Enhanced Summary info in log to distiguish between Param/Frequency discrepancies.
      - Excel file is now saved on a temp folder first and then moved to the output folder (this reduces lags on remote folders such as Onedrive).
    - **ConfigurationAudit module:**
      - ConfigurationAudit exports now all Correction Commands to `Correction_Cmd_CA` folder instead of `Correction_Cmd`. 
      - Avoid printing “Consistency Checks …” messages when the export is executed by ConfigurationAudit. 
      - In ConfigurationAudit, the `NRCellRelation` sheet now generates Correction_Cmd ONLY based on frequency (not parameter-comparison mismatches). 
      - In ConfigurationAudit, the `GUtranCellRelation` sheet now includes these extra columns:
        - `Frequency` (from GUtranFreqRelationId).
        - `ExternalGNodeBFunction` (extracted from the ref like neighborCellRef / nCellRef).
        - `GNodeB_SSB_Target` (same logic as ExternalGUtranCell).
      - In ConfigurationAudit, the `GUtranCellRelation` seeht now generates Correction_Cmd ONLY based on frequency, using the SAME logic as GU_disc in ConsistencyChecks.
      - ConfigurationAudit now exporst ALL commands that do NOT require 2 audits, into `Correction_Cmd_CA` folder.
        - All MOs where a column `Correction_Cmd` is found in the Excel sheet will be exported as text file command.
        - External/Termpoints commands (they already come from the single Audit Excel).
      - Disabled (by default) printing list of nodes that have already been retuned and nodes that still have not been retuned.
      - Improved recursive ConfigurationAudit to avoid run another ConfigurationAudit if the folder contains any previous audit run with the same tool version.
    - **Export Correction Commands:**
      - Enhancements in the way that the Correction_Cmd is loaded (before it was read from final Excel file, now it is read from the dataframe already in memory).
      - Correction Commands files are now exported as ZIP files (by default) to reduce latency and avoid to write hundreds of files on disk.

- #### 🐛 Bug fixes:
    - Stopped ConfigurationAudit from creating ConsistencyCheck-only folders (MissingRelations/ NewRelations/ RelationsDiscrepancies) under Correction_Cmd.
    - Fixed Correction_Cmd exporters so it now includes all sheets that contain Correction_Cmd (e.g., NRCellRelation, GUtranCellRelation), not only External/TermPoint.
    - Fixed NRCellRelation command generation by ensuring canonical column names are present for the builders (so Correction_Cmd is no longer all-empty).
    - Fixed GUtranCellRelation processing: restored missing output columns (Correction_Cmd, Frequency, GNodeB_SSB_Target) via proper reinjection, and set SubCategory back to LTE Frequency Audit.
    - Fixed Summary sheet when one MO appears in multiple logs: File/LogFile now list the real logs (deduped), instead of duplicating the same name.
    - Fixed Summary LogPath: now points to <zip>/<log> instead of the temporary unzip folder (by passing source_zip_path/extracted_root through kwargs).
    - Fixed TermPointToGNB being empty / crashing: corrected reinjection issues (where applicable).
    - Fixed issue when `NRCellRelation` or `GUtranCellRelation` have to extract the list of nodes "Pre-retune" or "Post-retune" from SummaryAudit. Since those nodes are calculated by `process_nr_cell_du()` function, that function need to be run before to process other tables.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.7
### Release Date: 2026-01-23

- #### 🚨 Breaking Changes:

- #### 🌟 New Features:

- #### 🚀 Enhancements:

- #### 🐛 Bug fixes:
    - Fixed duplicates entries in Excel sheets of `ConfigurationAudit` module when one MO is separated in different log files. Now the tool merge all the logs files and creates only one sheet per MO.
    - Fixed bug in `is_n77_from_string()` function causing bad parsing of values like `653952-30-20-0-1`, `auto_647328`etc...
    - Fixed re-injection of `GUtranSyncSignalFrequency` MO in `ConfigurationAudit` module. Previously the MO re-injected was `GUSyncSignalFrequency` which does not exist.
    - Fixed potential bug closing Excel file twice in `ConfigurationAudit` module.
    - Fixed potential bug in `n77b_ssb_arfcn` field from Class `ConfigurationAudit` that may cause the constructor fail.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.6
### Release Date: 2026-01-16

- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
    - Added GNodeB_SSB_Target classification to TermPointToGNodeB using the same SummaryAudit node-id based logic (nodes_pre / nodes_post) already used in ExternalNRCellCU, ensuring consistent SSB-Pre/SSB-Post/Unknown targeting.
    - Updated the orchestrator call chain so process_termpoint_to_gnodeb() now receives nodes_id_pre and nodes_id_post, avoiding any dependency on “NR must run first” just to inherit the target.
    - Enhanced the external/termpoint command exporter to split TermPointToGNodeB outputs into two folders: TermPointToGNodeB/SSB-Post and TermPointToGNodeB/Unknown.
    - Implemented per-target grouping by NodeId for TermPointToGNodeB exports: if a NodeId contains rows with both targets, the commands are exported into separate node files per target (via target filtering before grouping).
    - Enhanced NRCellRelation processing to add the requested enrichment columns: Frequency (extracted from nRFreqRelationRef) plus ExternalGNBCUCPFunction and ExternalNRCellCU (parsed from nRCellRef), following the same extraction approach used in ExternalNRCellCU.
    - Added support to compute GNodeB_SSB_Target for NRCellRelation using nodes_pre / nodes_post (same target-detection logic as ExternalNRCellCU).

- #### 🚀 Enhancements:
    - Added pre- and post-change hget checks around the set in the ExternalNRCellCU correction command, so nRFrequencyRef is displayed before and after applying the SSB update.
    - Updated TermPointToGNodeB correction command template to match the slide’s final format: removed dynamic hget lines using ssb_pre/ssb_post and replaced them with the hardcoded hget ... nRFrequencyRef 64 checks in the correct places, aligning with the expected command sequence.

- #### 🐛 Bug fixes:
   - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.5
### Release Date: 2026-01-15

- #### 🚨 Breaking Changes:

- #### 🌟 New Features:
    - Added a post-processing cleanup step that deletes the extracted ZIP logs folder after processing, while leaving the original ZIP file untouched.
    - Kept cleanup failure-safe (best-effort): any deletion errors are swallowed to avoid breaking the main execution flow.
    - Market is extracted from Step0_<Market>_... and added to filenames; Pre* / Post* are ignored as markets.
    - Bulk mode output filenames now include the detected market prefix (before Pre/Post and before the timestamp) to keep per-market artifacts clearly separated and consistently named.

- #### 🚀 Enhancements:
    - Output files now use parent-folder timestamp (if present); otherwise they use the execution timestamp.
    - Output folders still use the execution timestamp (always).
    - ZIP logs are unzipped to the system temp folder, processed from there, then cleaned up.
    - Timestamps are normalized (no seconds, - → _, format YYYYMMDD_HHMM).

- #### 🐛 Bug fixes:
    - Bulk market detection now skips tool output folders so they’re not treated as markets.
    - Tk dialogs now use temporary hidden roots + parent= + destroy() to avoid the blank GUI window hang.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.4
### Release Date: 2026-01-13

- #### 🚨 Breaking Changes:
  
- #### 🚀 Enhancements:
    - Scope ProfilesAudit's checks to nodes that have completed retuning (nodes_post), when provided.
    - Added TrStSaNrFreqRelProfileUeCfg to the ProfilesAudit with the same two checks as McpcPCellNrFreqRelProfileUeCfg (missing old→new clone and parameter discrepancies), scoped to post-retuned nodes (nodes_post_scope).
    - Logging messages now has a timestamp prefix.
    - Input folders now can contain the logs files in ZIP format (for a faster download from Onedrive). If Input Folder contains some zip files with .txt/.log inside, it will consideer as a valid Input folder with Logs and the logs will be extracted into `__unzipped_logs__` suffolder to be processed.
    - Excel Output file is now saved into a TEMP folder and then moved into the final output folder. This prevents to save the Excel file into a Onedrive folder which is quite slow. 
    - Performance Improvements on `ConfigurationAudit` module.
    - `ConfigurationAudit` module now shows details of time per phase to see which phases are more time-consuming.

- #### 🐛 Bug fixes:
    - Fixed bug with table `TrStSaNrFreqRelProfileUeCfg` that was not being collected properly.
    - Exclude any `Id` field from Profiles Discrepancies checks.
    - Avoid filter by nodes that have already been retuned the following check: `Profiles with old N77 SSB (xxxx_648672) but not new N77 SSB (xxxx_647328) (from McpcPCellNrFreqRelProfileUeCfg)`.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.3
### Release Date: 2026-01-08

- #### 🚨 Breaking Changes:
  
- #### 🚀 Enhancements:

- #### 🐛 Bug fixes:
    - Fixed the “same profileRef containing old SSB name” KPI to exclude Default / empty profile refs, so nodes pointing to ...=Default are no longer incorrectly counted as “same old-SSB profile”.
    - Tightened the “same profileRef” condition to only count refs that actually encode an SSB and where the extracted SSB matches n77_ssb_pre, aligning the output with the wording “containing old SSB name”.
    - Corrected the “cloned or Other” KPI so it now represents all nodes having both OLD+NEW SSB minus the “same old-name profileRef” nodes, ensuring “Default/Other” cases fall into the intended bucket (as shown in your slide).
    - Optimized the new SSB referencing old-prefix profile inconsistency detection by replacing the slow iterrows() loop with a vectorized approach using map + unique().
    - Replaced the heavy per-cell slicing loop (for cell_id in cells_both + repeated full_n77.loc[...]) with a merge-based OLD vs NEW comparison, drastically reducing repeated filtering and improving runtime on large tables.
    - Added a robust value normalization step (_normalize_value_for_compare) to avoid false mismatches caused by complex objects (lists/tuples/dicts/numpy arrays) and to make comparisons stable.
    - Improved mismatch detection to only report real parameter differences after applying the “expected profile clone is not a mismatch” rule, reducing noisy “empty-column” inconsistencies.
    - Fixed GitHub repository name.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.2
### Release Date: 2026-01-07

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - New Checks in `McpcPCellNrFreqRelProfileUeCfg` table to consideer the following consraints:
      - Step1 script will create new McpcPCellNrFreqRelProfile with id xxxx_647328, exact replica of existing profiles with ids xxxx_648672.
      - Step2 script will create new McpcPCellNrFreqRelProfile with id 647328_xxxx, exact replica of existing profiles with ids 648672_xxxx.

- #### 🚀 Enhancements:
    - Improved Checks in `NRFreqRelation` table to check that all rows with NRFreqRelationId = SSB-Post and with the same NRCellCUId will have column mcpcPCellNrFreqRelProfileRef identical for both, but only difference will be SSB in profile name replacing xxxx_SSB-Pre with xxxx_SSB-Post.  
      - Example: McpcPCellNrFreqRelProfile=430090_648672 -> McpcPCellNrFreqRelProfile=430090_647328 

- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.1
### Release Date: 2025-12-23

- #### 🚨 Breaking Changes:
    - Removed module `4. Profiles Audit` and incorporated the logic within module `1. Configuration Audit`.
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:

- #### 🐛 Bug fixes:
    - Fixed error in `NR Frequency Inconsistencies (from NRFreqRelation)`
    - Fixed error in `Profiles Discrepancies (from McpcPCellNrFreqRelProfileUeCfg)`
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.0
### Release Date: 2025-12-22

- #### 🚨 Breaking Changes:
    - Renamed Tool name from `RetuningAutomations` to `SSB_RetuningAutomations`.
    - Renamed module `4. Initial Clean-Up` to `4. Profiles Audit`.
  
- #### 🌟 New Features:
    - Include `Profiles Inconsistencies` to `Configuration Audit` module (in SummaryAudit table) when it is executed from module `4. Profiles Audit` module.
    - Include `Profiles Discrepancies` to `Configuration Audit` module (in SummaryAudit table) when it is executed from module `4. Profiles Audit` module.
    - Added new checks to `ConfigurationAuudit` module to include:
      - NR nodes with the old N77 SSB and the new SSB pointing to some mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to clone mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
    - Added new checks to `ProfilesAuudit` module to include:
      - NR nodes with the new N77 SSB and NRCellCU Ref parameters to Profiles with the old SSB name (from NRCellCU table)
      - NR nodes with the new N77 SSB and EUtranFreqRelation Ref parameters to Profiles with the old SSB name (from EUtranFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to some mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to clone mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)

- #### 🚀 Enhancements:

- #### 🐛 Bug fixes:
    - Fixed build_all.yml to update the new Tool name.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated README.md.
    - Updated ROADMAP.md.
    - Logo updated.
    - Updated documentation with latest changes.

---

## Release: v0.4.3
### Release Date: 2025-12-19

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Added `FoldersCompared.txt` file to output folder when `ConsistencyCheck` module has been executed to show the folders used for Pre and Post. 

- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated README.md.
    - Updated documentation with latest changes.

---

## Release: v0.4.2
### Release Date: 2025-12-19

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Optimized TXT Commands file to group all commands within the same nodes and avoid duplicate lines with `confb+, lt all, alt, wait` commands.
    - Modified TermpointToGNB Correction Command.
    - Modified TermpointToGnodeB Correction Command.
    - Modified NR Relations Discrepancies Correction Command.
    - Correction_Cmd output subfolders have been renamed as follow:
      - from `Discrepancies` to `RelationsDiscrepancies`
      - from `New Relations` to `NewRelations`
      - from `Missing Relations` to `MissingRelations`
    - Correction_Cmd output subfolders for `RelationsDiscrepancies`, `NewRelations` and `MissingRelations` are now sepparated into two subfolders (`NR` and `GU`) for a better organization.

- #### 🐛 Bug fixes:
    - Fixed bug on table `Summary GU_FreqRelation` that was taking the data from `GUtranSyncSignalFrequency` table instead of `GUtranFreqRelation`.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.4.1
### Release Date: 2025-12-18

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - When running `ConsistencyCheck` module, the Pre/Post Audits now are saved into the same output folder as the `ConsistencyCheck` output.

- #### 🚀 Enhancements:
    - Added Correction Commands for LTE External Cells and Termpoints.
    - External commands and Termpoint commands are now split by Node.
    - External commands are now saved into subfolders `ExternalNRCellCU` and `ExternalGUtranCell`.
    - Termpoint commands are now saved into subfolders `TermPointToGNodeB` and `TermPointToGNB`.
    - Code Refactored to split the module ca_summary into smaller modules for a better management of it.

- #### 🐛 Bug fixes:
    - Fixed `ERROR: 'str' object has no attribute 'astype'` on `ConfigurationAudit` module when `availabilityStatus` column is not found.
    - Fixed Error when any MO has the same name as the expected table but with some letter in upper/lower capital.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.4.0
### Release Date: 2025-12-16

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - New metrics added to `ConfigurationAudit` module to count:
      - Table/Category: `ExternalNRCellCU`
        - ExternalNRCellCU to old N77 SSB (from ExternalNRCellCU)
        - ExternalNRCellCU to new N77 SSB (from ExternalNRCellCU)
      - Table/Category: `TermPointToGNodeB`
        - NR to NR TermPoint administrativeState LOCKED (from TermPointToGNodeB)
        - NR to NR TermPoint operationalState DISABLED  (from TermPointToGNodeB)
      - Table/Category: `ExternalGUtranCell`
        - ExternalGUtranCell to old N77 SSB (xxxx) (from ExternalGUtranCell)
        - ExternalGUtranCell to new N77 SSB (xxxx) (from ExternalGUtranCell)
        - ExternalGUtranCell to old N77 SSB (xxx) serviceStatus OUT_OF_SERVICE (from ExternalGUtranCell)
        - ExternalGUtranCell to new N77 SSB (xxx) serviceStatus OUT_OF_SERVICE (from ExternalGUtranCell)
      - Table/Category: `TermPointToGNB`
          - LTE to NR TermPoints with administrativeState=LOCKED (from TermPointToGNB)
          - LTE to NR TermPoints with operationalState=DISABLED (from TermPointToGNB)
          - LTE to NR TermPoints with usedIpAddress=0.0.0.0/:: (from TermPointToGNB)
      - Table/Category: `TermPointToENodeB`
        - NR to LTE TermPoints with administrativeState=LOCKED (from TermPointToENodeB)
        - NR to LTE TermPoints with operationalState=DISABLED (from TermPointToENodeB)
    - Added Additional Columns to `ExternalNRCellCU` table:
      - `Termpoint`: Unique identifier to identify the termpoint.
      - `TermpointStatus`: Contains the current status of the termpoint as concatenation of `administrativeState`-`operationalState`-`availabilityStatus`.
      - `TermpointConsolidatedStatus`: Contains the current status of the termpoint in a consolidated way.
      - `GNodeB_SSB_Target`: Useful to identify nodes with SSB-Pre/SSB-Post or nodes with SSB different to Pre/Post.
      - `Correction_Cmd`: Contains the correction command to fix the termpoint.
    - Added Additional Columns to `TermPointToGNodeB` table:
      - `Termpoint`: Unique identifier to identify the termpoint.
      - `TermpointStatus`: Contains the current status of the termpoint as concatenation of `administrativeState`-`operationalState`-`availabilityStatus`.
      - `TermpointConsolidatedStatus`: Contains the current status of the termpoint in a consolidated way.
      - `SSB needs update`: Indicates those termpoints whose termpoint id has been found in `ExternalNRCellCU` with "SSB-Pre" and with `GNodeB_SSB_Target` different to "SSB-Pre".
      - `Correction_Cmd`: Contains the correction command to fix the termpoint.
    - Added Correction Commands for NR External Cells and Termpoints.

- #### 🚀 Enhancements:
    - Excel columns Autofit now is limited to a maximum column width of 100.
    - `SummaryAudit` Category column now contains links to the source table of each audit.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.11
### Release Date: 2025-12-04

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - New feature to allow Frequency Filtering for `ConsistencyCheck` module.
    - GUI lanuncher adapted to include the list of frequencies to filter on `ConsistencyCheck` module.

- #### 🚀 Enhancements:
    - Some code refactoring to clean-up duplicates functions and group them by logic.
    - Output folder for `ConfigurationAudits` now has the market suffix if they have been executed from `ConsistencyCheck` module and any market have been detected.
    - Removed `timeOfCreation` from list of parameters to find discrepancies on `ConsistencyCheck` moodule.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.10
### Release Date: 2025-12-03

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - New feature to recursively run Audit on all subfolders of Input Folder where any valid log is found. To run this feature you just need to provide an Input folder with no logs inside but with subfolders with valid logs inside.
    - New feature to auto-detect Pre/Post folders given only one Input folder with a predefined folder structure.
      - For this feature to work, the given input folder should contain subfolders with this naming convention: `yyyymmdd_hhmm_step0` (Optionally they may be a Market Subfolder inside it). 
        - Example 1:
          - 20251203_0530_step0 --> This is selected as Pre folder since is the oldest folder for the latest day
          - 20251203_0730_step0 --> This is selected as Post folder since is the latest folder for the latest day
        - Example 2:
          - 20251202_0530_step0
          - 20251202_0730_step0 --> This is selected as Pre folder since is the latest folder for the latest day previous to the Post folder day 
          - 20251203_0530_step0
          - 20251203_0730_step0 --> This is selected as Post folder since is the latest folder for the latest day
    - Added a Blacklist of words to discard any step0 subfolders from auto-detection function. By default the tool will not consideer as Pre/Post candidates any folder with any of the following words in its name: "ignore", "old", "discard", "bad".
    - Added a Blacklist of words to discard any input subfolders from auto-detection function. By default the tool will not consideer as input folder candidates for Audits any folder with any of the following words in its name: "ignore", "old", "discard", "bad".
    - New module called `3. Consistency Check (Bulk mode Pre/Post auto-detection)`. When this module is selected, the tool will automatically run an Smart Consistency Check in all markets detected in the input folder, selecting the most suitable folder for Pre and Post for each market.

- #### 🚀 Enhancements:
    - Added column `GNBCUCPFunction` to NR tables in `ConcistencyCheck` module to be able to filter those relations of interest.
    - Added `Freq_Pre` and `FreqPost` columns also to `_new` tables of `ConcistencyCheck` module to be consistent with the other tables generated by this module.
    - Deleted unnecessary columns on all `_new`, `_missing`, `_disc` tables in `ConsistencyCheck` module.
    - The GUI launcher dialog now comes to the foreground when it is reopened after the execution of any module.
    - The GUI launcher dialog now is always centered on screen.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.9
### Release Date: 2025-12-02

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Excluded Relations Discrepancies on those relations towards nodes that have not been retuned (buffer) on `ConsistencyCheck` module.
    - Added a new sheet called `SummaryAuditComparison` to `ConsistencyCheck` output Excel file to compare the values of both Audits run (for Pre-folder and Post-folder).

- #### 🚀 Enhancements:
    - Fixed `Freq_Pre/Freq_Post` values in `NR_Disc` table on `ConsistencyCheck` module.
    - Renamed sheet `Summary_Detailed` to `Summary_CellRelation`.
    - Removed Exception when input folder does not contain logs files. Now a message is displayed instead of throw an exception.
  
- #### 🐛 Bug fixes:
    - Removed Duplicated check in `GUtranSyncSignalFrequency` Table.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.8
### Release Date: 2025-12-01

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Added Correction Commands on GU_Disc and NR_Disc tables.
    - NR Nodes classification MACRO/mmWave (#35)

- #### 🚀 Enhancements:
    - Created separated subfolders for each type of Correction Commands.
    - Apply red color font only to Inconsistency rows whose value is higher than zero.
  
- #### 🐛 Bug fixes:
    - Fixed abnormal prefix for long paths in Windows.
    - Fixed invalid Inconsistency in mixed nodes (MidLowBand & mmWave).
    - Aligned all Audit messages.
    - Modified the mmWave range up to 2.300.000 instead of 2.100.000.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.7
### Release Date: 2025-11-28

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Redesigned GUI Dialog.
  
- #### 🐛 Bug fixes:
    - Fixed Long path issues on Windows.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.6
### Release Date: 2025-11-27

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Added commands to `Consistency Checks` module.
      - Correction Commands to `GU_new` table.
      - Correction Commands to `NR_new` table.
      - Correction Commands to `GU_missing` table.
      - Correction Commands to `NR_missing` table.
    - Added a TXT file per node with the Correction Commands for each node.
    - Auto-Fit column sizes in Excel sheets.
    - Header is now coloured in different color in Excel sheets.

- #### 🚀 Enhancements:
    - Created a scheduled workflow for binary genaration.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.5
### Release Date: 2025-11-26

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Added new checks to `Configuration Audit` module.
      - NR cellRelations to old SSB (from NRCellRelation table)
      - NR cellRelations to new SSB (from NRCellRelation table)
      - LTE cellRelations to old SSB (from GUtranCellRelation table)
      - LTE cellRelations to new SSB (from GUtranCellRelation table)
      - LTE nodes with the old SSB but without the new SSB (from FreqPrioNR table)
      - LTE nodes with both, the old SSB and the new SSB (from FreqPrioNR table)
      - LTE cells with mismatching params between FreqPrioNR 648672 and 647328
    - Added new sheet `Summary NR Params Missmatching` with columns `NodeId`, `GNBCUCPFunctionId`, `NRCellCUId` and `NRFreqRelationId`.
    - Added new sheet `Summary LTE Params Missmatching` with columns `NodeId`, `EUtranCellId` and `GUtranFreqRelationId`.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.4
### Release Date: 2025-11-25

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Added new table `Summary Param Missmatching` to `Configuration Audit` module to show which cells/nodes has changed and which params have been changed. 

- #### 🚀 Enhancements:
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - README.md updated.
    - Updated documentation with latest changes.


---

## Release: v0.3.3
### Release Date: 2025-11-24

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Swapped columns Category/SubCategory in `ConfigurationAudit`.
    - Added more Checks to `NRCellDU` Category.
    - Included colors in `SummaryAudit` table for an easier visibility of each category. 
    - `ConfigurationAudit` output is now saved into a subfolder per execution within the log folder.
    - Other changes to `ConfigurationAudit`.
    - Input folders now are not cleaned when a module that requires two input folders is selected.
  
- #### 🐛 Bug fixes:
    - Fixed errors when running `Consistency Checks` module after refactoring.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.2
### Release Date: 2025-11-21

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Detect Auto-created NRFreqRelation to new SSB, will not follow VZ naming convention NRFreqRelation=647328.
    - Detect Auto-created GUtranFreqRelation to new SSB, will not follow VZ naming convention GUtranFreqRelation=647328-30-20-0-1.
    - Added two new lists of `Allowed N77 SSB (Post)` and `Allowed N77 ARFCN (Post)` to use in 'ConfigurationAudit'.
    - Added new check to detect number of nodes from NRSectorCarrier whose ARFCN is in the list of allowed ARFCN (Pre).
    - Added new check to detect number of nodes from NRSectorCarrier whose ARFCN is in the list of allowed ARFCN (Post).
    - Added new check to detect those nodes from NRSectorCarrier whose ARFCN is not in the list of allowed ARFCN (Pre) nor allowed ARFCN (Post).
    - Added mismatching params between cells with old SSB and cells with new SSB in tables NRFreqRelation and GUtranFreqRelation

- #### 🚀 Enhancements:
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.1
### Release Date: 2025-11-20

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Included Pivot `GUtranFreqRelation` in `ConfigurationAudit` module.
    - Included LogPath in Summary tab of `ConfigurationAudit`.
    - Check that all nodes with NrFrequency=old_arfcn also have NrFrequency=new_arfcn. (#36)
    - Check that all NRCellCUId with NrFreqRelation=old_arfcn also have NrFreqRelation=new_arfcn and all params are same (except nRFreqRelationId, nRFrequencyRef and reservedBy). (#36)
    - Check that all nodes with GUtranSyncSignalFrequency=old_arfcn also have GUtranSyncSignalFrequency=new_arfcn. (#36)
    - Check that all EUtranCellFDDId with GUtranFreqRelationId=old_arfcn-30-20-0-1 also have GUtranFreqRelationId=new_arfcn-30-20-0-1 and all params are same (except gUtranFreqRelationId and gUtranSyncSignalFrequencyRef). (#36)
    - From table FreqPrioNR, detect how many N77 nodes has RATFreqPrioId equal to 'fwa' and 'publicsafety' and add them to Frequency Audit. (#40)
    - From table FreqPrioNR, detect how many N77 nodes has any RATFreqPrioId different from 'fwa' or 'publicsafety' and add them to Frequency Inconsistencies. (#40)
    - Increased up to 100 nodes/slide (max 4 columns) for the Inconsistencies slides in `ConfigurationAudit` module.
    - Other minor changes to `ConfigurationAudit`.
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.0
### Release Date: 2025-11-19

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Refactor main module `RetuningAutomations` to simplify the logic.
    - Added ARFCN 650006 to default Allowed ARFCN list.
    - Added 'LTE nodes with GUtranSyncSignalFrequency defined' to LTE Frequency Audit.
    - Changed N77 band detection to filter freqs within range 646600-660000.
    - Avoid adding Inconsistencies slides if there is no any inconsistency found for each metric.
  
- #### 🐛 Bug fixes:
    - Fix PPT template not found.
    - Fixed some Configuration Audit Metrics.
    - Fixed truncated lists of nodes in Configuration Audit.
    - Fixed duplicates in extra column of NR_Sector_Carrier Inconsistencies.
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.2.9
### Release Date: 2025-11-18

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Check if all EndcDistrProfile gUtranFreqRef is always set to (old_arfcn or new_arfcn) and & n77b_ssb (653952)
    - Added N77B SSB fields to GUI and CLI intefaces.

- #### 🚀 Enhancements:
    - Refactor module `ConfigurationAudit` and split in different submodules
    - Network Audit Enhancements (#26) now includes: 
      - Count/List of Nodes/Cells per category:
        - Count Nodes with N77 cells. NRCellDU Pivot table to check if all cells with SSB 648672&653952. NRSectorCarrier to check if all N77B sectors with ARFCN 654652, 655324, 655984 or 656656
        - Count NR nodes with NrFrequency/NrFreqRelation 648672 defined. Pivot tables to detect if 647328 already defined 
        - Count LTE nodes with GUtranSyncSignalFrequency/GUtranFreqRelation 648672 defined. Pivot table to detect if 647328 already defined
      - Check if any referece to new SSB in any node: FreqRelation, Frequency. If autocreated might need additional actions
      - Check if any cell reached max FreqRelation cardinality (max 16 NrFreqRelation and 16 GUtranFreqRelation per Cell)
      - Check if any nodes reached max 24 GUtranSyncSignalFrequency definitions
      - Check if any nodes reached max 64 NRFrequency definitions
      - Check if all EndcDistrProfile gUtranFreqRef always set to 648672&653952
  
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.8
### Release Date: 2025-11-17

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Added PPT Template.
    - Modified PPT to split nodes in chunks of 50 nodes per slide (2 columns of 50 nodes).
    - Persist Allowed SSB and ARFCN lists between different executions.
    - Improvements on Configuration Audit.
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.7
### Release Date: 2025-11-14

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - If Consistency Check module is selected, then use two input folders, one for Pre and one for Post (#25)
    - Network Audit Enhancements:
      - Count/List of Nodes/Cells per category:
        - Count Nodes with N77 cells. NRCellDU Pivot table to check if all cells with SSB 648672&653952. NRSectorCarrier to check if all N77B sectors with ARFCN 654652, 655324, 655984 or 656656
        - Count NR nodes with NrFrequency/NrFreqRelation 648672 defined. Pivot tables to detect if 647328 already defined 
        - Count LTE nodes with GUtranSyncSignalFrequency/GUtranFreqRelation 648672 defined. Pivot table to detect if 647328 already defined
      - Check if any referece to new SSB in any node: FreqRelation, Frequency. If autocreated might need additional actions
      - Check if any cell reached max FreqRelation cardinality (max 16 NrFreqRelation and 16 GUtranFreqRelation per Cell)
      - Check if any nodes reached max 24 GUtranSyncSignalFrequency definitions
      - Check if any nodes reached max 64 NRFrequency definitions
      - Check if all EndcDistrProfile gUtranFreqRef always set to 648672&653952
    - Configuration Audit now generates a PPT with a Text Summary.

- #### 🚀 Enhancements:
    - Align Summary headers to left (#23)
    - Include input log folder in Summary sheets of Consistency Check (#24)
    - Modified GUI to accept `ALLOWED_SSB_N77` and `ALLOWED_N77B_ARFCN` lists for Configuration Audit.
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes
  
---

## Release: v0.2.6
### Release Date: 2025-11-12

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Improvements on Date detection method to do an smart detection of any date included in the input folder.
    - Changed date format to YYYY-MM-DD for a better visualization.
    - Changed color of Summary Sheets in output Excel to green for a better visualization. 
    - Changed macos-x64 runer on GitHub workflow.
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    - Added `Summary NRFrequency` table to `Configuration Audit` module.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.5
### Release Date: 2025-11-11

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:
    - Configuration Audit now creates Pivot Table with useful Summary Information. 
    - Added Frequencies selector in GUI to select which frequencies you want to run Audit for.

- #### 🚀 Enhancements:
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    - Fixed Exception on `Consistency Check` module when Pre or Post folder has not been found in Input Folder
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.4
### Release Date: 2025-11-10

- #### 🚨 Breaking Changes:
  
- #### 🌟 New Features:

- #### 🚀 Enhancements:
    - Common methods and helpers from different Automation Modules extracted from their classes and added to a new Python module called common.py for a better efficiency and maintainance.
    - Smart sorting of MO names based on prefefined list `TABLES_ORDER` or log filename.
    - Added timer to measure the execution time of each module.
    - Module's Renaming. Current Module Names are:  
      `1. Configuration Audit (Logs Parser)`  
      `2. Consistency Check (Pre/Post Comparison)`  
      `3. Initial Clean-Up (During Maintenance Window)`  
      `4. Final Clean-Up (After Retune is completed)`  
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.
    
- #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.3
### Release Date: 2025-11-07
 
- #### 🌟 New Features:
    - Logger support for all print outputs.
    - `Input Folder` persistent between executions.

- #### 🚀 Enhancements:
    - GUI for module selection/launcher open again when any module finish successfully.
    - MO autodetection based on content instead of log filename.
    - Multiple table detection for each MO log file.
    - Capture exceptions during modules executions and avoid the tool to exit when any error is found.
    - Added 2 new columns in Summary sheet of module `2. Create Excel from Logs` with the Log filename where the table have been found and with the number of tables of each log file.
    
- #### 🐛 Bug fixes:
    - Minor bug fixing.

- #### 📚 Documentation: 
    - Updated documentation with latest changes
    
---

## Release: v0.2.2
### Release Date: 2025-11-06

- #### 🚀 Enhancements:
    - Improvements on GitHub Automatic Release creation.
    - Added Splash Logo on Windows executable while loading.
     
- #### 📚 Documentation: 
    - Created DOWNLOAD.md
    - Created CONTRIBUTING.md
    - Created CODE_OF_CONDUCT.md

---

## Release: v0.2.1
### Release Date: 2025-11-06

- #### 🌟 New Features:
    - Module '2. Create Excel from Logs' ready.

- #### 🚀 Enhancements:
    - Created module to compile the tool and generate binaries files for the different OS and architecture.
    - Created GitHub repository.
    - Created GitHub workflow to automatically generate binaries for the different OS and architecture.
    
- #### 🐛 Bug fixes:
    - Fixed bug on Summary_CellRelation that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

- #### 📚 Documentation: 
    - Created README.md
    - Created CHANGELOG.md
    - Created ROADMAP.md
     
---

## Release: v0.2.0
### Release Date: 2025-11-05

- #### 🌟 New Features:
    - Create a Module Selector & Configuration window.

- #### 🚀 Enhancements:
    - Improvements on Module '1. Pre/Post Relations Consistency Check' to satisfy the requirements.
    
- #### 🐛 Bug fixes:
    - Fixed bug on Summary_CellRelation that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

---

## Release: v0.1.1
### Release Date: 2025-11-04

- #### 🚀 Enhancements:
    - Improvements on Module '1. Pre/Post Relations Consistency Check' to satisfy the requirements.

---

## Release: v0.1.0
### Release Date: 2025-11-03

 
- #### 🌟 New Features:
    - Module '1. Pre/Post Relations Consistency Check' first release.

    
---
