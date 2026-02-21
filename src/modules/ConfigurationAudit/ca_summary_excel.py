# -*- coding: utf-8 -*-

"""
Summary Audit Orchestrator.

This module coordinates:
  - NR audit logic
  - LTE audit logic
  - External / TermPoint enrichment
  - Other functions to build the SummaryAudit table

IMPORTANT:
- build_summary_audit() returns only the SummaryAudit dataframe plus the NR/LTE parameter mismatching dataframes.
- Input dataframes may be modified in-place by the underlying process_* functions (side-effects), depending on their implementation.
"""


import pandas as pd
import re
from typing import List, Dict

from src.modules.Common.common_functions import load_nodes_names_and_id_from_summary_audit
from src.modules.ConfigurationAudit.ca_process_external_termpoint_tables import process_external_nr_cell_cu, process_external_gutran_cell, process_termpoint_to_gnodeb, process_termpoint_to_gnb, process_term_point_to_enodeb
from src.modules.ConfigurationAudit.ca_process_lte_tables import process_gu_sync_signal_freq, process_gu_freq_rel, process_gu_cell_relation
from src.modules.ConfigurationAudit.ca_process_nr_tables import process_nr_cell_du, process_nr_freq, process_nr_freq_rel, process_nr_sector_carrier, process_nr_cell_relation
from src.modules.ConfigurationAudit.ca_process_others_tables import process_endc_distr_profile, process_freq_prio_nr, process_cardinalities
from src.modules.ProfilesAudit.ProfilesAudit import cc_post_step2, process_profiles_tables
from src.utils.utils_frequency import parse_int_frequency


# =====================================================================
#                        SUMMARY AUDIT BUILDER
# =====================================================================

def build_summary_audit(
        df_mecontext: pd.DataFrame,
        df_nr_cell_du: pd.DataFrame,
        df_nr_freq: pd.DataFrame,
        df_nr_freq_rel: pd.DataFrame,
        df_nr_cell_rel: pd.DataFrame,
        df_freq_prio_nr: pd.DataFrame,
        df_gu_sync_signal_freq: pd.DataFrame,
        df_gu_freq_rel: pd.DataFrame,
        df_gu_cell_rel: pd.DataFrame,
        df_nr_sector_carrier: pd.DataFrame,
        df_endc_distr_profile: pd.DataFrame,

        # <<< NEW: needed for Post Step2 checks >>>
        df_nr_cell_cu: pd.DataFrame,
        df_eutran_freq_rel: pd.DataFrame,

        n77_ssb_pre: int,
        n77_ssb_post: int,
        n77b_ssb: int,
        allowed_n77_ssb_pre,
        allowed_n77_arfcn_pre,
        allowed_n77_ssb_post,
        allowed_n77_arfcn_post,
        df_external_nr_cell_cu,
        df_external_gutran_cell,
        df_term_point_to_gnodeb,
        df_term_point_to_gnb,
        df_term_point_to_enodeb,
        module_name,
        profiles_tables: Dict[str, pd.DataFrame] | None = None,
        profiles_audit: bool = False,
        frequency_audit: bool = False,
        unsync_already_filtered: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build a synthetic 'SummaryAudit' table with high-level checks:

      - N77 detection on NRCellDU and NRSectorCarrier.
      - NR/LTE nodes where specific SSBs (old_arfcn / new_arfcn) are defined.
      - NR/LTE nodes with SSBs not in {old_arfcn, new_arfcn}.
      - Cardinality limits per cell and per node.
      - EndcDistrProfile gUtranFreqRef values.

    Notes:
      - N77 cells are those with SSB/SSB in range [646600-660000].
      - This function is best-effort and should not raise exceptions; any error is
        represented as a row in the resulting dataframe.
    """

    allowed_n77_ssb_pre_set = {int(v) for v in (allowed_n77_ssb_pre or [])}
    allowed_n77_arfcn_pre_set = {int(v) for v in (allowed_n77_arfcn_pre or [])}
    allowed_n77_ssb_post_set = {int(v) for v in (allowed_n77_ssb_post or [])}
    allowed_n77_arfcn_post_set = {int(v) for v in (allowed_n77_arfcn_post or [])}

    rows: List[Dict[str, object]] = []

    # -------------------------------------  HELPERS (Embedded, minimal impact) -------------------------------------
    def is_not_old_not_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq not in (n77_ssb_pre, n77_ssb_post)

    def series_only_not_old_not_new(series) -> bool:
        return all(is_not_old_not_new(v) for v in series)

    def is_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_post

    def is_old(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_pre

    def has_value(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq is not None

    def is_n77_ssb_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_pre_set if freq is not None else False

    def is_n77_ssb_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_post_set if freq is not None else False

    def is_n77_arfcn_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_pre_set if freq is not None else False

    def is_n77_arfcn_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_post_set if freq is not None else False

    def all_n77_arfcn_in_pre(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_pre_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_pre_set)

    def all_n77_arfcn_in_post(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_post_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_post_set)

    def add_row(
            category: str,
            subcategory: str,
            metric: str,
            value: object,
            extra: str = "",
    ) -> None:
        rows.append(
            {
                "Category": category,
                "SubCategory": subcategory,
                "Metric": metric,
                "Value": value,
                "ExtraInfo": extra,
                "Tips": "",
            }
        )

    def extract_freq_from_nrfrequencyref(value: object) -> int | None:
        """Extract NRFrequency integer from reference string like '...NRFrequency=648672'."""
        if value is None:
            return None
        s = str(value)
        key = "NRFrequency="
        idx = s.rfind(key)
        if idx == -1:
            # fallback: try plain int in the string
            return parse_int_frequency(s)
        substr = s[idx + len(key):]
        digits = []
        for ch in substr:
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    # Helper embedded inside the main function
    def extract_freq_from_nrfreqrelationref(value: object) -> int | None:
        """Extract NRFreqRelation integer from NRCellRelation-like reference string."""
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None

        # Preferred: explicit key inside the reference
        key = "NRFreqRelation="
        idx = s.rfind(key)
        if idx != -1:
            tail = s[idx + len(key):].strip()
            freq = parse_int_frequency(tail)
            return int(freq) if freq is not None else None

        # Fallback 1: take the last '=' chunk and try to parse leading digits
        if "=" in s:
            tail = s.split("=")[-1].strip()
            freq = parse_int_frequency(tail)
            if freq is not None:
                return int(freq)

        # Fallback 2: scan comma-separated tokens and pick the last token that starts with digits
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for token in reversed(parts):
            freq = parse_int_frequency(token)
            if freq is not None:
                return int(freq)

        return None

    def extract_ssb_from_gutran_sync_ref(value: object) -> int | None:
        """
        Extract SSB integer from references containing:
          GUtranSyncSignalFrequency=647328-30
        In that example, returned SSB is 647328.
        """
        if value is None:
            return None
        s = str(value)
        key = "GUtranSyncSignalFrequency="
        idx = s.rfind(key)
        if idx == -1:
            return None
        substr = s[idx + len(key):]
        digits = []
        for ch in substr:
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    def extract_nr_network_tail(value: object) -> str:
        """Return substring starting from 'NRNetwork='."""
        if value is None:
            return ""
        s = str(value)
        idx = s.find("NRNetwork=")
        return s[idx:] if idx != -1 else ""

    def normalize_state(value: object) -> str:
        """Normalize state values for robust comparisons."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip().upper()

    def normalize_ip(value: object) -> str:
        """Normalize usedIpAddress values for robust comparisons."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip()

    # ============================ MAIN CODE ================================
    # NOTE:
    # - Some processors (NRCellRelation, GUtranCellRelation, Externals/Termpoints, Profiles audit scoping)
    #   need to know which nodes are considered "Pre-retune" vs "Post-retune".
    # - The helper load_nodes_names_and_id_from_summary_audit() extracts those node lists from rows produced
    #   by process_nr_cell_du() (NR nodes with N77 SSB in Pre/Post-Retune allowed lists).
    # - Therefore, we MUST run process_nr_cell_du() first to populate rows, then load nodes, then run the rest.

    # -------------------------------------  MeContext checks (must be first rows) -------------------------------------
    def _find_col_ci_local(df: pd.DataFrame, names: List[str]) -> str | None:
        if df is None or df.empty:
            return None
        cols_l = {str(c).strip().lower(): c for c in df.columns}
        for n in names:
            key = str(n).strip().lower()
            if key in cols_l:
                return cols_l[key]
        return None

    me_node_col = _find_col_ci_local(df_mecontext, ["NodeId"])
    me_parent_col = _find_col_ci_local(df_mecontext, ["ParentId"])
    me_sync_col = _find_col_ci_local(df_mecontext, ["syncStatus"])

    unsync_nodes: set[str] = set()
    if df_mecontext is not None and not df_mecontext.empty and me_node_col and me_sync_col:
        try:
            mask_unsync = df_mecontext[me_sync_col].astype(str).str.upper().eq("UNSYNCHRONIZED")
            unsync_nodes = set(df_mecontext.loc[mask_unsync, me_node_col].astype(str).unique())
        except Exception:
            unsync_nodes = set()

    total_nodes = []
    if df_mecontext is not None and not df_mecontext.empty and me_node_col:
        try:
            total_nodes = sorted(df_mecontext[me_node_col].astype(str).unique())
        except Exception:
            total_nodes = []

    total_parents = []
    if df_mecontext is not None and not df_mecontext.empty and me_parent_col:
        try:
            total_parents = sorted(df_mecontext[me_parent_col].astype(str).unique())
        except Exception:
            total_parents = []

    add_row("MeContext", "MeContext Audit", "Total unique nodes", len(total_nodes), ", ".join(total_parents))
    add_row("MeContext", "MeContext Audit", "Nodes with syncStatus='UNSYNCHRONIZED' (being excluded in all Audits)", len(unsync_nodes), ", ".join(sorted(unsync_nodes)))

    def _exclude_unsync_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty or not unsync_nodes:
            return df
        node_col = _find_col_ci_local(df, ["NodeId"])
        if not node_col:
            return df
        try:
            return df.loc[~df[node_col].astype(str).isin(unsync_nodes)].copy()
        except Exception:
            return df

    if not unsync_already_filtered:
        df_nr_cell_du = _exclude_unsync_df(df_nr_cell_du)
        df_nr_freq = _exclude_unsync_df(df_nr_freq)
        df_nr_freq_rel = _exclude_unsync_df(df_nr_freq_rel)
        df_nr_cell_rel = _exclude_unsync_df(df_nr_cell_rel)
        df_freq_prio_nr = _exclude_unsync_df(df_freq_prio_nr)
        df_gu_sync_signal_freq = _exclude_unsync_df(df_gu_sync_signal_freq)
        df_gu_freq_rel = _exclude_unsync_df(df_gu_freq_rel)
        df_gu_cell_rel = _exclude_unsync_df(df_gu_cell_rel)
        df_nr_sector_carrier = _exclude_unsync_df(df_nr_sector_carrier)
        df_endc_distr_profile = _exclude_unsync_df(df_endc_distr_profile)
        df_nr_cell_cu = _exclude_unsync_df(df_nr_cell_cu)
        df_eutran_freq_rel = _exclude_unsync_df(df_eutran_freq_rel)
        df_external_nr_cell_cu = _exclude_unsync_df(df_external_nr_cell_cu)
        df_external_gutran_cell = _exclude_unsync_df(df_external_gutran_cell)
        df_term_point_to_gnodeb = _exclude_unsync_df(df_term_point_to_gnodeb)
        df_term_point_to_gnb = _exclude_unsync_df(df_term_point_to_gnb)
        df_term_point_to_enodeb = _exclude_unsync_df(df_term_point_to_enodeb)

    # Detailed parameter mismatching rows to build Excel sheets "Summary NR Param Missmatching" and
    # "Summary LTE Param Missmatching"
    param_mismatch_rows_nr: List[Dict[str, object]] = []
    param_mismatch_rows_gu: List[Dict[str, object]] = []

    param_mismatch_columns_nr = [
        "Layer",  # "NR"
        "Table",  # "NRFreqRelation"
        "NodeId",
        "GNBCUCPFunctionId",
        "NRCellCUId",
        "NRFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    param_mismatch_columns_gu = [
        "Layer",  # "LTE"
        "Table",  # "GUtranFreqRelation"
        "NodeId",
        "EUtranCellId",  # generic LTE cell id
        "GUtranFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    # 1) Create NRCellDU summary rows first (required for node extraction)
    nodes_id_pre, nodes_name_pre = set(), set()
    nodes_id_post, nodes_name_post = set(), set()
    process_nr_cell_du(df_nr_cell_du, add_row, allowed_n77_ssb_pre_set, allowed_n77_ssb_post_set, nodes_id_pre, nodes_id_post)

    # 2) Now that rows contains NRCellDU metrics, load node identifiers for Pre/Post
    nodes_id_pre, nodes_name_pre = load_nodes_names_and_id_from_summary_audit(rows, stage="Pre", module_name=module_name)
    nodes_id_post, nodes_name_post = load_nodes_names_and_id_from_summary_audit(rows, stage="Post", module_name=module_name)

    # IMPORTANT: use both numeric IDs and full node names for matching inside reference strings (ExternalGNodeBFunction / ExternalGNBCUCPFunction)
    nodes_pre_all = set(nodes_id_pre or set()) | set(nodes_name_pre or set())
    nodes_post_all = set(nodes_id_post or set()) | set(nodes_name_post or set())

    # NR Tables
    if frequency_audit:
        process_nr_freq(df_nr_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, nodes_pre_all, nodes_post_all)
    process_nr_freq_rel(df_nr_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_nr, nodes_pre_all, nodes_post_all)
    process_nr_sector_carrier(df_nr_sector_carrier, add_row, allowed_n77_arfcn_pre_set, all_n77_arfcn_in_pre, allowed_n77_arfcn_post_set, all_n77_arfcn_in_post, nodes_pre_all, nodes_post_all)
    process_nr_cell_relation(df_nr_cell_rel, extract_freq_from_nrfreqrelationref, n77_ssb_pre, n77_ssb_post, add_row, nodes_pre_all, nodes_post_all)

    # LTE Tables
    if frequency_audit:
        process_gu_sync_signal_freq(df_gu_sync_signal_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, nodes_pre_all, nodes_post_all)
    process_gu_freq_rel(df_gu_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_gu, nodes_pre_all, nodes_post_all)
    process_gu_cell_relation(df_gu_cell_rel, n77_ssb_pre, n77_ssb_post, add_row, nodes_pre_all, nodes_post_all)

    # Externals & Termpoints tables
    process_external_nr_cell_cu(df_external_nr_cell_cu, n77_ssb_pre, n77_ssb_post, add_row, df_term_point_to_gnodeb, extract_freq_from_nrfrequencyref, extract_nr_network_tail, nodes_pre_all, nodes_post_all)
    process_external_gutran_cell(df_external_gutran_cell, extract_ssb_from_gutran_sync_ref, n77_ssb_pre, n77_ssb_post, add_row, normalize_state, df_term_point_to_gnb, nodes_pre_all, nodes_post_all)
    process_termpoint_to_gnodeb(df_term_point_to_gnodeb, add_row, df_external_nr_cell_cu, n77_ssb_post, n77_ssb_pre, nodes_pre_all, nodes_post_all)
    process_termpoint_to_gnb(df_term_point_to_gnb, normalize_state, normalize_ip, add_row, df_external_gutran_cell, n77_ssb_post, n77_ssb_pre, nodes_pre_all, nodes_post_all)
    process_term_point_to_enodeb(df_term_point_to_enodeb, normalize_state, add_row, nodes_pre_all, nodes_post_all)

    # Other Tables
    process_endc_distr_profile(df_endc_distr_profile, n77_ssb_pre, n77_ssb_post, n77b_ssb, add_row, nodes_pre_all, nodes_post_all)
    process_freq_prio_nr(df_freq_prio_nr, n77_ssb_pre, n77_ssb_post, add_row, nodes_pre_all, nodes_post_all)
    process_cardinalities(df_nr_freq, add_row, df_nr_freq_rel, df_gu_sync_signal_freq, df_gu_freq_rel, nodes_pre_all, nodes_post_all)

    # Profiles Tables (optional)
    if profiles_audit:
        profiles_tables_work = profiles_tables or {}

        # Scope profiles audit to nodes that have completed retuning
        nodes_post_scope = {str(x).strip() for x in (list(nodes_id_post or []) + list(nodes_name_post or [])) if x is not None and str(x).strip()}

        process_profiles_tables(profiles_tables_work, add_row, n77_ssb_pre, n77_ssb_post, nodes_post=nodes_post_scope)

        # NEW: pass ALL required Post-Step2 tables via a single dict argument (in-memory tables)
        post_step2_tables = {
            "NRCellCU": df_nr_cell_cu,
            "EUtranFreqRelation": df_eutran_freq_rel,
            "McpcPCellNrFreqRelProfileUeCfg": profiles_tables_work.get("McpcPCellNrFreqRelProfileUeCfg", pd.DataFrame()),
            "TrStSaNrFreqRelProfileUeCfg": profiles_tables_work.get("TrStSaNrFreqRelProfileUeCfg", pd.DataFrame()),
        }

        cc_post_step2(post_step2_tables, add_row, n77_ssb_pre, n77_ssb_post, nodes_post=nodes_post_scope)

    # If nothing was added, return at least an informational row
    if not rows:
        rows.append({
            "Category": "Info",
            "SubCategory": "Info",
            "Metric": "SummaryAudit",
            "Value": "No data available",
            "ExtraInfo": "",
            "Tips": "",
        })

    # Build final DataFrame
    df = pd.DataFrame(rows)

    # Custom logical ordering for SummaryAudit (also drives PPT order)
    if not df.empty and all(col in df.columns for col in ["Category", "SubCategory", "Metric"]):
        # We now order only by (Category, SubCategory).
        # Inside each group, rows keep the insertion order.
        desired_order = [
            # MeContext
            ("MeContext", "MeContext Audit"),
            ("MeContext", "MeContext Inconsistencies"),

            # NR Frequency NRCellDU
            ("NRCellDU", "NR Frequency Audit"),
            ("NRCellDU", "NR Frequency Inconsistencies"),

            # NR Frequency NRSectorCarrier
            ("NRSectorCarrier", "NR Frequency Audit"),
            ("NRSectorCarrier", "NR Frequency Inconsistencies"),

            # NR Frequency NRFrequency
            ("NRFrequency", "NR Frequency Audit"),
            ("NRFrequency", "NR Frequency Inconsistencies"),

            # NR Frequency NRFreqRelation
            ("NRFreqRelation", "NR Frequency Audit"),
            ("NRFreqRelation", "NR Frequency Inconsistencies"),

            # NR Frequency NRCellRelation
            ("NRCellRelation", "NR Frequency Audit"),
            ("NRCellRelation", "NR Frequency Inconsistencies"),

            # NEW: ExternalNRCellCU
            ("ExternalNRCellCU", "NR Frequency Audit"),
            ("ExternalNRCellCU", "NR Frequency Inconsistencies"),

            # NEW: TermPointToGNodeB
            ("TermPointToGNodeB", "NR Termpoint Audit"),

            # NEW: TermPointToENodeB
            ("TermPointToENodeB", "X2 Termpoint Audit"),

            # LTE Frequency GUtranSyncSignalFrequency
            ("GUtranSyncSignalFrequency", "LTE Frequency Audit"),
            ("GUtranSyncSignalFrequency", "LTE Frequency Inconsistencies"),

            # LTE Frequency GUtranFreqRelation
            ("GUtranFreqRelation", "LTE Frequency Audit"),
            ("GUtranFreqRelation", "LTE Frequency Inconsistencies"),

            # LTE Frequency GUtranCellRelation
            ("GUtranCellRelation", "LTE Frequency Audit"),
            ("GUtranCellRelation", "LTE Frequency Inconsistencies"),

            # NEW: ExternalGUtranCell
            ("ExternalGUtranCell", "LTE Frequency Audit"),
            ("ExternalGUtranCell", "LTE Frequency Inconsistencies"),

            # NEW: TermPointToGNB
            ("TermPointToGNB", "X2 Termpoint Audit"),

            # EndcDistrProfile
            ("EndcDistrProfile", "ENDC Audit"),
            ("EndcDistrProfile", "ENDC Inconsistencies"),

            # FreqPrioNR
            ("FreqPrioNR", "ENDC Audit"),
            ("FreqPrioNR", "ENDC Inconsistencies"),

            # Cardinality NRFrequency
            ("Cardinality NRFrequency", "Cardinality Audit"),
            ("Cardinality NRFrequency", "Cardinality Inconsistencies"),

            # Cardinality NRFreqRelation
            ("Cardinality NRFreqRelation", "Cardinality Audit"),
            ("Cardinality NRFreqRelation", "Cardinality Inconsistencies"),

            # Cardinality GUtranSyncSignalFrequency
            ("Cardinality GUtranSyncSignalFrequency", "Cardinality Audit"),
            ("Cardinality GUtranSyncSignalFrequency", "Cardinality Inconsistencies"),

            # Cardinality GUtranFreqRelation
            ("Cardinality GUtranFreqRelation", "Cardinality Audit"),
            ("Cardinality GUtranFreqRelation", "Cardinality Inconsistencies"),

            # Profiles Audit (Post Step2)
            ("NRCellCU", "Profiles Audit"),
            ("EUtranFreqRelation", "Profiles Audit"),

            # Profiles tables
            ("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies"),
            ("McpcPCellNrFreqRelProfileUeCfg", "Profiles Discrepancies"),

            ("TrStSaNrFreqRelProfileUeCfg", "Profiles Inconsistencies"),
            ("TrStSaNrFreqRelProfileUeCfg", "Profiles Discrepancies"),

            ("McpcPCellProfileUeCfg", "Profiles Inconsistencies"),
            ("McpcPCellProfileUeCfg", "Profiles Discrepancies"),

            ("McpcPSCellProfileUeCfg", "Profiles Inconsistencies"),
            ("McpcPSCellProfileUeCfg", "Profiles Discrepancies"),

            ("UlQualMcpcMeasCfg", "Profiles Inconsistencies"),
            ("UlQualMcpcMeasCfg", "Profiles Discrepancies"),

            ("McfbCellProfile", "Profiles Inconsistencies"),
            ("McfbCellProfile", "Profiles Discrepancies"),

            ("McfbCellProfileUeCfg", "Profiles Inconsistencies"),
            ("McfbCellProfileUeCfg", "Profiles Discrepancies"),

            ("TrStSaCellProfile", "Profiles Inconsistencies"),
            ("TrStSaCellProfile", "Profiles Discrepancies"),

            ("TrStSaCellProfileUeCfg", "Profiles Inconsistencies"),
            ("TrStSaCellProfileUeCfg", "Profiles Discrepancies"),

            ("CaCellProfile", "Profiles Inconsistencies"),
            ("CaCellProfile", "Profiles Discrepancies"),

            ("CaCellProfileUeCfg", "Profiles Inconsistencies"),
            ("CaCellProfileUeCfg", "Profiles Discrepancies"),

            ("McpcPCellEUtranFreqRelProfile", "Profiles Inconsistencies"),
            ("McpcPCellEUtranFreqRelProfile", "Profiles Discrepancies"),

            ("McpcPCellEUtranFreqRelProfileUeCfg", "Profiles Inconsistencies"),
            ("McpcPCellEUtranFreqRelProfileUeCfg", "Profiles Discrepancies"),

            ("UeMCEUtranFreqRelProfile", "Profiles Inconsistencies"),
            ("UeMCEUtranFreqRelProfile", "Profiles Discrepancies"),

            ("UeMCEUtranFreqRelProfileUeCfg", "Profiles Inconsistencies"),
            ("UeMCEUtranFreqRelProfileUeCfg", "Profiles Discrepancies"),

        ]

        order_map = {k: i for i, k in enumerate(desired_order)}

        df["_order_"] = df.apply(
            lambda r: order_map.get((r["Category"], r["SubCategory"]), len(order_map)),
            axis=1,
        )

        df = df.sort_values(by=["_order_"], kind="stable").drop(columns="_order_").reset_index(drop=True)


    # Assign Tips to each Metric in SummaryAudit table
    preferred_cols = ["Category", "SubCategory", "Metric", "Value", "ExtraInfo", "Tips"]
    df = df[[c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]]

    old_ssb = n77_ssb_pre
    new_ssb = n77_ssb_post
    expected_old_rel_id = f"{n77_ssb_pre}-30-20-0-1"
    expected_new_rel_id = f"{n77_ssb_post}-30-20-0-1"

    allowed_pre_str = ", ".join(str(v) for v in sorted(allowed_n77_ssb_pre_set))
    allowed_post_str = ", ".join(str(v) for v in sorted(allowed_n77_ssb_post_set))

    # Tips column (requested in SummaryAudit slide updates)
    tips_by_metric: Dict[tuple[str, str], str] = {
        ("MeContext", f"Total unique nodes"): "MeContext table to create lists of sites for implementation",
        ("MeContext", f"Nodes with syncStatus='UNSYNCHRONIZED' (being excluded in all Audits)"): "Unsynch nodes excluded in Audits and Implementation",

        ("NRCellDU", f"NR Nodes with ssbFrequency"): " ",
        ("NRCellDU", f"NR LowMidBand Nodes"): " ",
        ("NRCellDU", f"NR mmWave Nodes"): " ",
        ("NRCellDU", f"NR nodes with N77 SSB in band (646600-660000)"): " ",
        ("NRCellDU", f"NR nodes with N77 SSB in Pre-Retune allowed list ({allowed_pre_str})"): "Nodes with N77A Step2b pending",
        ("NRCellDU", f"NR nodes with N77 SSB in Post-Retune allowed list ({allowed_post_str})"): "Nodes with N77A Step2b completed",
        ("NRCellDU", f"NR nodes with N77 SSB not in Pre/Post Retune allowed lists"): "These nodes must be checked in preparation phase and confirm if any special action needed",

        ("NRSectorCarrier", f"NR nodes with N77 ARCFN in band (646600-660000)"): " ",
        ("NRSectorCarrier", f"NR nodes with N77 ARCFN in Pre-Retune allowed list ({allowed_pre_str})"): "Nodes with N77B Step2b pending",
        ("NRSectorCarrier", f"NR nodes with N77 ARCFN in Post-Retune allowed list ({allowed_post_str})"): "Nodes with N77B Step2b completed",
        ("NRSectorCarrier", f"NR nodes with N77 ARCFN not in Pre/Post Retune allowed lists"): "These nodes must be checked in preparation phase and confirm if any special action needed",

        ("NRFreqRelation", f"NR nodes with the old N77 SSB ({n77_ssb_pre})"): " ",
        ("NRFreqRelation", f"NR nodes with the new N77 SSB ({n77_ssb_post})"): " ",
        ("NRFreqRelation", f"NR nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post})"): "Need to run Step1 on these nodes",
        ("NRFreqRelation", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post})"): "Need to run Step1 on these nodes",
        ("NRFreqRelation", f"NR nodes with some cells missing relations to new SSB ({n77_ssb_post})"): "Nodes with any relations Step1 pending",
        ("NRFreqRelation", f"NR nodes with the new N77 SSB ({n77_ssb_post}) NRFreqRelation pointing to mcpcPCellNrFreqRelProfileRef containing new SSB name (cloned) or Other"): "Nodes with Step1 completed",
        ("NRFreqRelation", f"NR nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post})"): " ",
        ("NRFreqRelation", f"NR nodes with Auto-created NRFreqRelationId to new N77 SSB ({n77_ssb_post}) but not following VZ naming convention (e.g. with extra characters: 'auto_{n77_ssb_post}')"): " ",
        ("NRFreqRelation", f"NR Nodes with the new N77 SSB ({n77_ssb_post}) and NRFreqRelation reference to McpcPCellNrFreqRelProfile with old SSB before '_' ({n77_ssb_pre}_xxxx)"): "Need to review Step2b execution",
        ("NRFreqRelation", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) and the new SSB ({n77_ssb_post}) NRFreqRelation pointing to same mcpcPCellNrFreqRelProfileRef containing old SSB name"): "Need to run Step1 on these nodes",
        ("NRFreqRelation", f"NR nodes with mismatching params (cell-level) between old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post})"): "Need to review. Step1 run could solve most cases",

        ("NRCellRelation", f"NR cellRelations to old N77 SSB ({old_ssb})"): "Post Step2 some relations pointing to other Mkts could be on old SSB. See details in table",
        ("NRCellRelation", f"NR cellRelations to new N77 SSB ({new_ssb})"): " ",

        ("ExternalNRCellCU", f"External cells to old N77 SSB ({old_ssb})"): "PN and DAS might have no External relations defined",
        ("ExternalNRCellCU", f"External cells to new N77 SSB ({new_ssb})"): "PN and DAS might have no External relations defined",

        ("TermPointToGNodeB", f"NR to NR TermPoints with administrativeState=LOCKED"): " ",
        ("TermPointToGNodeB", f"NR to NR TermPoints with operationalState=DISABLED"): " ",

        ("TermPointToENodeB", f"NR to LTE TermPoints with administrativeState=LOCKED"): "Helpful to troubleshoot External updates",
        ("TermPointToENodeB", f"NR to LTE TermPoints with operationalState=DISABLED"): "Helpful to troubleshoot External updates",

        ("GUtranFreqRelation", f"LTE nodes with the old N77 SSB"): " ",
        ("GUtranFreqRelation", f"LTE nodes with the new N77 SSB"): " ",
        ("GUtranFreqRelation", f"LTE nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post})"): "Nodes with Step1 completed",
        ("GUtranFreqRelation", f"LTE nodes with the old N77 SSB ({n77_ssb_pre}) but without the new SSB ({n77_ssb_post})"): "Need to run Step1 on these nodes",
        ("GUtranFreqRelation", f"LTE nodes with some cells missing relations to new SSB {n77_ssb_post}"): "Need to run Step1 on these nodes",
        ("GUtranFreqRelation", f"LTE nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post})"): " ",
        ("GUtranFreqRelation", f"LTE nodes with Auto-created GUtranFreqRelationId to new N77 SSB ({n77_ssb_post}) but not following VZ naming convention ({n77_ssb_post}-30-20-0-1)"): "Not an issue, unless other inconsistencies raised",
        ("GUtranFreqRelation", f"LTE nodes with same endcB1MeasPriority in old N77 SSB"): "Need to review and fix with Step1 or Step2c",
        ("GUtranFreqRelation", f"LTE nodes with mismatching params between GUtranFreqRelationId {n77_ssb_pre} and {n77_ssb_post}"): "Parameters qQualMin, qRxLevMin, threshXHigh agreed to set to fixed values on new freqs and inconsistencies should be reported to VZ. Other inconsistent parameters would require review for further actions.",

        ("GUtranCellRelation", f"LTE cellRelations to old N77 SSB"): "Post Step2 some relations pointing to other Mkts could be on old SSB. See details in table",
        ("GUtranCellRelation", f"LTE cellRelations to new N77 SSB"): " ",

        ("ExternalGUtranCell", f"External cells to old N77 SSB ({old_ssb})"): "Post Step2 some relations pointing to other Mkts could be on old SSB. See details in table",
        ("ExternalGUtranCell", f"External cells to new N77 SSB ({new_ssb})"): " ",
        ("ExternalGUtranCell", f"External cells to old N77 SSB ({old_ssb}) with serviceStatus=OUT_OF_SERVICE"): "Externals with unavailable TermPoints are not operational for ENDC and might not be updated immediately after Step2",
        ("ExternalGUtranCell", f"External cells to new N77 SSB ({new_ssb}) with serviceStatus=OUT_OF_SERVICE"): " ",

        ("TermPointToGNB", f"LTE to NR TermPoints with administrativeState=LOCKED"): "Helpful to troubleshoot External updates",
        ("TermPointToGNB", f"LTE to NR TermPoints with operationalState=DISABLED"): "Helpful to troubleshoot External updates",
        ("TermPointToGNB", f"LTE to NR TermPoints with usedIpAddress=0.0.0.0/::"): "Helpful to troubleshoot External updates",

        ("EndcDistrProfile", f"Nodes with gUtranFreqRef containing the old N77 SSB ({n77_ssb_pre}) and the N77B SSB ({n77b_ssb})"): "Need to run Step2c on this nodes",
        ("EndcDistrProfile", f"Nodes with gUtranFreqRef containing the new N77 SSB ({n77_ssb_post}) and the N77B SSB ({n77b_ssb})"): "Nodes with Step2c completed",
        ("EndcDistrProfile", f"Nodes with mandatoryGUtranFreqRef containing the old N77 SSB ({n77_ssb_pre}) and the N77B SSB ({n77b_ssb})"): "Need to run Step2c on this nodes",
        ("EndcDistrProfile", f"Nodes with mandatoryGUtranFreqRef containing the new N77 SSB"): "Nodes with Step2c completed",
        ("EndcDistrProfile", f"Nodes with gUtranFreqRef not containing N77 SSBs ({n77_ssb_pre} or {n77_ssb_post}) together with N77B SSB ({n77b_ssb})"): "Need to run Step1 on this nodes",
        ("EndcDistrProfile", f"Nodes with mandatoryGUtranFreqRef not empty and not containing N77 SSBs ({n77_ssb_pre} or {n77_ssb_post}) together with N77B SSB ({n77b_ssb})"): "These nodes must be checked in preparation phase and confirm if any special action needed",

        ("FreqPrioNR", f"LTE nodes with the old N77 SSB ({old_ssb}) but without the new N77 SSB ({new_ssb})"): "Need to run Step1 on this nodes if existing GUtranFreqRelation",
        ("FreqPrioNR", f"LTE nodes with both, the old N77 SSB ({old_ssb}) and the new N77 SSB ({new_ssb})"): "Nodes with Step1 completed",
        ("FreqPrioNR", f"NR nodes with RATFreqPrioId = 'fwa' in N77 band"): " ",
        ("FreqPrioNR", "NR nodes with RATFreqPrioId = 'publicsafety' in N77 band"): " ",
        ("FreqPrioNR", f"LTE nodes with mismatching params (cell-level) between FreqPrioNR {old_ssb} and {new_ssb}"): " ",
        ("FreqPrioNR", f"NR nodes with RATFreqPrioId different from 'fwa'/'publicsafety' in N77 band"): "These nodes must be checked in preparation phase and confirm if any special action needed",

        ("Cardinality NRFrequency", f"NR nodes with NRFrequency definitions at limit 64"): "These nodes must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality NRFreqRelation", f"NR cells with NRFreqRelation count at limit 16"): "These cells must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality GUtranSyncSignalFrequency", f"LTE nodes with GUtranSyncSignalFrequency definitions at limit 24"): "These nodes must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality GUtranFreqRelation", f"LTE cells with GUtranFreqRelation count at limit 16"): "These cells must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality NRFrequency", f"Max NRFrequency definitions per node (limit 64)"): "These nodes must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality NRFreqRelation", f"Max NRFreqRelation per NR cell (limit 16)"): "These cells must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality GUtranSyncSignalFrequency", f"Max GUtranSyncSignalFrequency definitions per node (limit 24)"): "These nodes must be checked in preparation phase and confirm if any special action needed",
        ("Cardinality GUtranFreqRelation", f"Max GUtranFreqRelation per LTE cell (limit 16)"): "These cells must be checked in preparation phase and confirm if any special action needed",

        ("", "Profiles Inconsistencies"): "Need to review profile consistency before/after retune",
        ("", "Profiles Discrepancies"): "Need to review profile discrepancies before/after retune",
    }

    if not df.empty:
        def _normalize_metric_for_tip_lookup(text: object) -> str:
            s = str(text or "").strip()
            s = re.sub(r"\s*\(from [^)]*\)", "", s, flags=re.IGNORECASE)  # Remove "(from ...)" source suffix
            s = re.sub(r"\s*\((?=[^)]*\d)[^)]*\)", "", s)  # Remove any "(...)" group containing digits (frequencies, ids, lists)
            s = re.sub(r"\d{3,}(?:-\d+)+", "<id>", s)  # Normalize ids like 648672-30-20-0-1 or ranges 646600-660000
            s = re.sub(r"\d{3,}", "<n>", s)  # Normalize long numeric values like 648672, 653952, 650006, etc.
            s = re.sub(r"\s+", " ", s).strip()
            return s.casefold()

        def _tip_for_metric(row_tuple: object) -> str:
            if not isinstance(row_tuple, tuple) or len(row_tuple) != 3:
                return "Pair (Category, SubCategory, Metric) is not a Tuple. Cannot match Metric. Tip not found."

            category = str(row_tuple[0] or "").strip()
            subcategory = str(row_tuple[1] or "").strip()
            metric = str(row_tuple[2] or "").strip()

            # 1) Exact match for (Category, Metric)
            exact = tips_by_metric.get((category, metric))
            if exact is not None:
                return exact

            # 2) Prefix match inside SAME Category only (ignoring variable numeric values)
            metric_norm = _normalize_metric_for_tip_lookup(metric)
            best_tip = None
            best_len = -1

            for (k_cat, k_metric), tip in tips_by_metric.items():
                if str(k_cat or "").strip() != category:
                    continue
                k_norm = _normalize_metric_for_tip_lookup(k_metric)
                if metric_norm.startswith(k_norm) and len(k_norm) > best_len:
                    best_tip = tip
                    best_len = len(k_norm)

            if best_tip is not None:
                return best_tip

            # 3) Generic tips per SubCategory using keys like ("", "Profiles Inconsistencies")
            if subcategory:
                generic = tips_by_metric.get(("", subcategory))
                if generic is not None:
                    return generic

            return "Metric match failed. Tip not found"

        category_list = df["Category"].astype(str).tolist() if "Category" in df.columns else [""] * len(df)
        subcategory_list = df["SubCategory"].astype(str).tolist() if "SubCategory" in df.columns else [""] * len(df)
        metric_list = df["Metric"].astype(str).tolist() if "Metric" in df.columns else [""] * len(df)
        df["Tips"] = [_tip_for_metric(t) for t in zip(category_list, subcategory_list, metric_list)]

    # Build NR param mismatching dataframe
    df_param_mismatch_nr = pd.DataFrame(param_mismatch_rows_nr, columns=param_mismatch_columns_nr)
    if df_param_mismatch_nr.empty:
        df_param_mismatch_nr = pd.DataFrame(columns=param_mismatch_columns_nr)

    # Build LTE param mismatching dataframe
    df_param_mismatch_gu = pd.DataFrame(param_mismatch_rows_gu, columns=param_mismatch_columns_gu)
    if df_param_mismatch_gu.empty:
        df_param_mismatch_gu = pd.DataFrame(columns=param_mismatch_columns_gu)

    return df, df_param_mismatch_nr, df_param_mismatch_gu
