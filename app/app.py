"""RetailBank KPI Dashboard – Streamlit (Databricks App). Layout follows the Figma design 'RetailBank KPI Dashboard'."""
# HOW TO EXPLAIN THIS FILE
# ------------------------
# app.py builds the 8 screens of the Figma design. Streamlit re-runs this script from top to bottom every time the
# user clicks something; widgets (selectbox, toggle ...) keep their values in st.session_state between runs.
# Structure of the file:
#   1. page set-up + theme                        (top)
#   2. "shell": user name, batch selector, top bar (current_user, batch_selector, top_bar)
#   3. one function per page: page_overview(), page_transactions(), page_risk(), page_dq(), page_day2(),
#      page_pipeline(), page_customers(), page_reports(), page_settings()
#   4. navigation: st.navigation registers the pages in the sidebar and runs the selected one (bottom)
# Data comes ONLY from data.py (module D); the visual components come from ui.py (module ui).
# Every page reads the selected batch from st.session_state["batch"], so the "As of DAY1/DAY2" selector
# switches every KPI between the Day-1 snapshot and the Day-2 snapshot.
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import data as D
import ui
from ui import e, inr, num, pct, pill, delta_pill, bar

# Must be the first Streamlit call: browser tab title, wide layout, sidebar open.
st.set_page_config(page_title="RetailBank KPI Dashboard", page_icon="🏦", layout="wide", initial_sidebar_state="expanded")
# Theme chosen in the sidebar toggle (dark by default, like the main Figma frames). Then inject the matching CSS
# and keep the colour palette in T for charts and inline styles.
st.session_state.setdefault("theme", "dark")
ui.inject_css()
T = ui.theme()

# Value -> pill colour mappings used in tables (green / red / yellow / blue / grey pills).
TONE_STATUS = {"Success": "pos", "Failed": "neg", "Pending": "warn"}
TONE_KYC = {"Verified": "pos", "Pending": "warn", "Rejected": "neg", "Unknown": "mute"}
TONE_SEG = {"Platinum": "acc", "Gold": "warn", "Silver": "info", "Bronze": "mute"}
TONE_RISK = {"CRITICAL": "neg", "HIGH": "warn", "MEDIUM": "info"}
# Human readable names of the 3 fraud rules of KPI 5.
RULE_LABEL = {"HIGH_VALUE_GT_1L": "> ₹1 lakh", "VELOCITY_3PLUS_IN_10MIN": "3+ txns in 10 min", "ODD_HOUR_00_05": "00:00–05:00"}


# ====================================================================== shell
# Databricks Apps put the signed-in user's name/email in request headers -> shown in the top bar.
def current_user():
    try:
        h = st.context.headers
        return h.get("X-Forwarded-Preferred-Username") or h.get("X-Forwarded-Email") or "Analyst"
    except Exception:
        return "Analyst"


# Reads rb_audit.batch_control. Only SUCCEEDED batches can be selected; the latest one is the default.
# If the warehouse or tables are not reachable, show the error and stop the page (st.stop).
def batch_selector():
    try:
        b = D.batches()
    except Exception as ex:
        st.error(f"Cannot reach the SQL warehouse / tables: {ex}")
        st.stop()
    done = b[b["status"] == "SUCCEEDED"]
    if done.empty:
        st.warning("No successful batch yet – run the pipeline job first.")
        st.stop()
    opts = done["batch_id"].tolist()
    st.session_state.setdefault("batch", opts[-1])
    if st.session_state["batch"] not in opts:
        st.session_state["batch"] = opts[-1]
    return b, opts


# Top bar of the design: avatar + user name, search box, "As of" batch selector, Export button (links to Reports).
def top_bar(opts):
    user = current_user()
    initials = "".join(p[0] for p in user.split("@")[0].replace(".", " ").replace("_", " ").split()[:2]).upper() or "RB"
    c1, c2, c3, c4 = st.columns([3.2, 3.6, 1.6, 1.2], vertical_alignment="center")
    with c1:
        st.markdown(f'<div class="rb-top"><div class="rb-avatar">{e(initials)}</div><div class="rb-user"><b>{e(user.split("@")[0])}</b>'
                    f'<span>RetailBank analytics</span></div>{pill("Senior DE", "warn")}</div>', unsafe_allow_html=True)
    with c2:
        st.text_input("search", placeholder="Search customers, accounts, branches…", label_visibility="collapsed", key="global_search")
    with c3:
        st.selectbox("Batch", opts, key="batch", label_visibility="collapsed",
                     format_func=lambda x: f"As of {x.upper()}")
    with c4:
        st.page_link(PAGES["reports"], label="Export", icon=":material/download:")


# Like D.kpi() but returns an empty table instead of crashing when a KPI does not exist for that batch
# (e.g. KPI 11 exists only for Day 2).
def kpi_or_empty(name, batch, **kw) -> pd.DataFrame:
    try:
        return D.kpi(name, batch, **kw)
    except Exception:
        return pd.DataFrame()


# ====================================================================== 1 Overview
# ================================ PAGE 1 · OVERVIEW =====================================================
# Row 1: 3 KPI tiles (net volume, flagged items, KYC exposure) + data pipeline card.
# Row 2: monthly trend by region (KPI 2) + top 5 customers (KPI 1).
# Row 3: product contribution (KPI 3) + top/bottom branch per region (KPI 7).
def page_overview():
    b = st.session_state["batch"]
    ui.page_header("Overview", "Customer transaction analytics across all branches")
    # all tile numbers in one query; trend + flagged per month feed the sparklines
    h = D.headline(b)
    trend = D.daily_net(b)
    flag_m = D.monthly_flagged()
    c = st.columns([1, 1, 1, 1.15])
    with c[0]:
        # c[0..3] are the 4 columns of the first row
        ui.tile("Net volume", inr(h["net_value"]), pill("Success · INR · net of refunds", "pos"), "₹", "Successful INR, refunds netted",
                tag="KPI 1", spark=trend["net_value"].tolist())
    with c[1]:
        ui.tile("Flagged items", num(h["flagged"]), pill(f"{h['flagged'] / max(h['fact_rows'], 1) * 100:.1f}% of transactions", "warn"),
                "⚠", "Suspicious transactions", tag="KPI 5", spark=flag_m["flagged"].tolist())
    with c[2]:
        ui.tile("KYC exposure", pct(h["kyc_pct"]), pill(f"{num(h['kyc_txns'])} txns by non-verified", "neg"), "🛡",
                "Value by non-verified KYC", tag="KPI 8", tag_tone="neg")
    with c[3]:
        with st.container(border=True):
            # records processed / passed for the selected batch (from the KPI 10 scorecard)
            sc = D.scorecard_all()
            bsc = sc[sc["as_of_batch"] == b]
            ui.card_header("Data pipeline", "Medallion · serverless", tag=b.upper(), tag_tone="acc")
            tot_r, tot_p, tot_x = int(bsc["received"].sum()), int(bsc["passed"].sum()), int(bsc["rejected"].sum())
            st.markdown(f'<div class="rb-mini"><small>Records processed ({b})</small><b>{num(tot_r)}</b> &nbsp;{pill(f"{tot_p / max(tot_r, 1) * 100:.1f}% passed", "pos")}</div>',
                        unsafe_allow_html=True)
            st.page_link(PAGES["pipeline"], label="Run batch / pipeline runs", icon=":material/play_arrow:")

    c = st.columns([2.1, 1.25])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Monthly trend", "Net value by region", tag="KPI 2")
            m = kpi_or_empty("kpi02_monthly_trend", b, where="grain = 'REGION'", order="month")
            fig = go.Figure()
            for i, (reg, g) in enumerate(m.groupby("region")):
                fig.add_trace(go.Scatter(x=g["month"], y=g["net_value"], name=reg, mode="lines+markers", line=dict(width=2.4, shape="spline"),
                                         marker=dict(size=5), hovertemplate="%{x|%b %Y}<br>₹%{y:,.0f}<extra>" + reg + "</extra>"))
            ui.chart(fig, 318)
    with c[1]:
        with st.container(border=True):
            ui.card_header("Top customers", "By net transaction value", tag="KPI 1")
            top = kpi_or_empty("kpi01_customer_net_ranking", b, where="is_top5", order="rank")
            rows = "".join(
                f'<div class="rb-row"><div class="rb-avatar" style="width:30px;height:30px;font-size:11px">{e(r.customer_name.split("_")[-1])}</div>'
                f'<div style="flex:1"><b>{e(r.customer_name)}</b><br><span style="color:{T["muted"]};font-size:11.5px">{e(r.customer_id)} · {int(r.txn_count)} txns</span></div>'
                f'<div style="text-align:right"><b>{inr(r.net_value)}</b><br>{pill(r.kyc_status, TONE_KYC.get(r.kyc_status, "mute"))}</div></div>'
                for r in top.itertuples())
            st.markdown(rows, unsafe_allow_html=True)
    c = st.columns([1, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Product contribution", "Revenue share – current quarter", tag="KPI 3")
            p = kpi_or_empty("kpi03_product_revenue", b, where="grain = 'PRODUCT_TYPE' AND is_current_quarter", order="revenue DESC")
            mx = p["revenue_share_pct"].max() if not p.empty else 1
            ui.html_table(p, [
                ("Product", lambda r: f"<b>{e(r.dim_name)}</b>", ""),
                ("", lambda r: bar(r.revenue_share_pct / mx), ""),
                ("Share", lambda r: pct(r.revenue_share_pct), "num"),
                ("QoQ", lambda r: delta_pill(r.share_change_pp, " pp"), "num")])
    with c[1]:
        with st.container(border=True):
            ui.card_header("Branch ranking", "Top & bottom per region", tag="KPI 7")
            br = kpi_or_empty("kpi07_branch_ranking", b, where="is_top_performer OR is_bottom_performer")
            tops = br[br.is_top_performer].groupby("region").first()
            bots = br[br.is_bottom_performer].groupby("region").first()
            df = pd.DataFrame({"top": tops["branch_id"] + " · " + tops["net_revenue"].map(inr),
                               "bottom": bots["branch_id"] + " · " + bots["net_revenue"].map(inr)}).reset_index()
            ui.html_table(df, [("Region", lambda r: e(r.region), ""),
                               ("Top", lambda r: f'<span style="color:{T["pos"]}">{e(r.top)}</span>', ""),
                               ("Bottom", lambda r: f'<span style="color:{T["neg"]}">{e(r.bottom)}</span>', "")])


# ====================================================================== 2 Transactions & Branches
# ================================ PAGE 2 · TRANSACTIONS & BRANCHES ======================================
# Monthly trend table by region with MoM pills (KPI 2), branch ranking within region (KPI 7),
# product contribution vs prior quarter (KPI 3), refund-rate heatmap + outliers (KPI 9).
def page_transactions():
    b = st.session_state["batch"]
    ui.page_header("Transactions & Branches", "Monthly trend, branch ranking, product revenue and refund analysis")
    m = kpi_or_empty("kpi02_monthly_trend", b, order="month")
    with st.container(border=True):
        ui.card_header("Monthly transaction trend by region", "Net value per month with month-over-month growth (LAG window)", tag="KPI 2")
        reg = m[m.grain == "REGION"].copy()
        months = sorted(reg["month"].unique())[-3:]
        piv = reg[reg.month.isin(months)].pivot_table(index="region", columns="month", values="net_value", aggfunc="sum")
        last = reg[reg.month == months[-1]].set_index("region") if months else pd.DataFrame()
        rows = piv.reset_index()
        cols = [("Region", lambda r: f"<b>{e(r.region)}</b>", "")]
        for mo in months:
            cols.append((pd.Timestamp(mo).strftime("%b %Y"), (lambda mo: lambda r: inr(r[mo]))(mo), "num"))
        cols += [("MoM value", lambda r: delta_pill(last.loc[r.region, "mom_value_growth_pct"] if r.region in last.index else None), "num"),
                 ("MoM count", lambda r: delta_pill(last.loc[r.region, "mom_txn_count_growth_pct"] if r.region in last.index else None), "num")]
        ui.html_table(rows, cols)
        latest_days = D.q(f"SELECT count(DISTINCT txn_date) d, max(txn_date) mx FROM {D.G}.fact_transaction "
                          f"WHERE trunc(txn_date, 'MM') = (SELECT trunc(max(txn_date), 'MM') FROM {D.G}.fact_transaction)").iloc[0]
        if months and int(latest_days["d"]) < 20:
            st.caption(f"{pd.Timestamp(months[-1]).strftime('%b %Y')} is a partial month ({int(latest_days['d'])} day(s) of data up to "
                       f"{latest_days['mx']}), so its month-over-month change is not comparable yet.")
        with st.expander("Branch-level monthly trend"):
            regions = sorted(m.region.dropna().unique())
            sel = st.multiselect("Regions", regions, default=regions[:1])
            bm = m[(m.grain == "BRANCH") & (m.region.isin(sel))]
            fig = go.Figure()
            for bid, g in bm.groupby("branch_id"):
                fig.add_trace(go.Scatter(x=g["month"], y=g["net_value"], name=bid, mode="lines"))
            ui.chart(fig, 300)

    c = st.columns([1.6, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Branch ranking within region", "RANK() / DENSE_RANK() on net revenue", tag="KPI 7")
            br = kpi_or_empty("kpi07_branch_ranking", b, order="region, rank_in_region")
            region = st.segmented_control("Region", ["All"] + sorted(br.region.unique()), default="All", label_visibility="collapsed")
            v = br if region in (None, "All") else br[br.region == region]
            v = v.sort_values(["region", "rank_in_region"]).groupby("region").head(6 if region in (None, "All") else 100)
            mx = br["net_revenue"].abs().max() or 1
            ui.html_table(v, [
                ("Rank", lambda r: f"#{int(r.rank_in_region)}", ""),
                ("Branch", lambda r: f"<b>{e(r.branch_name)}</b><br><span style='color:{T['muted']};font-size:11px'>{e(r.branch_id)} · {e(r.location)}</span>", ""),
                ("Region", lambda r: e(r.region), ""),
                ("Revenue", lambda r: inr(r.net_revenue), "num"),
                ("Share of best", lambda r: bar(r.net_revenue / mx, T["pos"] if r.net_revenue >= 0 else T["neg"]), ""),
                ("", lambda r: pill("Top", "pos") if r.is_top_performer else (pill("Bottom", "neg") if r.is_bottom_performer else ""), "")],
                max_height=430)
    with c[1]:
        with st.container(border=True):
            ui.card_header("Product contribution", "Current vs prior quarter", tag="KPI 3")
            grain = st.segmented_control("Grain", ["CATEGORY", "PRODUCT_TYPE", "PRODUCT"], default="PRODUCT_TYPE", label_visibility="collapsed")
            p = kpi_or_empty("kpi03_product_revenue", b, where=f"grain = '{grain or 'PRODUCT_TYPE'}' AND is_current_quarter", order="revenue DESC")
            ui.html_table(p.head(12), [
                ("Name", lambda r: f"<b>{e(r.dim_name or r.dim_id)}</b>", ""),
                ("Revenue", lambda r: inr(r.revenue), "num"),
                ("Share", lambda r: pct(r.revenue_share_pct), "num"),
                ("Prior Q", lambda r: inr(r.prior_quarter_revenue), "num"),
                ("Change", lambda r: delta_pill(r.qoq_revenue_change_pct), "num")], max_height=360)
            if not p.empty:
                st.caption(f"Quarter starting {pd.Timestamp(p.quarter.iloc[0]).strftime('%d %b %Y')} – partial quarters make QoQ volatile.")

    with st.container(border=True):
        ui.card_header("Refund rate by product and region", "Refund count as % of transactions · heat = higher refund rate", tag="KPI 9")
        # pivot: rows = product type, columns = region, values = refund rate % -> Plotly heatmap
        hm = D.refund_heatmap()
        piv = hm.pivot_table(index="product_type", columns="region", values="refund_rate_pct")
        fig = go.Figure(go.Heatmap(z=piv.values, x=piv.columns, y=piv.index, text=piv.values, texttemplate="%{text:.1f}%",
                                   colorscale=[[0, T["heat"][0]], [.35, T["heat"][1]], [.7, T["heat"][2]], [1, T["heat"][3]]],
                                   showscale=False, xgap=4, ygap=4, hovertemplate="%{y} · %{x}<br>%{z:.1f}% refunds<extra></extra>"))
        ui.chart(fig, 260, legend=False)
        k9 = kpi_or_empty("kpi09_refund_rate", b, where="is_outlier", order="refund_rate_pct DESC")
        if not k9.empty:
            st.markdown("Outliers (z-score > 2): " + " ".join(pill(f"{r.grain.title()} {r.dim_id} · {r.refund_rate_pct:.0f}%", "neg") for r in k9.itertuples()),
                        unsafe_allow_html=True)


# ====================================================================== 3 Risk & Customers
# ================================ PAGE 3 · RISK & CUSTOMERS =============================================
# Tiles (dormant, high-risk dormant, flagged, KYC exposure), suspicious transactions with filters (KPI 5),
# KYC donut (KPI 8), RFM segments (KPI 6), dormant accounts list (KPI 4).
def page_risk():
    b = st.session_state["batch"]
    ui.page_header("Risk & Customers", "Dormancy, suspicious activity, customer segments and KYC exposure")
    h = D.headline(b)
    c = st.columns(4)
    with c[0]:
        ui.tile("Dormant accounts", num(h["dormant"]), pill("No txn in last 90 days", "mute"), "⏸", "KPI 4")
    with c[1]:
        ui.tile("High-risk dormant", num(h["hr_dormant"]), pill("Credit Card / Loan", "neg"), "⚑", "KPI 4", tag_tone="neg")
    with c[2]:
        ui.tile("Flagged transactions", num(h["flagged"]), pill("Rules: >₹1L · velocity · 00–05h", "warn"), "⚠", "KPI 5")
    with c[3]:
        ui.tile("KYC exposure", pct(h["kyc_pct"]), pill("of value by non-verified", "neg"), "🛡", "KPI 8")

    c = st.columns([1.7, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("High-value and suspicious transactions", "Amount > ₹1,00,000 · ≥3 txns in 10 min · 00:00–04:59", tag="KPI 5")
            s = kpi_or_empty("kpi05_suspicious_transactions", b, order="risk_score DESC, amount DESC")
            f1, f2 = st.columns(2)
            lvl = f1.multiselect("Risk level", ["CRITICAL", "HIGH", "MEDIUM"], default=["CRITICAL", "HIGH", "MEDIUM"])
            rule = f2.multiselect("Rule", list(RULE_LABEL), format_func=RULE_LABEL.get)
            v = s[s.risk_level.isin(lvl)]
            if rule:
                v = v[v.risk_reason_text.apply(lambda t: any(x in t for x in rule))]
            ui.html_table(v.head(60), [
                ("Transaction", lambda r: f"<b>{e(r.transaction_id)}</b>", ""),
                ("Account", lambda r: e(r.account_id), ""),
                ("Amount", lambda r: inr(r.amount, short=False), "num"),
                ("Time", lambda r: pd.Timestamp(r.txn_ts).strftime("%d %b %H:%M"), ""),
                ("Status", lambda r: pill(r.status, TONE_STATUS.get(r.status, "mute")), ""),
                ("Risk reason", lambda r: " ".join(pill(RULE_LABEL.get(x, x), "neg" if "VALUE" in x else ("warn" if "HOUR" in x else "info"))
                                                   for x in r.risk_reason_text.split(", ")), ""),
                ("Level", lambda r: pill(r.risk_level, TONE_RISK.get(r.risk_level, "mute")), "")], max_height=420)
            st.caption(f"{len(v):,} of {len(s):,} flagged transactions shown (max 60 rows).")
    with c[1]:
        with st.container(border=True):
            ui.card_header("KYC risk exposure", "Share of transaction value by KYC status", tag="KPI 8")
            k = kpi_or_empty("kpi08_kyc_exposure", b, where="kyc_status <> 'ALL_NON_VERIFIED'")
            col = {"Verified": T["pos"], "Pending": T["warn"], "Rejected": T["neg"]}
            # donut chart = pie chart with a big hole; the % of non-verified value is written in the middle
            fig = go.Figure(go.Pie(labels=k.kyc_status, values=k.net_value, hole=.72, sort=False,
                                   marker=dict(colors=[col.get(x, T["muted"]) for x in k.kyc_status], line=dict(width=0)),
                                   textinfo="none", hovertemplate="%{label}: ₹%{value:,.0f} (%{percent})<extra></extra>"))
            fig.add_annotation(text=f"<b>{pct(h['kyc_pct'])}</b><br><span style='font-size:11px'>non-verified</span>", showarrow=False,
                               font=dict(size=22, color=T["text"]))
            ui.chart(fig, 220, legend=False)
            ui.html_table(k, [("Status", lambda r: pill(r.kyc_status, TONE_KYC.get(r.kyc_status, "mute")), ""),
                              ("Txns", lambda r: num(r.txn_count), "num"),
                              ("Value", lambda r: pct(r.pct_of_value), "num")])

    c = st.columns([1, 1.25])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Customer segmentation (RFM-lite)", "NTILE(3) on recency, frequency, monetary", tag="KPI 6")
            r = kpi_or_empty("kpi06_customer_rfm_segments", b)
            seg = r.groupby("segment").agg(customers=("customer_id", "count"), monetary=("monetary", "sum")).reindex(
                ["Platinum", "Gold", "Silver", "Bronze"]).fillna(0).reset_index()
            mx = seg.customers.max() or 1
            colors = {"Platinum": T["accent"], "Gold": T["warn"], "Silver": T["info"], "Bronze": T["muted"]}
            ui.html_table(seg, [("Segment", lambda x: pill(x.segment, TONE_SEG[x.segment]), ""),
                                ("", lambda x: bar(x.customers / mx, colors[x.segment]), ""),
                                ("Customers", lambda x: num(x.customers), "num"),
                                ("Value", lambda x: inr(x.monetary), "num")])
            st.caption("Score = R+F+M (3–9): Platinum ≥ 8 · Gold 6–7 · Silver 4–5 · Bronze 3")
    with c[1]:
        with st.container(border=True):
            ui.card_header("Dormant accounts", "No transactions in the 90 days before the business date", tag="KPI 4")
            d = kpi_or_empty("kpi04_dormant_accounts", b, order="is_high_risk_dormant DESC, days_since_last_txn DESC")
            only_hr = st.toggle("High-risk only", value=True)
            v = d[d.is_high_risk_dormant] if only_hr else d
            ui.html_table(v.head(40), [
                ("Account", lambda x: f"<b>{e(x.account_id)}</b>", ""),
                ("Customer", lambda x: e(x.customer_name), ""),
                ("Products", lambda x: e(", ".join(list(x.product_types)) if x.product_types is not None else ""), ""),
                ("Days idle", lambda x: num(x.days_since_last_txn) if pd.notna(x.days_since_last_txn) else "never", "num"),
                ("Risk", lambda x: pill("High risk", "neg") if x.is_high_risk_dormant else pill("Dormant", "mute"), "")], max_height=330)


# ====================================================================== 4 Data Quality
# ================================ PAGE 4 · DATA QUALITY =================================================
# Pipeline steps of the selected run, KPI 10 scorecard per file, automated quality gates, rejections by reason,
# and tabs with the fixes applied, a quarantine explorer, the column profile and the rule catalogue.
def page_dq():
    b = st.session_state["batch"]
    ui.page_header("Data Quality", "Profiling, cleansing rules, quarantine and automated quality gates")
    sc = D.scorecard_all()
    bsc = sc[sc.as_of_batch == b]
    ing = D.ingestion(b)
    with st.container(border=True):
        ui.card_header(f"Pipeline run – {b}", "Bronze → Silver → Gold with a blocking DQ gate", tag="SUCCEEDED", tag_tone="pos")
        chk = D.dq_checks(b)
        blocking = int(((chk.status == "FAIL") & (chk.severity == "ERROR")).sum())
        steps = [("Ingest", f"{len(ing)} files · {num(ing.row_count.sum())} rows"),
                 ("Profile", "column profile + patterns"),
                 ("Cleanse", f"{num(bsc.passed_with_fixes.sum())} rows fixed"),
                 ("Quarantine", f"{num(bsc.rejected.sum())} rejected"),
                 ("DQ gate", f"{len(chk)} checks · {blocking} blocking"),
                 ("Model + KPIs", "SCD2 dims · fact · KPI 1–13")]
        cols = st.columns(len(steps))
        for col, (t_, s_) in zip(cols, steps):
            col.markdown(f'<div class="rb-step"><b>✓ {e(t_)}</b><span>{e(s_)}</span></div>', unsafe_allow_html=True)
        if ing.structural_issues.notna().any():
            for x in ing[ing.structural_issues.notna()].itertuples():
                st.markdown(pill(f"{x.source_file}: {x.structural_issues}", "warn"), unsafe_allow_html=True)

    with st.container(border=True):
        ui.card_header("Data quality scorecard", "Records received vs passed vs rejected per source file", tag="KPI 10")
        ui.html_table(bsc, [
            ("File", lambda r: f"<b>{e(r.source_file)}</b>", ""),
            ("Entity", lambda r: pill(r.entity, "mute"), ""),
            ("Received", lambda r: num(r.received), "num"),
            ("Passed", lambda r: num(r.passed), "num"),
            ("Rejected", lambda r: num(r.rejected), "num"),
            ("Dups", lambda r: num(r.duplicates_removed), "num"),
            ("Fixed", lambda r: num(r.passed_with_fixes), "num"),
            ("Pass rate", lambda r: bar(r.pass_rate_pct / 100, T["pos"] if r.pass_rate_pct >= 95 else (T["warn"] if r.pass_rate_pct >= 85 else T["neg"])), ""),
            ("", lambda r: f"<b>{pct(r.pass_rate_pct)}</b>", "num"),
            ("Top 3 rejection reasons", lambda r: f"<span style='color:{T['muted']};font-size:11.5px'>{e(r.top3_rejection_reasons or '—')}</span>", "")])

    c = st.columns([1, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Automated quality gates", "ERROR fails the job · WARN is reported", tag=f"{(chk.status == 'PASS').sum()}/{len(chk)} pass", tag_tone="pos")
            ui.html_table(chk, [
                ("", lambda r: pill("PASS", "pos") if r.status == "PASS" else pill("WARN" if r.severity == "WARN" else "FAIL", "warn" if r.severity == "WARN" else "neg"), ""),
                ("Check", lambda r: e(r.check_name.replace("_", " ")), ""),
                ("Entity", lambda r: f"<span style='color:{T['muted']}'>{e(r.entity)}</span>", ""),
                ("Observed", lambda r: e(r.observed), "num")], max_height=380)
    with c[1]:
        with st.container(border=True):
            ui.card_header("Rejections by reason code", "Quarantined records (nothing silently dropped)")
            rq = D.quarantine_reasons(b)
            mx = rq.records.max() if not rq.empty else 1
            ui.html_table(rq, [
                ("Reason", lambda r: f"<b>{e(r.reason_code)}</b><br><span style='color:{T['muted']};font-size:11px'>{e(r.entity)} · {e(r.action)}</span>", ""),
                ("", lambda r: bar(r.records / mx, T["neg"] if r.severity == "ERROR" else T["info"]), ""),
                ("Records", lambda r: num(r.records), "num")], max_height=380)

    tabs = st.tabs(["Fixes applied", "Quarantine explorer", "Column profile", "Rule catalogue"])
    with tabs[0]:
        fx = D.fixes(b)
        fig = go.Figure(go.Bar(x=fx.records, y=fx.entity + " · " + fx.flag, orientation="h", marker_color=T["accent"]))
        fig.update_yaxes(autorange="reversed")
        ui.chart(fig, max(260, 22 * len(fx)), legend=False)
    with tabs[1]:
        ent = st.selectbox("Entity", ["all", "transactions", "customers", "products", "branches"])
        qr = D.quarantine_rows(b, None if ent == "all" else ent)
        st.dataframe(qr, width="stretch", hide_index=True, height=380)
    with tabs[2]:
        st.dataframe(D.profile(b).drop(columns=["profiled_at"], errors="ignore"), width="stretch", hide_index=True, height=420)
    with tabs[3]:
        st.dataframe(D.rule_catalog(), width="stretch", hide_index=True, height=420)


# ====================================================================== 5 Day 1 vs Day 2
# ================================ PAGE 5 · DAY 1 vs DAY 2 ===============================================
# Everything the incremental load changed: KPI 11 reconciliation, top-5 and branch-rank deltas,
# KPI 12 KYC transitions, KPI 13 new account activation, SCD2 history, late corrections, recovered rows.
def page_day2(all_batches: pd.DataFrame):
    b = st.session_state["batch"]
    ui.page_header("Data changes", "What the incremental load changed – new records, SCD2 versions, late corrections")
    # KPI 11 only exists for an incremental batch; when DAY1 is selected, fall back to the latest incremental batch
    rec = kpi_or_empty("kpi11_load_reconciliation", b)
    if rec.empty:
        later = all_batches[(all_batches.status == "SUCCEEDED") & (all_batches.batch_seq > 1)]
        st.info("The selected batch is the baseline load – select an incremental batch (e.g. DAY2) in the top bar.")
        if later.empty:
            return
        b = later.batch_id.iloc[-1]
        rec = kpi_or_empty("kpi11_load_reconciliation", b)
        st.caption(f"Showing {b}.")
    r = rec.set_index("entity")
    g = lambda ent, col: int(r.loc[ent, col]) if ent in r.index and pd.notna(r.loc[ent, col]) else 0
    new_all = sum(g(x, "new_records") for x in r.index)
    upd = sum(g(x, "updated_scd2_new_version") + g(x, "updated_in_place") for x in r.index)
    c = st.columns(4)
    with c[0]:
        ui.tile("New records", num(new_all), pill(f"{g('transactions', 'new_records')} txns · {g('customers', 'new_records')} customers · "
                                                  f"{g('products', 'new_records')} products · {g('branches', 'new_records')} branch", "pos"), "＋", "KPI 11")
    with c[1]:
        ui.tile("Dimension changes", num(upd), pill(f"{sum(g(x, 'updated_scd2_new_version') for x in r.index)} new SCD2 versions", "info"), "⟳", "KPI 11")
    with c[2]:
        ui.tile("Late corrections", num(g("transactions", "corrected_late_arriving")), pill("updated in place · no double count", "warn"), "✎", "KPI 11")
    with c[3]:
        ui.tile("Recovered from quarantine", num(g("transactions", "recovered_from_prior_quarantine")), pill("late-arriving product P299", "acc"), "↺", "KPI 11")

    with st.container(border=True):
        ui.card_header("Load reconciliation", "Records processed in the incremental load per entity", tag="KPI 11")
        ui.html_table(rec, [("Entity", lambda x: f"<b>{e(x.entity)}</b>", ""),
                            ("Received", lambda x: num(x.records_received), "num"), ("Rejected", lambda x: num(x.records_rejected), "num"),
                            ("New", lambda x: num(x.new_records), "num"), ("SCD2 new version", lambda x: num(x.updated_scd2_new_version), "num"),
                            ("Updated in place", lambda x: num(x.updated_in_place), "num"),
                            ("Corrected", lambda x: num(x.corrected_late_arriving), "num"), ("Unchanged", lambda x: num(x.unchanged), "num"),
                            ("Recovered", lambda x: num(x.recovered_from_prior_quarantine), "num")])

    c = st.columns([1.2, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Top 5 customers, before and after the update", "KPI 1 delta vs previous run", tag="KPI 11")
            d1 = kpi_or_empty("kpi11_kpi01_delta", b, order="coalesce(curr_rank, 99)")
            tone = {"ENTERED": "pos", "DROPPED_OUT": "neg", "RANK_CHANGED": "info", "UNCHANGED": "mute"}
            ui.html_table(d1, [("Customer", lambda x: f"<b>{e(x.customer_name)}</b><br><span style='color:{T['muted']};font-size:11px'>{e(x.customer_id)}</span>", ""),
                               ("Before", lambda x: f"#{int(x.prev_rank)}" if pd.notna(x.prev_rank) else "–", "num"),
                               ("After", lambda x: f"#{int(x.curr_rank)}" if pd.notna(x.curr_rank) else "–", "num"),
                               ("Δ value", lambda x: inr(x.net_value_delta), "num"),
                               ("Movement", lambda x: pill(x.top5_movement.replace("_", " ").title(), tone.get(x.top5_movement, "mute")), "")])
    with c[1]:
        with st.container(border=True):
            ui.card_header("Branch ranking changes", "KPI 7 delta vs previous run", tag="KPI 11")
            d7 = kpi_or_empty("kpi11_kpi07_delta", b, where="has_changed AND (rank_change <> 0 OR is_new_branch OR coalesce(was_top,false) <> coalesce(is_top,false))",
                              order="abs(coalesce(rank_change,0)) DESC")
            ui.html_table(d7.head(12), [("Branch", lambda x: f"<b>{e(x.branch_id)}</b>", ""), ("Region", lambda x: e(x.region), ""),
                                        ("Before", lambda x: f"#{int(x.prev_rank)}" if pd.notna(x.prev_rank) else pill("new", "acc"), "num"),
                                        ("After", lambda x: f"#{int(x.curr_rank)}", "num"),
                                        ("Change", lambda x: delta_pill(x.rank_change, "", 0) if pd.notna(x.rank_change) else "", "num")], max_height=330)

    c = st.columns([1, 1.3])
    with c[0]:
        with st.container(border=True):
            ui.card_header("KYC status transitions", "Detected from SCD2 history", tag="KPI 12")
            k12 = kpi_or_empty("kpi12_kyc_transitions", b)
            tr = k12.groupby("transition").size().reset_index(name="n")
            mx = tr.n.max() if not tr.empty else 1
            ui.html_table(tr, [("Transition", lambda x: f"<b>{e(x.transition)}</b>", ""), ("", lambda x: bar(x.n / mx, T["pos"]), ""),
                               ("Customers", lambda x: num(x.n), "num")])
            with st.expander(f"{len(k12)} customers"):
                st.dataframe(k12[["customer_id", "customer_name", "transition", "changed_effective_from", "address_also_changed"]], hide_index=True, width="stretch")
            ui.card_header("New account activation", "First-time transacting accounts and new customers", tag="KPI 13")
            k13 = kpi_or_empty("kpi13_new_account_activation", b)
            a1, a2 = st.columns(2)
            a1.markdown(f'<div class="rb-mini"><small>First-time transacting accounts</small><b>{num((k13.get("category") == "FIRST_TIME_TRANSACTING_ACCOUNT").sum() if not k13.empty else 0)}</b></div>', unsafe_allow_html=True)
            a2.markdown(f'<div class="rb-mini"><small>Newly onboarded customers</small><b>{num((k13.get("category") == "NEWLY_ONBOARDED_CUSTOMER").sum() if not k13.empty else 0)}</b></div>', unsafe_allow_html=True)
            with st.expander("Details"):
                st.dataframe(k13.drop(columns=["as_of_batch", "as_of_batch_seq", "business_date", "computed_at"], errors="ignore"), hide_index=True, width="stretch")
    with c[1]:
        with st.container(border=True):
            ui.card_header("SCD Type 2 history", "Expired and current versions of changed records")
            which = st.segmented_control("Dimension", ["customer", "branch", "product"], default="customer", label_visibility="collapsed")
            hist = D.scd2_history(which or "customer")
            st.dataframe(hist, hide_index=True, width="stretch", height=260,
                         column_config={"is_current": st.column_config.CheckboxColumn("current")})
        with st.container(border=True):
            ui.card_header("Late corrections applied", "Re-used Day-1 transaction ids – fact updated in place, old version in history")
            cr = D.corrections()
            ui.html_table(cr, [("Transaction", lambda x: f"<b>{e(x.transaction_id)}</b>", ""), ("Account", lambda x: e(x.account_id), ""),
                               ("Amount", lambda x: inr(x.new_amount, short=False), "num"),
                               ("Before", lambda x: pill(x.old_status, TONE_STATUS.get(x.old_status, "mute")), ""),
                               ("After", lambda x: pill(x.new_status, TONE_STATUS.get(x.new_status, "mute")), ""),
                               ("Version", lambda x: f"v{int(x.version)}", "num")], max_height=300)
            rc = D.recovered()
            st.markdown(" ".join(pill(f"{x.transaction_id} · {x.resolution.replace('_', ' ').lower()}", "acc" if x.resolution.startswith("RECOVERED") else "mute")
                                 for x in rc.itertuples()), unsafe_allow_html=True)


# ====================================================================== 6 Pipeline runs
# ================================ PAGE 6 · PIPELINE RUNS ================================================
# Last run status/duration (Jobs API), medallion layer volumes, run history, batches, a "Run batch" button
# and the load-statistics log of the selected batch.
def page_pipeline(all_batches: pd.DataFrame):
    ui.page_header("Pipeline runs", "Orchestration, medallion layer volumes and run history")
    runs = pd.DataFrame()
    try:
        runs = D.job_runs()
    except Exception as ex:
        st.caption(f"Job history unavailable: {ex}")
    lc = D.layer_counts()
    last = runs.iloc[0] if not runs.empty else None
    c = st.columns(4)
    with c[0]:
        status = (last["result"] or last["state"]) if last is not None else "–"
        ui.tile("Last run", status.title() if status else "–", pill(f"batch {last['batch_id']}" if last is not None else "", "pos" if status == "SUCCESS" else "warn"), "▶", "Batch pipeline job")
    with c[1]:
        dur = last["duration_s"] if last is not None and pd.notna(last["duration_s"]) else None
        ui.tile("Duration", f"{int(dur // 60)}m {int(dur % 60)}s" if dur else "–", pill("serverless", "mute"), "⏱", "Last run")
    with c[2]:
        ui.tile("Records in gold", num(lc["gold"]), pill(f"bronze {num(lc['bronze'])} · silver {num(lc['silver'])}", "info"), "▦", "Dims + fact")
    with c[3]:
        ui.tile("Quarantined", num(lc["quarantined"]), pill("raw payload + reason codes", "neg"), "⛔", "All batches")

    with st.container(border=True):
        ui.card_header("Medallion stages", "Unity Catalog · workspace.rb_*")
        s = st.columns(4)
        for col, (name, sub, val, tone) in zip(s, [
                ("Landing", "Volume rb_landing.raw + immutable archive", "files", "mute"),
                ("Bronze", "STRING columns + ingestion metadata", num(lc["bronze"]), "warn"),
                ("Silver", "cleansed · validated · quarantine", num(lc["silver"]), "info"),
                ("Gold", "SCD2 dims · fact · KPI 1–13", num(lc["gold"]), "pos")]):
            col.markdown(f'<div class="rb-step"><b>{e(name)} {pill(val, tone)}</b><span>{e(sub)}</span></div>', unsafe_allow_html=True)

    c = st.columns([1.45, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Run history", "Databricks Workflows – RetailBank Batch Pipeline")
            if runs.empty:
                st.caption("No job runs visible to the app.")
            else:
                ui.html_table(runs, [("Run", lambda x: f'<a href="{e(x.url)}" target="_blank" style="color:{T["accent"]}">{x.run_id}</a>', ""),
                                     ("Batch", lambda x: e(x.batch_id), ""),
                                     ("Started (UTC)", lambda x: pd.Timestamp(x.started).strftime("%d %b %H:%M") if pd.notna(x.started) else "", ""),
                                     ("Duration", lambda x: f"{int(x.duration_s // 60)}m {int(x.duration_s % 60)}s" if pd.notna(x.duration_s) else "", "num"),
                                     ("Status", lambda x: pill(x.result or x.state, "pos" if x.result == "SUCCESS" else ("neg" if x.result in ("FAILED", "TIMEDOUT", "CANCELED") else "warn")), "")],
                              max_height=360)
            st.markdown("**Batches**")
            ui.html_table(all_batches, [("Batch", lambda x: f"<b>{e(x.batch_id)}</b>", ""), ("Seq", lambda x: num(x.batch_seq), "num"),
                                        ("Business date", lambda x: e(x.business_date), ""),
                                        ("Status", lambda x: pill(x.status, "pos" if x.status == "SUCCEEDED" else "warn"), ""),
                                        ("Finished (UTC)", lambda x: pd.Timestamp(x.finished_at).strftime("%d %b %H:%M") if pd.notna(x.finished_at) else "", "")])
    with c[1]:
        with st.container(border=True):
            ui.card_header("Run a batch", "Triggers the job with batch_id (idempotent – re-runs change nothing)")
            bsel = st.selectbox("Batch to run", all_batches.batch_id.tolist() or ["day1", "day2"], index=len(all_batches) - 1 if len(all_batches) else 0)
            # starts the Databricks job with batch_id = the selected batch (idempotent, so re-running is safe)
            if st.button("Run batch", type="primary", icon=":material/play_arrow:"):
                try:
                    rid = D.trigger_run(bsel)
                    st.success(f"Started run {rid} for {bsel}. It takes ~8 minutes; refresh this page to follow it.")
                    D.q.clear()
                except Exception as ex:
                    st.error(f"Could not start the job: {ex}")
            if st.button("Refresh data", icon=":material/refresh:"):
                D.q.clear()
                st.rerun()
        with st.container(border=True):
            ui.card_header("Run log", "Load statistics of the selected batch")
            ls = D.load_stats(st.session_state["batch"])
            lines = "".join(f"<div><em style='color:{T['accent']}'>{e(r.stage)}</em> {e(r.entity)} · {e(r.metric)} = <em>{num(r.value)}</em></div>"
                            for r in ls.itertuples())
            st.markdown(f'<div class="rb-log">{lines}</div>', unsafe_allow_html=True)


# ====================================================================== 7 Customers
# ================================ PAGE 7 · CUSTOMERS ====================================================
# Tiles, searchable customer directory (current SCD2 version + RFM segment), customer profile card with
# contact data, DQ fixes, possible duplicate and transactions, and the segment mix chart.
def page_customers():
    b = st.session_state["batch"]
    ui.page_header("Customers", "Segments, KYC and lifetime activity per customer")
    h = D.headline(b)
    c = st.columns(4)
    with c[0]:
        ui.tile("Customers", num(h["customers"]), pill(f"{num(h['segmented'])} with successful INR history", "mute"), "◎", "Current dimension rows")
    with c[1]:
        ui.tile("Platinum", num(h["platinum"]), pill("RFM score ≥ 8", "acc"), "★", "KPI 6")
    with c[2]:
        ui.tile("Dormant", num(h["dormant"]), pill(f"{num(h['hr_dormant'])} high-risk", "neg"), "⏸", "KPI 4")
    with c[3]:
        ui.tile("KYC exposure", pct(h["kyc_pct"]), pill("of value by non-verified", "warn"), "🛡", "KPI 8")

    # the search box in the top bar pre-fills the directory search
    dirx = D.customer_directory()
    search = (st.session_state.get("global_search") or "").strip().lower()
    c = st.columns([1.7, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Customer directory", "Current version of every customer (SCD2) with RFM segment")
            f1, f2, f3 = st.columns([2, 1, 1])
            s = f1.text_input("Search", value=search, placeholder="Name, id, account or city")
            seg = f2.multiselect("Segment", ["Platinum", "Gold", "Silver", "Bronze"])
            kyc = f3.multiselect("KYC", ["Verified", "Pending", "Rejected"])
            v = dirx
            if s:
                s_ = s.lower()
                v = v[v.apply(lambda r: s_ in f"{r.customer_id} {r.customer_name} {r.account_id} {r.city}".lower(), axis=1)]
            if seg:
                v = v[v.segment.isin(seg)]
            if kyc:
                v = v[v.kyc_status.isin(kyc)]
            ui.html_table(v.head(80), [
                ("Customer", lambda x: f"<b>{e(x.customer_name)}</b><br><span style='color:{T['muted']};font-size:11px'>{e(x.customer_id)}</span>", ""),
                ("Account", lambda x: e(x.account_id), ""), ("City", lambda x: e(x.city), ""),
                ("Segment", lambda x: pill(x.segment, TONE_SEG.get(x.segment, "mute")) if pd.notna(x.segment) else pill("No history", "mute"), ""),
                ("KYC", lambda x: pill(x.kyc_status, TONE_KYC.get(x.kyc_status, "mute")), ""),
                ("Txns", lambda x: num(x.frequency) if pd.notna(x.frequency) else "–", "num"),
                ("Value", lambda x: inr(x.monetary) if pd.notna(x.monetary) else "–", "num")], max_height=470)
            st.caption(f"{len(v):,} customers match (first 80 shown).")
    with c[1]:
        with st.container(border=True):
            ui.card_header("Customer profile", "Select a customer")
            ids = v.customer_id.tolist() or dirx.customer_id.tolist()
            cid = st.selectbox("Customer", ids, format_func=lambda x: f"{x} · {dirx.set_index('customer_id').loc[x, 'customer_name']}", label_visibility="collapsed")
            p = dirx.set_index("customer_id").loc[cid]
            st.markdown(f"""<div class="rb-top" style="margin:6px 0 12px"><div class="rb-avatar">{e(p.customer_name.split('_')[-1])}</div>
              <div class="rb-user"><b>{e(p.customer_name)}</b><span>{e(cid)} · {e(p.account_id)} · {e(p.city)}</span></div></div>""", unsafe_allow_html=True)
            m1, m2, m3 = st.columns(3)
            m1.markdown(f'<div class="rb-mini"><small>Segment</small><b>{e(p.segment if pd.notna(p.segment) else "–")}</b></div>', unsafe_allow_html=True)
            m2.markdown(f'<div class="rb-mini"><small>KYC</small><b>{e(p.kyc_status)}</b></div>', unsafe_allow_html=True)
            m3.markdown(f'<div class="rb-mini"><small>Net value</small><b>{inr(p.monetary) if pd.notna(p.monetary) else "–"}</b></div>', unsafe_allow_html=True)
            st.markdown(f"<div style='margin-top:10px;font-size:12.5px;color:{T['muted']}'>✉ {e(p.email or 'missing')} &nbsp; ☎ {e(p.phone or 'invalid')}<br>"
                        f"Registered {e(p.registration_date)} · SCD2 version v{int(p.version)}"
                        + (f"<br>{pill('Possible duplicate of ' + str(p.potential_duplicate_of), 'warn')}" if pd.notna(p.potential_duplicate_of) else "")
                        + (f"<br><span style='font-size:11px'>DQ fixes: {e(p.dq_flags)}</span>" if p.dq_flags else "") + "</div>", unsafe_allow_html=True)
            tx = D.customer_txns(cid)
            st.markdown("**Transactions**")
            ui.html_table(tx.head(15), [("Id", lambda x: e(x.transaction_id), ""),
                                        ("Date", lambda x: pd.Timestamp(x.txn_ts).strftime("%d %b %y"), ""),
                                        ("Amount", lambda x: inr(x.amount, short=False) + (" ↩" if x.is_refund else ""), "num"),
                                        ("Status", lambda x: pill(x.status, TONE_STATUS.get(x.status, "mute")), "")], max_height=260)
        with st.container(border=True):
            ui.card_header("Segment mix", "Customers per RFM segment", tag="KPI 6")
            mix = dirx.segment.fillna("No history").value_counts().reindex(["Platinum", "Gold", "Silver", "Bronze", "No history"]).fillna(0)
            fig = go.Figure(go.Bar(x=mix.values, y=mix.index, orientation="h",
                                   marker_color=[T["accent"], T["warn"], T["info"], T["muted"], T["grid"]]))
            fig.update_yaxes(autorange="reversed")
            ui.chart(fig, 200, legend=False)


# ====================================================================== 8 Reports & exports
# ================================ PAGE 8 · REPORTS & EXPORTS ============================================
# (label, KPI table) pairs that get a CSV download button.
EXPORTS = [
    ("KPI 1 · Customer net ranking", "kpi01_customer_net_ranking"), ("KPI 2 · Monthly trend", "kpi02_monthly_trend"),
    ("KPI 3 · Product revenue", "kpi03_product_revenue"), ("KPI 4 · Dormant accounts", "kpi04_dormant_accounts"),
    ("KPI 5 · Suspicious transactions", "kpi05_suspicious_transactions"), ("KPI 6 · RFM segments", "kpi06_customer_rfm_segments"),
    ("KPI 7 · Branch ranking", "kpi07_branch_ranking"), ("KPI 8 · KYC exposure", "kpi08_kyc_exposure"),
    ("KPI 9 · Refund rate", "kpi09_refund_rate"), ("KPI 10 · DQ scorecard", "kpi10_dq_scorecard"),
    ("KPI 11 · Load reconciliation", "kpi11_load_reconciliation"), ("KPI 11 · Top-5 delta", "kpi11_kpi01_delta"),
    ("KPI 11 · Branch rank delta", "kpi11_kpi07_delta"), ("KPI 12 · KYC transitions", "kpi12_kyc_transitions"),
    ("KPI 13 · New account activation", "kpi13_new_account_activation"),
]


# Download buttons: every KPI table for the selected batch + the data-quality evidence (quarantine, checks, profile, rules).
def page_reports():
    b = st.session_state["batch"]
    ui.page_header("Reports and exports", "Download KPI views, the data-quality scorecard and rejected records as CSV")
    c = st.columns([1.3, 1])
    with c[0]:
        with st.container(border=True):
            ui.card_header("Available exports", f"Snapshot as of batch {b}", tag=f"{len(EXPORTS) + 2} reports", tag_tone="acc")
            for label, tbl in EXPORTS:
                a, z = st.columns([3, 1], vertical_alignment="center")
                a.markdown(f"<b>{e(label)}</b><br><span style='color:{T['muted']};font-size:11.5px'>workspace.rb_gold.{tbl}</span>", unsafe_allow_html=True)
                with z:
                    df = kpi_or_empty(tbl, b)
                    st.download_button("CSV", df.to_csv(index=False).encode(), f"{tbl}_{b}.csv", "text/csv", key=f"dl_{tbl}",
                                       disabled=df.empty, icon=":material/download:")
    with c[1]:
        with st.container(border=True):
            ui.card_header("Data-quality exports", "Audit evidence")
            q_ = D.quarantine_rows(b, None, limit=100000)
            st.download_button("Rejected rows (quarantine)", q_.to_csv(index=False).encode(), f"quarantine_{b}.csv", "text/csv",
                               type="primary", icon=":material/download:", width="stretch")
            st.download_button("DQ check results", D.dq_checks(b).to_csv(index=False).encode(), f"dq_checks_{b}.csv", "text/csv",
                               icon=":material/download:", width="stretch")
            st.download_button("Column profile", D.profile(b).to_csv(index=False).encode(), f"profile_{b}.csv", "text/csv",
                               icon=":material/download:", width="stretch")
            st.download_button("Rule catalogue", D.rule_catalog().to_csv(index=False).encode(), "dq_rule_catalog.csv", "text/csv",
                               icon=":material/download:", width="stretch")
        with st.container(border=True):
            ui.card_header("Where the data lives", "Unity Catalog")
            st.markdown(f"""<div style="font-size:12.5px;line-height:1.9">
              {pill('Landing', 'mute')} workspace.rb_landing.raw (Volume)<br>
              {pill('Bronze', 'warn')} workspace.rb_bronze.*<br>
              {pill('Silver', 'info')} workspace.rb_silver.* + quarantine<br>
              {pill('Gold', 'pos')} workspace.rb_gold.dim_* · fact_transaction · kpi01…13<br>
              {pill('Audit', 'mute')} workspace.rb_audit.*</div>""", unsafe_allow_html=True)


# ====================================================================== settings
# Settings: switch theme, show which catalog / warehouse / job the app is connected to, clear the data cache.
def page_settings():
    ui.page_header("Settings", "Appearance and connection")
    with st.container(border=True):
        ui.card_header("Appearance", "Dark and light themes from the design")
        mode = st.segmented_control("Theme", ["dark", "light"], default=st.session_state["theme"], format_func=str.title)
        if mode and mode != st.session_state["theme"]:
            st.session_state["theme"] = mode
            st.rerun()
    with st.container(border=True):
        import os
        ui.card_header("Connection", "Databricks App resources")
        st.markdown(f"""<div style="font-size:12.5px;line-height:1.9">Catalog: <b>{e(D.CATALOG)}</b><br>
            SQL warehouse: <b>{e(os.getenv('DATABRICKS_WAREHOUSE_ID', '–'))}</b><br>
            Pipeline job: <b>{e(os.getenv('RB_JOB_ID', '–'))}</b><br>Signed in as: <b>{e(current_user())}</b></div>""", unsafe_allow_html=True)
        if st.button("Clear cache & reload", icon=":material/refresh:"):
            D.q.clear()
            st.rerun()


# ====================================================================== navigation
# ================================ NAVIGATION ============================================================
# RB_DEFAULT_PAGE is only used by automated tests to open a specific page first.
import os as _os
_DEFAULT = _os.getenv("RB_DEFAULT_PAGE", "overview")
# Load the batches once per run (needed by the top bar and by pages 5 and 6).
ALL_BATCHES, OPTS = batch_selector()
# Register every page: function to run, title + Material icon shown in the sidebar, URL path.
PAGES = {
    "overview": st.Page(page_overview, title="Overview", icon=":material/space_dashboard:", url_path="overview", default=_DEFAULT == "overview"),
    "transactions": st.Page(page_transactions, title="Transactions & Branches", icon=":material/account_balance:", url_path="transactions", default=_DEFAULT == "transactions"),
    "risk": st.Page(page_risk, title="Risk & Customers", icon=":material/shield:", url_path="risk", default=_DEFAULT == "risk"),
    "dq": st.Page(page_dq, title="Data Quality", icon=":material/fact_check:", url_path="data-quality", default=_DEFAULT == "dq"),
    "day2": st.Page(lambda: page_day2(ALL_BATCHES), title="Day 1 vs Day 2", icon=":material/compare_arrows:", url_path="day2", default=_DEFAULT == "day2"),
    "pipeline": st.Page(lambda: page_pipeline(ALL_BATCHES), title="Pipeline runs", icon=":material/account_tree:", url_path="pipeline", default=_DEFAULT == "pipeline"),
    "customers": st.Page(page_customers, title="Customers", icon=":material/group:", url_path="customers", default=_DEFAULT == "customers"),
    "reports": st.Page(page_reports, title="Reports & exports", icon=":material/description:", url_path="reports", default=_DEFAULT == "reports"),
    "settings": st.Page(page_settings, title="Settings", icon=":material/settings:", url_path="settings", default=_DEFAULT == "settings"),
}
with st.sidebar:
    st.markdown(f'<div class="rb-top" style="margin:4px 0 8px"><div class="rb-avatar">RB</div><div class="rb-user"><b>RetailBank</b>'
                f'<span>KPI dashboard</span></div></div>', unsafe_allow_html=True)
# st.navigation draws the page list in the sidebar and returns the page the user selected.
nav = st.navigation(list(PAGES.values()), position="sidebar")
with st.sidebar:
    st.markdown("---")
    dark = st.toggle("Dark mode", value=st.session_state["theme"] == "dark")
    if dark != (st.session_state["theme"] == "dark"):
        st.session_state["theme"] = "dark" if dark else "light"
        st.rerun()
    st.caption("Data: workspace.rb_gold · refreshed every 5 min")
# Draw the top bar, then run the selected page function.
top_bar(OPTS)
nav.run()
