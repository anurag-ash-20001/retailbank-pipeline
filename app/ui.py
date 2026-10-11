"""Theme + HTML components reproducing the RetailBank KPI Dashboard Figma design (dark / light glass)."""
import html
import math

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

THEMES = {
    "dark": dict(
        bg="#05110c", bg2="#0a1f17", glow1="rgba(16,185,129,.28)", glow2="rgba(5,150,105,.18)",
        card="rgba(18,40,31,.62)", card2="rgba(24,52,40,.72)", border="rgba(110,231,183,.14)", border2="rgba(110,231,183,.28)",
        text="#ecfdf5", muted="#8fb5a5", faint="#5f8676", accent="#34d399", accent2="#10b981", accent_ink="#022c22",
        pos="#4ade80", pos_bg="rgba(74,222,128,.14)", neg="#f87171", neg_bg="rgba(248,113,113,.14)",
        warn="#facc15", warn_bg="rgba(250,204,21,.14)", info="#60a5fa", info_bg="rgba(96,165,250,.14)",
        grid="rgba(143,181,165,.12)", series=["#34d399", "#facc15", "#60a5fa", "#f472b6", "#a78bfa", "#fb923c"],
        heat=["#0f2a20", "#14532d", "#a16207", "#b91c1c"],
    ),
    "light": dict(
        bg="#f3faf6", bg2="#e6f5ed", glow1="rgba(16,185,129,.20)", glow2="rgba(52,211,153,.14)",
        card="rgba(255,255,255,.78)", card2="rgba(255,255,255,.92)", border="rgba(6,95,70,.10)", border2="rgba(6,95,70,.22)",
        text="#052e22", muted="#4b6b5e", faint="#7c998d", accent="#059669", accent2="#047857", accent_ink="#ffffff",
        pos="#15803d", pos_bg="rgba(22,163,74,.12)", neg="#b91c1c", neg_bg="rgba(220,38,38,.10)",
        warn="#a16207", warn_bg="rgba(234,179,8,.16)", info="#1d4ed8", info_bg="rgba(59,130,246,.12)",
        grid="rgba(6,95,70,.10)", series=["#059669", "#ca8a04", "#2563eb", "#db2777", "#7c3aed", "#ea580c"],
        heat=["#dcfce7", "#86efac", "#fde68a", "#fca5a5"],
    ),
}


def theme() -> dict:
    return THEMES[st.session_state.get("theme", "dark")]


def inject_css():
    t = theme()
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
:root {{ --rb-text:{t['text']}; }}
html, body, [class*="css"], .stApp, .stMarkdown, button, input, textarea, select {{ font-family: 'Inter', system-ui, sans-serif !important; }}
.stApp {{
  background:
    radial-gradient(900px 520px at 12% -8%, {t['glow1']}, transparent 60%),
    radial-gradient(800px 600px at 92% 18%, {t['glow2']}, transparent 62%),
    radial-gradient(700px 500px at 40% 110%, {t['glow2']}, transparent 60%),
    linear-gradient(180deg, {t['bg']}, {t['bg2']});
  background-attachment: fixed; color: {t['text']};
}}
header[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1480px; }}
h1, h2, h3, h4, p, span, label, li, div {{ color: {t['text']}; }}
[data-testid="stCaptionContainer"], .stCaption, small {{ color: {t['muted']} !important; }}

/* sidebar = glass rail */
section[data-testid="stSidebar"] {{ background: {t['card']}; backdrop-filter: blur(18px); border-right: 1px solid {t['border']}; }}
section[data-testid="stSidebar"] * {{ color: {t['text']}; }}
[data-testid="stSidebarNav"] a, [data-testid="stSidebarNavLink"] {{ border-radius: 12px; }}
[data-testid="stSidebarNavLink"][aria-current="page"], [data-testid="stSidebarNav"] a[aria-current="page"] {{
  background: linear-gradient(90deg, {t['accent2']}, {t['accent']}); }}
[data-testid="stSidebarNavLink"][aria-current="page"] * {{ color: {t['accent_ink']} !important; }}

/* bordered containers = glass cards */
[data-testid="stVerticalBlockBorderWrapper"], div[data-testid="stVerticalBlock"] > div > div[data-testid="stVerticalBlockBorderWrapper"] {{
  background: {t['card']}; border: 1px solid {t['border']} !important; border-radius: 18px !important;
  backdrop-filter: blur(16px); box-shadow: 0 10px 30px rgba(0,0,0,.18), inset 0 1px 0 rgba(255,255,255,.04);
}}

/* widgets */
.stButton > button, .stDownloadButton > button, .stLinkButton > a {{
  border-radius: 999px !important; border: 1px solid {t['border2']} !important; background: {t['card2']} !important;
  color: {t['text']} !important; font-weight: 600; padding: .35rem 1rem;
}}
.stButton > button[kind="primary"], .stDownloadButton > button[kind="primary"] {{
  background: linear-gradient(90deg, {t['accent2']}, {t['accent']}) !important; color: {t['accent_ink']} !important; border: 0 !important;
}}
.stButton > button[kind="primary"] *, .stDownloadButton > button[kind="primary"] * {{ color: {t['accent_ink']} !important; }}
div[data-baseweb="select"] > div, .stTextInput input, div[data-baseweb="input"] {{
  background: {t['card2']} !important; border-radius: 12px !important; border-color: {t['border2']} !important; color: {t['text']} !important;
}}
div[data-baseweb="popover"] *, ul[role="listbox"] * {{ color: #052e22; }}
.stTabs [data-baseweb="tab-list"] {{ gap: 6px; background: transparent; }}
.stTabs [data-baseweb="tab"] {{ border-radius: 999px; padding: 6px 16px !important; height:auto; background: {t['card2']}; border: 1px solid {t['border']}; }}
.stTabs [aria-selected="true"] {{ background: linear-gradient(90deg, {t['accent2']}, {t['accent']}); }}
.stTabs [aria-selected="true"] p {{ color: {t['accent_ink']} !important; }}
.stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] {{ display: none; }}
[data-testid="stDataFrame"] {{ border-radius: 12px; overflow: hidden; }}
[data-testid="stExpander"] details {{ background: {t['card']}; border: 1px solid {t['border']}; border-radius: 14px; }}

/* custom components */
.rb-top {{ display:flex; align-items:center; gap:14px; }}
.rb-avatar {{ width:38px; height:38px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:700;
  background: linear-gradient(135deg, {t['accent']}, {t['accent2']}); color:{t['accent_ink']}; font-size:13px; flex:0 0 auto; }}
.rb-user b {{ display:block; font-size:14px; }} .rb-user span {{ font-size:12px; color:{t['muted']}; }}
.rb-chip {{ display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:999px; font-size:11.5px; font-weight:600;
  background:{t['warn_bg']}; color:{t['warn']}; border:1px solid {t['border']}; }}
.rb-h1 {{ font-size:30px; font-weight:700; margin:6px 0 2px; letter-spacing:-.01em; }}
.rb-sub {{ color:{t['muted']}; font-size:13.5px; margin-bottom:14px; }}
.rb-tile {{ background:{t['card']}; border:1px solid {t['border']}; border-radius:18px; padding:16px 18px; backdrop-filter: blur(16px);
  box-shadow: 0 10px 30px rgba(0,0,0,.16); min-height:132px; position:relative; overflow:hidden; margin-bottom:14px; }}
.rb-tile .lbl {{ display:flex; align-items:center; gap:10px; font-size:13px; font-weight:600; }}
.rb-tile .lbl small {{ display:block; font-weight:400; color:{t['muted']}; font-size:11.5px; }}
.rb-ico {{ width:34px; height:34px; border-radius:11px; display:flex; align-items:center; justify-content:center;
  background:{t['card2']}; border:1px solid {t['border2']}; font-size:16px; }}
.rb-tile .val {{ font-size:28px; font-weight:700; margin-top:12px; letter-spacing:-.02em; white-space:nowrap; }}
.rb-tile .tag {{ position:absolute; top:14px; right:14px; }}
.rb-valrow {{ display:flex; align-items:flex-end; justify-content:space-between; gap:8px; }}
.rb-tile svg.spark {{ flex:0 1 auto; min-width:0; max-width:45%; margin-bottom:6px; }}
.rb-pill {{ display:inline-block; padding:2px 9px; border-radius:999px; font-size:11px; font-weight:600; white-space:nowrap; }}
.rb-pos {{ background:{t['pos_bg']}; color:{t['pos']}; }} .rb-neg {{ background:{t['neg_bg']}; color:{t['neg']}; }}
.rb-warn {{ background:{t['warn_bg']}; color:{t['warn']}; }} .rb-info {{ background:{t['info_bg']}; color:{t['info']}; }}
.rb-mute {{ background:{t['card2']}; color:{t['muted']}; border:1px solid {t['border']}; }}
.rb-acc {{ background:linear-gradient(90deg,{t['accent2']},{t['accent']}); color:{t['accent_ink']}; }}
.rb-card-h {{ display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:10px; }}
.rb-card-h b {{ font-size:15px; }} .rb-card-h small {{ display:block; color:{t['muted']}; font-size:12px; margin-top:2px; }}
table.rb {{ width:100%; border-collapse:collapse; border-spacing:0; font-size:12.5px; border:none !important; margin:0; display:table; }}
table.rb tr, table.rb thead, table.rb tbody {{ border:none !important; background:transparent !important; }}
table.rb th {{ border:none !important; background:transparent !important; text-align:left; color:{t['muted']}; font-weight:500; padding:7px 8px; border-bottom:1px solid {t['border']}; font-size:11.5px; }}
table.rb td {{ border:none !important; border-bottom:1px solid {t['border']} !important; padding:8px 8px; color:{t['text']}; vertical-align:middle; }}
table.rb tr:last-child td {{ border-bottom:0 !important; }}
table.rb th {{ border-bottom:1px solid {t['border']} !important; }}
table.rb td.num, table.rb th.num {{ text-align:right; font-variant-numeric: tabular-nums; white-space:nowrap; }}
.rb-bar {{ height:7px; border-radius:99px; background:{t['grid']}; overflow:hidden; min-width:60px; }}
.rb-bar > i {{ display:block; height:100%; border-radius:99px; }}
.rb-row {{ display:flex; align-items:center; gap:10px; padding:7px 0; border-bottom:1px solid {t['border']}; font-size:13px; }}
.rb-row:last-child {{ border-bottom:0; }}
.rb-step {{ background:{t['card2']}; border:1px solid {t['border']}; border-radius:14px; padding:10px 12px; font-size:12px; }}
.rb-step b {{ display:block; font-size:13px; }} .rb-step span {{ color:{t['muted']}; }}
.rb-mini {{ background:{t['card2']}; border:1px solid {t['border']}; border-radius:14px; padding:10px 12px; }}
.rb-mini small {{ color:{t['muted']}; font-size:11px; display:block; }} .rb-mini b {{ font-size:17px; }}
.rb-log {{ font-family: ui-monospace, Consolas, monospace; font-size:11.5px; background:rgba(0,0,0,.28); border-radius:12px; padding:10px 12px;
  border:1px solid {t['border']}; max-height:300px; overflow:auto; }}
.rb-log div {{ color:{t['muted']}; padding:2px 0; }} .rb-log em {{ font-style:normal; font-weight:700; }}
</style>""", unsafe_allow_html=True)


# ------------------------------------------------------------------ formatting
def inr(v, short=True) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "–"
    v = float(v)
    sign = "-" if v < 0 else ""
    a = abs(v)
    if short and a >= 1e7:
        return f"{sign}₹{a / 1e7:.2f} Cr"
    if short and a >= 1e5:
        return f"{sign}₹{a / 1e5:.2f} L"
    s = f"{a:,.0f}"
    # Indian digit grouping
    n = s.replace(",", "")
    if len(n) > 3:
        head, tail = n[:-3], n[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        s = ",".join(parts) + "," + tail
    return f"{sign}₹{s}"


def num(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "–"
    return f"{int(v):,}"


def pct(v, digits=1) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "–"
    return f"{float(v):.{digits}f}%"


def e(x) -> str:
    return html.escape("" if x is None or (isinstance(x, float) and math.isnan(x)) else str(x))


# ------------------------------------------------------------------ components
def pill(text, tone="mute") -> str:
    return f'<span class="rb-pill rb-{tone}">{e(text)}</span>'


def delta_pill(v, suffix="%", digits=1, invert=False) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return pill("–")
    good = (v >= 0) ^ invert
    arrow = "▲" if v >= 0 else "▼"
    return pill(f"{arrow} {abs(v):.{digits}f}{suffix}", "pos" if good else "neg")


def bar(frac, color=None) -> str:
    t = theme()
    frac = 0 if frac is None or (isinstance(frac, float) and math.isnan(frac)) else max(0.0, min(1.0, float(frac)))
    return f'<div class="rb-bar"><i style="width:{frac * 100:.1f}%;background:{color or t["accent"]}"></i></div>'


def sparkline(values, color=None, w=84, h=30) -> str:
    vals = [float(v) for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1
    pts = " ".join(f"{i * w / (len(vals) - 1):.1f},{h - 3 - (v - lo) / rng * (h - 6):.1f}" for i, v in enumerate(vals))
    c = color or theme()["accent"]
    return f'<svg class="spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}"><polyline fill="none" stroke="{c}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" points="{pts}"/></svg>'


def tile(label, value, sub="", icon="◆", hint="", tag="", tag_tone="mute", spark=None):
    sp = sparkline(spark) if spark is not None else ""
    tg = f'<div class="tag">{pill(tag, tag_tone)}</div>' if tag else ""
    st.markdown(f"""<div class="rb-tile">{tg}
      <div class="lbl"><div class="rb-ico">{icon}</div><div>{e(label)}<small>{e(hint)}</small></div></div>
      <div class="rb-valrow"><div class="val">{e(value)}</div>{sp}</div><div style="margin-top:6px">{sub}</div></div>""", unsafe_allow_html=True)


def card_header(title, subtitle="", tag="", tag_tone="mute"):
    tg = pill(tag, tag_tone) if tag else ""
    st.markdown(f'<div class="rb-card-h"><div><b>{e(title)}</b><small>{e(subtitle)}</small></div>{tg}</div>', unsafe_allow_html=True)


def page_header(title, subtitle):
    st.markdown(f'<div class="rb-h1">{e(title)}</div><div class="rb-sub">{e(subtitle)}</div>', unsafe_allow_html=True)


def html_table(df: pd.DataFrame, cols: list, max_height: int | None = None):
    """cols: list of (header, renderer(row)->html, align) ; align 'num' right-aligns."""
    head = "".join(f'<th class="{a or ""}">{e(h)}</th>' for h, _, a in cols)
    body = "".join("<tr>" + "".join(f'<td class="{a or ""}">{fn(r)}</td>' for _, fn, a in cols) + "</tr>" for _, r in df.iterrows())
    style = f'style="max-height:{max_height}px;overflow:auto"' if max_height else ""
    st.markdown(f'<div {style}><table class="rb"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>', unsafe_allow_html=True)


def fig_layout(fig: go.Figure, height=280, legend=True) -> go.Figure:
    t = theme()
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=8), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=t["muted"], size=11.5), colorway=t["series"],
        showlegend=legend, legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color=t["text"])),
        hoverlabel=dict(bgcolor=t["bg2"], font_color=t["text"], bordercolor=t["border2"]),
    )
    fig.update_xaxes(gridcolor=t["grid"], zeroline=False, linecolor=t["grid"])
    fig.update_yaxes(gridcolor=t["grid"], zeroline=False, linecolor=t["grid"])
    return fig


def chart(fig, height=280, legend=True):
    st.plotly_chart(fig_layout(fig, height, legend), width="stretch", config={"displayModeBar": False})
