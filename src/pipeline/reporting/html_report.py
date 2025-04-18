import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import plotly.graph_objects as go
from pipeline.utils.duck_helpers import fetch_pnl_daily

def generate_html_report(lake_root: str, run_id: str, output_dir: str) -> str:
    # 1) Query Gold table via DuckDB or Spark
    # (Assume you already have helper to return a Pandas df or list of dicts)
    rows = fetch_pnl_daily(lake_root)

    # 2) Build Plotly figure
    desks = [r['desk_id'] for r in rows]
    pnls  = [r['real_pnl_usd'] for r in rows]
    fig = go.Figure([go.Bar(x=desks, y=pnls)])
    fig.update_layout(
      title="Real P&L by Desk",
      xaxis_title="Desk",
      yaxis_title="Real P&L (USD)",
      margin=dict(l=40, r=40, t=40, b=40)
    )

    # 3) Serialize Plotly JSON
    plot_json = fig.to_plotly_json()
    plot_data   = json.dumps(plot_json['data'])
    plot_layout = json.dumps(plot_json['layout'])

    # 4) Render Jinja template
    tpl_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(tpl_dir)))
    tpl = env.get_template("desk_pnl.html.j2")
    html = tpl.render(
      run_id           = run_id,
      rows             = rows,
      plotly_json_data = plot_data,
      plotly_json_layout = plot_layout,
    )

    # 5) Save to output_dir
    out_file = Path(output_dir) / f"desk_pnl_{run_id}.html"
    out_file.write_text(html, encoding="utf-8")
    return str(out_file)
