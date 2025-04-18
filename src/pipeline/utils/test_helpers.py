from pathlib import Path
from jinja2 import Environment, FileSystemLoader

def compile_model(model_path: Path) -> str:
    """
    Render a dbt-style SQL model (with Jinja) into plain SQL.
    - `model_path` is the full path to your .sql file under sb_pipeline/models.
    """
    # 1. Locate the root of your dbt models folder:
    models_root = model_path.parents[1]  # e.g. .../sb_pipeline/models

    # 2. Create a Jinja environment pointing there:
    env = Environment(
        loader=FileSystemLoader(str(models_root)),
        keep_trailing_newline=True,
    )

    # 3) Stub out dbt's `ref()`, `var()`, and `config()` so they render no‐ops
    env.globals['ref'] = lambda identifier: identifier
    env.globals['var'] = lambda name, default=None: default or ""
    env.globals['config'] = lambda *args, **kwargs: ""

    # 4. Load & render
    #    Use forward‑slash paths so Jinja can find it, even on Windows
    rel = model_path.relative_to(models_root).as_posix()
    template = env.get_template(rel)
    return template.render()