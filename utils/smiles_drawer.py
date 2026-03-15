"""
Render 2D molecular structure from SMILES using smiles-drawer (client-side JS).
Escapes SMILES for safe embedding in HTML/JS.
"""

from __future__ import annotations

import json
import streamlit as st


def _escape_smiles_for_js(smiles: str) -> str:
    """Escape backslashes and quotes for use inside JavaScript string."""
    if not smiles:
        return ""
    # Backslash must be first
    s = smiles.replace("\\", "\\\\")
    s = s.replace("'", "\\'")
    s = s.replace('"', '\\"')
    s = s.replace("\r", "\\r").replace("\n", "\\n")
    return s


def draw_smiles(smiles_string: str | None, width: int = 400, height: int = 300) -> None:
    """
    Render a molecule from SMILES using smiles-drawer in an iframe.
    If smiles_string is None or empty, does nothing.
    """
    if not smiles_string or not smiles_string.strip():
        return
    escaped = _escape_smiles_for_js(smiles_string.strip())
    # Use double quotes in JS and escape SMILES for JSON-style safety
    safe_smiles = json.dumps(smiles_string.strip())
    html_code = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <script src="https://unpkg.com/smiles-drawer@2.0.2/dist/smiles-drawer.min.js"></script>
    <style>
        body {{ margin: 0; padding: 0; display: flex; justify-content: center; align-items: center; min-height: 100%; }}
        #drawing {{ width: {width}px; height: {height}px; }}
    </style>
</head>
<body>
    <canvas id="drawing" width="{width}" height="{height}"></canvas>
    <script>
        (function() {{
            var smiles = {safe_smiles};
            var drawer = new SmilesDrawer.Drawer({{ width: {width}, height: {height}, bondThickness: 0.6, atomVisualization: 'balls', isomeric: true }});
            SmilesDrawer.parse(smiles, function(tree) {{
                if (tree) drawer.draw(tree, 'drawing', 'light', false);
            }}, function(err) {{ console.error(err); }});
        }})();
    </script>
</body>
</html>
"""
    st.components.v1.html(html_code, height=height + 20)
