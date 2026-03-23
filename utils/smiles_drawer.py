"""
RDKit-based molecular drawing for canonical, publication-quality structures.
Falls back to smiles-drawer (client-side JS) if RDKit is unavailable or rendering fails.
"""

from __future__ import annotations

import io
import json
from typing import Optional

import streamlit as st

# Optional RDKit import
try:
    from rdkit import Chem
    from rdkit.Chem import Draw, rdDepictor
    _RDKIT_AVAILABLE = True
except ImportError:
    _RDKIT_AVAILABLE = False

try:
    from PIL import Image
except ImportError:
    Image = None

# Prefer CoordGen for better 2D layout when available
if _RDKIT_AVAILABLE:
    try:
        rdDepictor.SetPreferCoordGen(True)
    except Exception:
        pass


def _escape_smiles_for_js(smiles: str) -> str:
    """Escape backslashes and quotes for use inside JavaScript string."""
    if not smiles:
        return ""
    s = smiles.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
    s = s.replace("\r", "\\r").replace("\n", "\\n")
    return s


def draw_molecule_canonical(
    smiles: str,
    width: int = 500,
    height: int = 300,
    style: str = "acs_1996",
    highlight_atoms: Optional[list[int]] = None,
    highlight_bonds: Optional[list[int]] = None,
    explicit_hydrogens: bool = False,
    atom_labels: bool = False,
) -> Optional["Image.Image"]:
    """
    Generate a canonical molecular drawing using RDKit.

    Parameters:
    -----------
    smiles : str
        SMILES string of the molecule
    width, height : int
        Image dimensions in pixels
    style : str
        Drawing style: "acs_1996", "acs_2006", "nature", "simple"
    highlight_atoms : list
        List of atom indices to highlight
    highlight_bonds : list
        List of bond indices to highlight
    explicit_hydrogens : bool
        Whether to show explicit H atoms
    atom_labels : bool
        Whether to show atom indices (for debugging)

    Returns:
    --------
    PIL Image or None if failed
    """
    if not _RDKIT_AVAILABLE or Image is None:
        return None
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None
        if explicit_hydrogens:
            mol = Chem.AddHs(mol)
        rdDepictor.Compute2DCoords(mol)
        # Use high-level MolToImage for compatibility (no Cairo required)
        size = (width, height)
        if highlight_atoms or highlight_bonds:
            img = Draw.MolToImage(
                mol,
                size=size,
                highlightAtoms=highlight_atoms or [],
                highlightBonds=highlight_bonds or [],
            )
        else:
            img = Draw.MolToImage(mol, size=size)
        return img
    except Exception as e:
        if st is not None:
            st.error(f"RDKit drawing error: {str(e)}")
        return None


def draw_molecule_with_stereo(smiles: str, width: int = 500, height: int = 300) -> Optional["Image.Image"]:
    """Emphasize stereochemistry with wedges/dashes."""
    if not _RDKIT_AVAILABLE or Image is None:
        return None
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None
        Chem.AssignStereochemistry(mol, force=True, cleanIt=True)
        rdDepictor.Compute2DCoords(mol)
        return Draw.MolToImage(mol, size=(width, height))
    except Exception as e:
        if st is not None:
            st.error(f"RDKit stereo drawing error: {str(e)}")
        return None


def draw_smiles(
    smiles_string: str | None,
    width: int = 500,
    height: int = 300,
    style: str = "acs_1996",
    explicit_hydrogens: bool = False,
):
    """
    Main entry: draw molecule with RDKit (canonical style) or fall back to JS drawer.
    Returns PIL Image when RDKit succeeds (caller should st.image); returns None when fallback is used (fallback renders via HTML).
    """
    if not smiles_string or not smiles_string.strip():
        return None
    smiles_string = smiles_string.strip()
    img = draw_molecule_canonical(
        smiles_string,
        width=width,
        height=height,
        style=style,
        explicit_hydrogens=explicit_hydrogens,
    )
    if img is not None:
        return img
    draw_smiles_fallback(smiles_string, width=width, height=height)
    return None


def draw_smiles_fallback(smiles_string: str | None, width: int = 400, height: int = 300) -> None:
    """Fallback to smiles-drawer (client-side JS) when RDKit is unavailable or fails."""
    if not smiles_string or not smiles_string.strip():
        return
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
