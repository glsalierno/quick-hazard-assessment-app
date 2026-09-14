"""
Generate assets/hazard_app.ico — warning triangle + chart motif (hazard analysis).
Run once: python scripts/generate_hazard_app_icon.py
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw


def draw_icon(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    s = float(size)
    margin = s * 0.06
    # Rounded dark slate background (tool / dashboard feel)
    pad = int(margin)
    draw.rounded_rectangle(
        [pad, pad, int(s - pad), int(s - pad)],
        radius=int(s * 0.12),
        fill=(30, 41, 59, 255),
        outline=(71, 85, 105, 255),
        width=max(1, int(s / 48)),
    )
    # Warning triangle (amber)
    cx, cy = s / 2, s * 0.42
    tri_h = s * 0.38
    tri_w = tri_h * 0.9
    top = (cx, cy - tri_h * 0.45)
    bl = (cx - tri_w / 2, cy + tri_h * 0.55)
    br = (cx + tri_w / 2, cy + tri_h * 0.55)
    draw.polygon([top, bl, br], fill=(251, 191, 36, 255), outline=(120, 53, 15, 255), width=max(1, int(s / 32)))
    # Exclamation stem + dot
    ew = max(2, int(s * 0.045))
    ex = cx - ew / 2
    ey1 = cy - tri_h * 0.15
    ey2 = cy + tri_h * 0.12
    draw.rectangle([ex, ey1, ex + ew, ey2], fill=(30, 27, 15, 255))
    dr = max(2, int(s * 0.035))
    draw.ellipse([cx - dr, ey2 + dr * 0.8, cx + dr, ey2 + dr * 2.6], fill=(30, 27, 15, 255))
    # Mini "analysis" bars (green = low hazard signal)
    bx0 = s * 0.22
    by0 = s * 0.72
    bw = s * 0.09
    gap = s * 0.04
    heights = [0.12, 0.22, 0.16, 0.28]
    colors = [(34, 197, 94, 255), (16, 185, 129, 255), (52, 211, 153, 255), (59, 130, 246, 255)]
    for i, (h, col) in enumerate(zip(heights, colors)):
        x = bx0 + i * (bw + gap)
        bh = s * h
        draw.rectangle(
            [x, by0 - bh, x + bw, by0],
            fill=col,
            outline=(15, 23, 42, 200),
            width=1,
        )
    return img


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    out_dir = root / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "hazard_app.ico"
    sizes = [256, 128, 64, 48, 32, 16]
    images = [draw_icon(sz) for sz in sizes]
    images[0].save(
        out_path,
        format="ICO",
        sizes=[(sz, sz) for sz in sizes],
        append_images=images[1:],
    )
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
