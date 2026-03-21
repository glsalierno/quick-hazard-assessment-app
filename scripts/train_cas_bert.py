#!/usr/bin/env python3
"""
Train DistilBERT token classifier for CAS spans.

Inference pipeline (utils/cas_extractor.py) has **no regex**; this script may use
string search when building labels from ``cas_substring`` fields in JSON.

Dataset JSON (list of objects):
  - ``text``: full cell or line
  - ``cas_substring``: exact CAS as it appears in ``text`` (e.g. \"67-64-1\")
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import torch
from torch.optim import AdamW
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm
from transformers import (
    DistilBertForTokenClassification,
    DistilBertTokenizerFast,
    get_linear_schedule_with_warmup,
)

_LABEL_O = 0
_LABEL_CAS = 1


def _find_cas_span(text: str, cas_substring: str) -> tuple[int, int] | None:
    """First occurrence of cas_substring in text (no regex)."""
    if not text or not cas_substring:
        return None
    i = text.find(cas_substring)
    if i < 0:
        return None
    return i, i + len(cas_substring)


class CASTokenDataset(Dataset):
    def __init__(self, items: list[dict], tokenizer: DistilBertTokenizerFast, max_length: int = 128):
        self.items = items
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        item = self.items[idx]
        text = str(item["text"])
        enc = self.tokenizer(
            text,
            truncation=True,
            padding="max_length",
            max_length=self.max_length,
            return_offsets_mapping=True,
            return_tensors="pt",
        )
        input_ids = enc["input_ids"].squeeze(0)
        attn = enc["attention_mask"].squeeze(0)
        offsets = enc["offset_mapping"].squeeze(0).tolist()

        labels = torch.full((self.max_length,), -100, dtype=torch.long)
        span = None
        if item.get("cas_substring"):
            span = _find_cas_span(text, str(item["cas_substring"]))
        if span is not None:
            c0, c1 = span
            for ti, off in enumerate(offsets):
                if ti >= self.max_length:
                    break
                if off[0] is None or off[1] is None:
                    continue
                s, e = int(off[0]), int(off[1])
                if s >= e:
                    continue
                if e <= c0 or s >= c1:
                    lab = _LABEL_O
                else:
                    lab = _LABEL_CAS
                labels[ti] = lab
        else:
            for ti, off in enumerate(offsets):
                if ti >= self.max_length:
                    break
                if attn[ti].item() == 0:
                    continue
                labels[ti] = _LABEL_O

        for ti in range(self.max_length):
            if attn[ti].item() == 0:
                labels[ti] = -100
                continue
            off = offsets[ti]
            if off[0] is None or off[1] is None:
                labels[ti] = -100
                continue
            if int(off[0]) == 0 and int(off[1]) == 0:
                labels[ti] = -100

        return {"input_ids": input_ids, "attention_mask": attn, "labels": labels}


def train(
    data_path: Path,
    out_dir: Path,
    epochs: int = 3,
    batch_size: int = 16,
    lr: float = 2e-5,
    max_length: int = 128,
) -> None:
    with open(data_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        raise SystemExit("Dataset must be a JSON list")

    tokenizer = DistilBertTokenizerFast.from_pretrained("distilbert-base-uncased")
    model = DistilBertForTokenClassification.from_pretrained("distilbert-base-uncased", num_labels=2)

    ds = CASTokenDataset(raw, tokenizer, max_length=max_length)
    loader = DataLoader(ds, batch_size=batch_size, shuffle=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    steps = len(loader) * epochs
    optimizer = AdamW(model.parameters(), lr=lr)
    sched = get_linear_schedule_with_warmup(optimizer, int(0.06 * steps), steps)

    model.train()
    for epoch in range(epochs):
        pbar = tqdm(loader, desc=f"epoch {epoch + 1}/{epochs}")
        for batch in pbar:
            optimizer.zero_grad(set_to_none=True)
            out = model(
                input_ids=batch["input_ids"].to(device),
                attention_mask=batch["attention_mask"].to(device),
                labels=batch["labels"].to(device),
            )
            loss = out.loss
            if loss is None:
                continue
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            sched.step()
            pbar.set_postfix(loss=float(loss.item()))

    out_dir.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(out_dir))
    tokenizer.save_pretrained(str(out_dir))
    print(f"Saved model to {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "train_cas.json")
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent.parent / "models" / "cas_bert")
    ap.add_argument("--epochs", type=int, default=3)
    ap.add_argument("--batch-size", type=int, default=16)
    args = ap.parse_args()
    train(args.data, args.out, epochs=args.epochs, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
