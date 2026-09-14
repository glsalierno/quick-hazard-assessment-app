"""
Hugging Face model manager for Smart CAS extraction.
Supports local causal LMs with optional quantization; progressive fallback.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional

try:
    import torch
    _TORCH_AVAILABLE = True
except ImportError:
    torch = None  # type: ignore[misc, assignment]
    _TORCH_AVAILABLE = False

try:
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    _TRANSFORMERS_AVAILABLE = True
except ImportError:
    AutoModelForCausalLM = AutoTokenizer = BitsAndBytesConfig = None  # type: ignore[misc, assignment]
    _TRANSFORMERS_AVAILABLE = False

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[misc, assignment]


class HFModelManager:
    """
    Manage local Hugging Face models for CAS extraction.
    Supports multiple backends with progressive fallback.
    """

    def __init__(self, cache_dir: Optional[Path] = None) -> None:
        self.cache_dir = Path(cache_dir) if cache_dir else Path.home() / ".cache" / "hf_models"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.model: Any = None
        self.tokenizer: Any = None
        self.model_loaded = False
        self.current_model: Optional[str] = None

        self.model_options: dict[str, dict[str, Any]] = {
            "tiny": {
                "name": "HuggingFaceTB/SmolLM2-1.7B-Instruct",
                "ram_gb": 4,
                "quantization": None,
                "description": "Tiny but capable - runs on 4GB RAM",
            },
            "small": {
                "name": "microsoft/Phi-3-mini-4k-instruct",
                "ram_gb": 6,
                "quantization": None,
                "description": "Microsoft's efficient Phi-3",
            },
        }

    def load_model(self, model_size: str = "small") -> bool:
        """Load model with appropriate quantization."""
        if not _TORCH_AVAILABLE or not _TRANSFORMERS_AVAILABLE:
            return False
        if model_size not in self.model_options:
            model_size = "small"

        model_info = self.model_options[model_size]
        model_id = model_info["name"]

        try:
            quantization_config = None
            if model_info.get("quantization") == "4bit" and torch is not None and torch.cuda.is_available():
                if BitsAndBytesConfig is not None:
                    quantization_config = BitsAndBytesConfig(
                        load_in_4bit=True,
                        bnb_4bit_quant_type="nf4",
                        bnb_4bit_compute_dtype=torch.float16,
                        bnb_4bit_use_double_quant=True,
                    )

            self.tokenizer = AutoTokenizer.from_pretrained(
                model_id,
                trust_remote_code=True,
                cache_dir=str(self.cache_dir),
            )
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            kwargs: dict[str, Any] = {
                "trust_remote_code": True,
                "cache_dir": str(self.cache_dir),
            }
            if quantization_config is not None:
                kwargs["quantization_config"] = quantization_config
            else:
                from utils.hf_transformers_compat import dtype_kw_for_from_pretrained

                if torch is not None and torch.cuda.is_available():
                    kwargs["device_map"] = "auto"
                    kwargs.update(
                        dtype_kw_for_from_pretrained(AutoModelForCausalLM, torch.float16)
                    )
                else:
                    kwargs.update(
                        dtype_kw_for_from_pretrained(AutoModelForCausalLM, torch.float32)
                    )

            self.model = AutoModelForCausalLM.from_pretrained(model_id, **kwargs)

            self.model_loaded = True
            self.current_model = model_id
            return True
        except Exception as e:
            if hasattr(e, "__module__"):
                print(f"Failed to load model: {e}")
            return False

    def generate(self, prompt: str, max_tokens: int = 500) -> str:
        """Generate text from prompt."""
        if not self.model_loaded or self.model is None or self.tokenizer is None:
            return ""
        if not _TORCH_AVAILABLE:
            return ""

        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=2048,
        )
        device = next(self.model.parameters()).device
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=max_tokens,
                temperature=0.1,
                do_sample=False,
                pad_token_id=self.tokenizer.pad_token_id,
            )

        response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if response.startswith(prompt[: min(100, len(prompt))]):
            response = response[len(prompt) :].strip()
        return response.strip()

    def is_available(self) -> bool:
        """Check if model is loaded and available."""
        return self.model_loaded

    def get_memory_usage(self) -> dict[str, float]:
        """Get current memory usage in GB."""
        memory: dict[str, float] = {}
        if psutil is not None:
            v = psutil.virtual_memory()
            memory["ram_gb"] = v.used / (1024**3)
            memory["ram_total_gb"] = v.total / (1024**3)
        if _TORCH_AVAILABLE and torch is not None and torch.cuda.is_available():
            memory["gpu_gb"] = torch.cuda.memory_allocated() / (1024**3)
            memory["gpu_total_gb"] = torch.cuda.get_device_properties(0).total_memory / (1024**3)
        return memory
