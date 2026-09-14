"""
Hugging Face Transformers API compatibility.

Recent versions deprecate ``torch_dtype=`` on ``from_pretrained`` in favor of ``dtype=``.
We pick the supported parameter name via the method signature so older Transformers still work.
"""

from __future__ import annotations

from typing import Any


def dtype_kw_for_from_pretrained(model_class: Any, dtype: Any) -> dict[str, Any]:
    """
    Return a single-key dict to pass to ``ModelClass.from_pretrained(..., **kw)``.

    Examples:
        from_pretrained(path, **dtype_kw_for_from_pretrained(DistilBertForTokenClassification, torch.float32))
    """
    try:
        import inspect

        sig = inspect.signature(model_class.from_pretrained)
        if "dtype" in sig.parameters:
            return {"dtype": dtype}
    except (TypeError, ValueError, AttributeError):
        pass
    return {"torch_dtype": dtype}
