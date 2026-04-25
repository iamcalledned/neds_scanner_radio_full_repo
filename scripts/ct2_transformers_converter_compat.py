#!/usr/bin/env python3
"""
Compatibility wrapper for ``ct2-transformers-converter``.

Why this exists:
- ``ctranslate2`` 4.7.1 passes ``dtype=...`` into
  ``transformers`` model loaders.
- Newer ``transformers`` releases, including the local 4.53.3 install,
  route that through model constructors such as
  ``WhisperForConditionalGeneration.__init__``, which reject ``dtype``.

This wrapper remaps that argument to ``torch_dtype`` before delegating to the
upstream converter CLI.
"""

from __future__ import annotations

from typing import Any

from ctranslate2.converters import transformers as ct2_transformers
from ctranslate2.converters.transformers import TransformersConverter


_ORIGINAL_LOAD_MODEL = TransformersConverter.load_model


def _compat_load_model(self: TransformersConverter, model_class: type, model_name_or_path: str, **kwargs: Any) -> Any:
    if "dtype" in kwargs and "torch_dtype" not in kwargs:
        kwargs["torch_dtype"] = kwargs.pop("dtype")
    return _ORIGINAL_LOAD_MODEL(self, model_class, model_name_or_path, **kwargs)


TransformersConverter.load_model = _compat_load_model


if __name__ == "__main__":
    ct2_transformers.main()
