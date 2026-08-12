from __future__ import annotations

import hashlib
import re
from collections import Counter

from pyspark.sql import DataFrame


_INVALID_COLUMN_CHARS = re.compile(r"[^a-z0-9]+")


def normalize_columns(df: DataFrame) -> DataFrame:
    """Normalize top-level DataFrame column names.

    Rules:
    - Convert names to lowercase.
    - Replace groups of non-alphanumeric characters with underscores.
    - Remove leading and trailing underscores.
    - Prefix names beginning with a digit with ``column_``.
    - Append a deterministic hash when different source names normalize
      to the same name.
    - Raise an error when identical original column names cannot be
      resolved deterministically.
    """

    def safe_name(name: str) -> str:
        normalized = (
            _INVALID_COLUMN_CHARS
            .sub("_", name.strip().lower())
            .strip("_")
        )

        if not normalized:
            raise ValueError(
                "A column name became empty after normalization."
            )

        return (
            f"column_{normalized}"
            if normalized[0].isdigit()
            else normalized
        )

    def name_hash(name: str) -> str:
        return hashlib.sha256(
            name.encode("utf-8")
        ).hexdigest()[:8]

    original_names = df.columns

    normalized_names = [
        safe_name(name)
        for name in original_names
    ]

    normalized_counts = Counter(normalized_names)

    duplicates = {
        name
        for name, count in normalized_counts.items()
        if count > 1
    }

    if duplicates:
        normalized_names = [
            (
                f"{normalized}_{name_hash(original)}"
                if normalized in duplicates
                else normalized
            )
            for original, normalized in zip(
                original_names,
                normalized_names,
            )
        ]

    final_counts = Counter(normalized_names)

    unresolved_duplicates = sorted(
        name
        for name, count in final_counts.items()
        if count > 1
    )

    if unresolved_duplicates:
        raise ValueError(
            "Column normalization still produced duplicates after "
            "appending deterministic hashes. This can occur when the "
            "source contains identical original column names: "
            + ", ".join(unresolved_duplicates)
        )

    return df.toDF(*normalized_names)