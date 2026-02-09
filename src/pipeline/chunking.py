"""
Chunking utilities for the audit‑trail pipeline.

This module breaks long documents into overlapping character‑based chunks.
Each chunk is assigned a deterministic identifier derived from the parent
document ID, the chunk index and the hash of the chunk text. Chunks are
written to disk in JSON lines format under ``data/processed/chunks/<doc_id>.jsonl``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, List


def sha256_hex(data: bytes | str) -> str:
    """Compute the SHA256 hash of a string or bytes and return its hex digest."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def chunk_document(
    doc_id: str,
    text: str,
    chunk_size: int = 1500,
    overlap: int = 200,
) -> List[Dict[str, object]]:
    """
    Split a document into overlapping chunks.

    Parameters
    ----------
    doc_id : str
        Identifier of the document.
    text : str
        The full text of the document.
    chunk_size : int, optional
        Maximum number of characters per chunk, by default 1500.
    overlap : int, optional
        Number of characters to overlap between consecutive chunks, by default 200.

    Returns
    -------
    List[Dict[str, object]]
        A list of chunk records. Each record contains the chunk_id, doc_id,
        chunk_index (0‑based), text, start_char and end_char positions.
    """
    chunks: List[Dict[str, object]] = []
    if not text:
        return chunks
    # Determine step size (chunk_size minus overlap, at least 1 to avoid infinite loop)
    step = max(chunk_size - overlap, 1)
    text_length = len(text)
    index = 0
    chunk_index = 0
    while index < text_length:
        start = index
        end = min(start + chunk_size, text_length)
        chunk_text = text[start:end]
        # Derive hash of chunk text and deterministic chunk_id
        chunk_hash = sha256_hex(chunk_text)
        chunk_id = sha256_hex(doc_id + str(chunk_index) + chunk_hash)
        chunks.append(
            {
                "chunk_id": chunk_id,
                "doc_id": doc_id,
                "chunk_index": chunk_index,
                "text": chunk_text,
                "start_char": start,
                "end_char": end,
            }
        )
        chunk_index += 1
        index += step
    return chunks


def write_chunks_to_file(
    doc_id: str,
    chunks: Iterable[Dict[str, object]],
    base_dir: Path,
) -> None:
    """
    Persist chunk records for a single document to a JSON lines file.

    Parameters
    ----------
    doc_id : str
        Identifier of the document.
    chunks : Iterable[Dict[str, object]]
        Chunk records to persist.
    base_dir : Path
        Root of the repository; chunks are written under ``data/processed/chunks``.
    """
    chunks_dir = base_dir / "data" / "processed" / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    out_path = chunks_dir / f"{doc_id}.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk) + "\n")
