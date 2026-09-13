"""H3 spatial indexing helpers.

H3 cell indexes are 64-bit values whose most-significant bit is always
reserved/zero (see H3's index bit layout: https://h3geo.org/docs/core-library/h3Indexing),
so the integer form always fits in a *signed* 64-bit long -- which is what
lets the same value flow through Spark's `LongType` and ClickHouse's
`UInt64` column without any overflow or sign-conversion handling.

Resolution choice (fine=9, coarse=8) is a config value, not hardcoded here,
so it can be tuned per deployment without touching this module -- see
docs/ARCHITECTURE.md §4 for why 9/8 was chosen for this project.
"""

from __future__ import annotations

import h3


def latlng_to_cell_int(lat: float, lon: float, resolution: int) -> int:
    """Assign a lat/lon pair to an H3 cell at `resolution`, returned as the
    unsigned 64-bit integer form ClickHouse's native H3 functions expect."""
    cell_hex = h3.latlng_to_cell(lat, lon, resolution)
    return h3.str_to_int(cell_hex)


def cell_int_to_parent_int(cell_int: int, parent_resolution: int) -> int:
    """Roll a fine-resolution cell up to its coarse-resolution ancestor,
    both as integers -- O(1), no re-indexing from lat/lon."""
    cell_hex = h3.int_to_str(cell_int)
    parent_hex = h3.cell_to_parent(cell_hex, parent_resolution)
    return h3.str_to_int(parent_hex)


def cell_int_to_boundary(cell_int: int) -> list[tuple[float, float]]:
    """Hexagon boundary vertices (lat, lon) for a cell -- used by the
    dashboard to render H3 polygons, not by the streaming job itself."""
    cell_hex = h3.int_to_str(cell_int)
    return list(h3.cell_to_boundary(cell_hex))
