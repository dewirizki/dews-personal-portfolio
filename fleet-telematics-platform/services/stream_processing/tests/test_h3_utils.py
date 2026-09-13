import h3

from stream_processing.h3_utils import cell_int_to_parent_int, latlng_to_cell_int

# Times Square, NYC
LAT, LON = 40.7580, -73.9855


def test_latlng_to_cell_int_matches_h3_reference() -> None:
    cell_int = latlng_to_cell_int(LAT, LON, resolution=9)
    expected_hex = h3.latlng_to_cell(LAT, LON, 9)

    assert cell_int == h3.str_to_int(expected_hex)
    assert h3.is_valid_cell(h3.int_to_str(cell_int))
    assert h3.get_resolution(h3.int_to_str(cell_int)) == 9


def test_parent_rollup_is_consistent_with_h3_hierarchy() -> None:
    fine_cell = latlng_to_cell_int(LAT, LON, resolution=9)
    parent_cell = cell_int_to_parent_int(fine_cell, parent_resolution=8)

    fine_hex = h3.int_to_str(fine_cell)
    parent_hex = h3.int_to_str(parent_cell)

    assert h3.get_resolution(parent_hex) == 8
    assert h3.cell_to_parent(fine_hex, 8) == parent_hex


def test_nearby_points_can_land_in_the_same_r9_cell() -> None:
    # Two points ~20m apart, well within an r9 hexagon's ~174m edge length.
    cell_a = latlng_to_cell_int(LAT, LON, resolution=9)
    cell_b = latlng_to_cell_int(LAT + 0.0001, LON + 0.0001, resolution=9)

    assert cell_a == cell_b


def test_distant_points_land_in_different_r9_cells() -> None:
    times_square_cell = latlng_to_cell_int(LAT, LON, resolution=9)
    # Brooklyn, several km away.
    brooklyn_cell = latlng_to_cell_int(40.6782, -73.9442, resolution=9)

    assert times_square_cell != brooklyn_cell
