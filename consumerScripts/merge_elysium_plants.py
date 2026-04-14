#!/usr/bin/env python3
"""
Merge partial raw-position files and generate the final ElysiumPlants.json.

Reads one or more JSON files containing raw foliage positions (produced by
generate_elysium_plants.py or tools/extract_plants.py) and writes a DataMaps-
format JSON file for the Icarus wiki interactive map.

Usage:
  # From partial files in a directory:
  python merge_elysium_plants.py --partials-dir /path/to/partials --out wiki/maps/ElysiumPlants.json

  # From a single raw-positions file (local dev):
  python merge_elysium_plants.py --raw tmp_raw_positions.json --out wiki/maps/ElysiumPlants.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants from IcarusDataMiner WorldDataUtil / FoliageMiner
# ---------------------------------------------------------------------------
WORLD_TILE_SIZE = 100800
WORLD_CELL_SIZE = WORLD_TILE_SIZE // 2  # 50400
CLUSTER_DISTANCE_THRESHOLD = WORLD_CELL_SIZE * 0.1  # 5040
PARTITION_SIZE = WORLD_CELL_SIZE * 0.25  # 12600

# ---------------------------------------------------------------------------
# Group metadata: id -> (display_name, icon, [w, h])
# Existing groups copied from the current ElysiumPlants.json, plus new ones
# for plant types that were missing from the original IcarusDataMiner output.
# ---------------------------------------------------------------------------
GROUP_META: dict[str, tuple[str, str, list[int]]] = {
    "Agave":      ("Agave",         "T ITEM Agave.png",        [40, 40]),
    "Avocado":    ("Avocados",      "ITEM_Avocado.png",        [40, 40]),
    "Banana":     ("Bananas",       "T_ITEM_Banana.png",       [40, 40]),
    "Beans":      ("Soy Beans",     "ITEM Bean.png",           [40, 40]),
    "Berries":    ("Wild Berries",  "ITEM Wild Berry.png",     [40, 40]),
    "Carrot":     ("Carrots",       "ITEM Carrot.png",         [40, 40]),
    "Cocoa":      ("Cocoa",         "ITEM Cocoa.png",          [40, 40]),
    "Coffee":     ("Coffee Beans",  "ITEM Coffee Bean.png",    [40, 40]),
    "Corn":       ("Corn",          "ITEM_Corn_Cob.png",       [40, 40]),
    "Garlic":     ("Garlic",        "T_ITEM_Garlic.png",       [40, 40]),
    "Kiwi":       ("Kiwifruit",     "ITEM_Kiwi_Fruit.png",    [40, 40]),
    "Kumara":     ("Kumara",        "ITEM_Kumara.png",         [40, 40]),
    "Lily":       ("Lily",          "ITEM_Alpine_Lily.png",    [40, 40]),
    "Mushroom":   ("Mushrooms",     "ITEM Mushroom.png",       [40, 40]),
    "Onion":      ("Onions",        "T_ITEM_Onion.png",        [35, 35]),
    "Potato":     ("Potatoes",      "ITEM Potato.png",         [40, 40]),
    "PricklyPear": ("Prickly Pear", "ITEM_Prickly_Pear.png",  [40, 40]),
    "Pumpkin":    ("Pumpkin",       "ITEM_Pumpkin.png",        [40, 40]),
    "Reed":       ("Reed Flowers",  "ITEM_Reeds.png",          [40, 40]),
    "Rhubarb":    ("Rhubarb",       "ITEM_Rhubarb.png",        [40, 40]),
    "Sponge":     ("Sponge",        "ITEM_Sponge.png",         [40, 40]),
    "Squash":     ("Squash",        "ITEM_Squash.png",         [40, 40]),
    "SugarCane":  ("Sugar Cane",    "ITEM_Sugar_Cane.png",     [40, 40]),
    "Tea":        ("Tea",           "ITEM Tea.png",            [40, 40]),
    "Tomato":     ("Tomato",        "ITEM_Tomato.png",         [35, 35]),
    "Truffle":    ("Truffles",      "T_ITEM_Truffle.png",      [35, 35]),
    "Watermelon": ("Watermelon",    "ITEM_Watermelon.png",     [40, 40]),
    "Wheat":      ("Wheat",         "ITEM_Wheat.png",          [40, 40]),
    "Yeast":      ("Yeast",         "ITEM_Yeast.png",          [40, 40]),
}

# Article name overrides (group_id -> wiki article name).
# Only needed when the article name differs from the display name.
GROUP_ARTICLES: dict[str, str] = {
    "Beans": "Soy Beans",
    "Berries": "Wild Berries",
    "Coffee": "Coffee Beans",
    "Reed": "Reed Flowers",
    "SugarCane": "Sugar Cane",
    "PricklyPear": "Prickly Pear",
}


# ---------------------------------------------------------------------------
# Bounding-box cluster (matches IcarusDataMiner's ClusterBuilder)
# ---------------------------------------------------------------------------
class _Cluster:
    """A cluster with a bounding box.  A point is accepted only when it falls
    within ``threshold`` of *every* edge of the current bounding box, which
    naturally caps the cluster at ~threshold in each dimension."""

    __slots__ = ("min_x", "max_x", "min_y", "max_y", "min_z", "max_z",
                 "count", "_threshold")

    def __init__(self, x: float, y: float, z: float, threshold: float) -> None:
        self.min_x = self.max_x = x
        self.min_y = self.max_y = y
        self.min_z = self.max_z = z
        self.count = 1
        self._threshold = threshold

    def try_add(self, x: float, y: float, z: float) -> bool:
        t = self._threshold
        if (x < self.min_x + t and x > self.max_x - t and
                y < self.min_y + t and y > self.max_y - t):
            if x < self.min_x:
                self.min_x = x
            elif x > self.max_x:
                self.max_x = x
            if y < self.min_y:
                self.min_y = y
            elif y > self.max_y:
                self.max_y = y
            if z < self.min_z:
                self.min_z = z
            elif z > self.max_z:
                self.max_z = z
            self.count += 1
            return True
        return False

    def try_combine(self, other: "_Cluster") -> bool:
        t = self._threshold
        if (abs(self.max_x - other.min_x) < t and
                abs(self.min_x - other.max_x) < t and
                abs(self.max_y - other.min_y) < t and
                abs(self.min_y - other.max_y) < t):
            if other.min_x < self.min_x:
                self.min_x = other.min_x
            if other.max_x > self.max_x:
                self.max_x = other.max_x
            if other.min_y < self.min_y:
                self.min_y = other.min_y
            if other.max_y > self.max_y:
                self.max_y = other.max_y
            if other.min_z < self.min_z:
                self.min_z = other.min_z
            if other.max_z > self.max_z:
                self.max_z = other.max_z
            self.count += other.count
            return True
        return False

    @property
    def center_x(self) -> float:
        return (self.min_x + self.max_x) * 0.5

    @property
    def center_y(self) -> float:
        return (self.min_y + self.max_y) * 0.5


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------
def cluster_positions(
    positions: list[tuple[float, float, float]],
    threshold: float = CLUSTER_DISTANCE_THRESHOLD,
    partition: float = PARTITION_SIZE,
) -> list[tuple[float, float, int]]:
    """Cluster 3D positions into (center_x, center_y, count) groups.

    Uses the same bounding-box algorithm as IcarusDataMiner's ClusterBuilder:
    each cluster's bounding box can grow at most ``threshold`` in each axis,
    preventing the transitive chaining that union-find would cause.
    """
    if not positions:
        return []

    # World bounds for cell mapping (Elysium: -400000 to 400000)
    world_min = -400000.0

    cell_count = int(math.ceil(800000.0 / partition))

    # Phase 1: assign each point to a cluster within its partition cell
    cells: list[list[_Cluster]] = [[] for _ in range(cell_count * cell_count)]

    def _cell_idx(x: float, y: float) -> int:
        cx = min(max(int(math.floor((x - world_min) / partition)), 0), cell_count - 1)
        cy = min(max(int(math.floor((y - world_min) / partition)), 0), cell_count - 1)
        return cy * cell_count + cx

    for x, y, z in positions:
        idx = _cell_idx(x, y)
        added = False
        for cluster in cells[idx]:
            if cluster.try_add(x, y, z):
                added = True
                break
        if not added:
            cells[idx].append(_Cluster(x, y, z, threshold))

    # Phase 2: merge clusters across adjacent cells (right, below, diagonal)
    for cy in range(cell_count - 1):
        for cx in range(cell_count - 1):
            targets = cells[cy * cell_count + cx]
            for dy in range(2):
                for dx in range(2):
                    if dx == 0 and dy == 0:
                        continue
                    sources = cells[(cy + dy) * cell_count + (cx + dx)]
                    for target in targets:
                        si = 0
                        while si < len(sources):
                            if target.try_combine(sources[si]):
                                sources.pop(si)
                            else:
                                si += 1

    # Collect results
    result: list[tuple[float, float, int]] = []
    for cell in cells:
        for cluster in cell:
            result.append((cluster.center_x, cluster.center_y, cluster.count))

    result.sort(key=lambda c: (c[0], c[1]))
    return result


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------
def load_partials(directory: str) -> dict[str, list[tuple[float, float, float]]]:
    """Load and merge all partial JSON files from a directory."""
    merged: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for fname in sorted(os.listdir(directory)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(directory, fname)
        with open(path, "r") as f:
            data = json.load(f)
        for group, positions in data.items():
            merged[group].extend(
                (p[0], p[1], p[2] if len(p) > 2 else 0.0) for p in positions
            )
    return dict(merged)


def load_raw(path: str) -> dict[str, list[tuple[float, float, float]]]:
    """Load a single raw-positions file (e.g. tmp_raw_positions.json)."""
    with open(path, "r") as f:
        data = json.load(f)
    return {
        group: [(p[0], p[1], p[2] if len(p) > 2 else 0.0) for p in positions]
        for group, positions in data.items()
    }


# ---------------------------------------------------------------------------
# DataMaps output
# ---------------------------------------------------------------------------
def build_datamaps_json(
    clustered: dict[str, list[tuple[float, float, int]]],
) -> dict:
    """Build the DataMaps-format JSON structure."""
    # Header
    output: dict = {
        "$schema": "https://icarus.wiki.gg/extensions/DataMaps/schemas/v17.3.json",
        "crs": {
            "order": "xy",
            "topLeft": [-400000, -400000],
            "bottomRight": [400000, 400000],
        },
        "backgrounds": [
            {
                "name": "Elysium",
                "associatedLayer": None,
                "image": "MAP Elysium.jpg",
                "overlays": [
                    {
                        "name": "Grid",
                        "image": "MAP Grid 4096.png",
                        "at": [[-400000, -400000], [400000, 400000]],
                    }
                ],
            }
        ],
        "settings": {"enableSearch": True, "showCoordinates": True},
    }

    # Build groups (only for plant types that have data)
    groups: dict[str, dict] = {}
    for group_id in sorted(clustered.keys()):
        meta = GROUP_META.get(group_id)
        if not meta:
            print(f"Warning: no metadata for group {group_id!r}, skipping", file=sys.stderr)
            continue
        display_name, icon, size = meta
        groups[group_id] = {"name": display_name, "size": size, "icon": icon}
    output["groups"] = groups

    # Build markers
    markers: dict[str, list[dict]] = {}
    for group_id in sorted(clustered.keys()):
        if group_id not in groups:
            continue
        meta = GROUP_META[group_id]
        display_name = meta[0]
        article = GROUP_ARTICLES.get(group_id, display_name)
        group_markers: list[dict] = []
        for idx, (cx, cy, count) in enumerate(clustered[group_id], start=1):
            group_markers.append(
                {
                    "x": round(cx, 2),
                    "y": round(cy, 2),
                    "id": f"{group_id}-{idx}",
                    "name": display_name,
                    "description": f"Amount of {display_name}: {count}",
                    "article": article,
                }
            )
        markers[group_id] = group_markers
    output["markers"] = markers

    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge raw foliage positions and generate ElysiumPlants.json",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--partials-dir",
        help="Directory containing partial JSON files to merge",
    )
    source.add_argument(
        "--raw",
        help="Single raw-positions JSON file (local dev shortcut)",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output path for ElysiumPlants.json",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=CLUSTER_DISTANCE_THRESHOLD,
        help=f"Clustering distance threshold (default: {CLUSTER_DISTANCE_THRESHOLD})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load positions
    if args.partials_dir:
        all_positions = load_partials(args.partials_dir)
    else:
        all_positions = load_raw(args.raw)

    if not all_positions:
        print("No positions loaded, nothing to do.", file=sys.stderr)
        sys.exit(1)

    # Cluster
    clustered: dict[str, list[tuple[float, float, int]]] = {}
    for group in sorted(all_positions.keys()):
        clusters = cluster_positions(all_positions[group], threshold=args.threshold)
        if clusters:
            clustered[group] = clusters

    # Summary
    total_instances = sum(
        sum(c[2] for c in clusters) for clusters in clustered.values()
    )
    total_clusters = sum(len(clusters) for clusters in clustered.values())
    print(f"Clustered {total_instances} instances into {total_clusters} markers "
          f"across {len(clustered)} plant types")
    for group in sorted(clustered.keys()):
        n_clusters = len(clustered[group])
        n_instances = sum(c[2] for c in clustered[group])
        print(f"  {group}: {n_clusters} clusters ({n_instances} instances)")

    # Generate output
    output = build_datamaps_json(clustered)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="\n") as f:
        json.dump(output, f, indent="\t", ensure_ascii=False)
        f.write("\n")

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
