#!/usr/bin/env python3
"""
Processor script for extracting plant foliage positions from Elysium (Terrain_021_DLC2).

Runs as a run_on_all processor: checks each pak for Terrain_021_DLC2 sublevel data,
extracts plant positions from foliage instances, and writes partial results to a JSON
file that is later merged across paks to produce the final ElysiumPlants.json.

Environment variables (set by process_single_pak.py):
  ICARUS_PAK_FILE       - Path to the original .pak file
  ICARUS_PAK_UNPACK_DIR - Path to extracted pak contents
  ICARUS_PAK_WORK_ROOT  - Per-pak temp work directory
  ICARUS_PAK_NAME       - Friendly pak name

Required tool:
  UE4EXPORT_EXE - Path to Ue4Export.exe (set by workflow)
"""

from __future__ import annotations

import glob
import json
import os
import struct
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Foliage type -> plant group mapping
# ---------------------------------------------------------------------------
# Built from InGameFiles/FLOD/D_FLODDescriptions.json: every FLOD row whose
# ViewTraceActor references a harvestable Resource Node blueprint.
PLANT_GROUPS: dict[str, str] = {
    # Agave
    "FT_DC_Agave_Var1": "Agave",
    "FT_DC_Agave_Var2": "Agave",
    "FT_DC_Agave_Var3": "Agave",
    # Avocado
    "FT_Avocado_Var1": "Avocado",
    "FT_Avocado_Var2": "Avocado",
    "FT_Avocado_Var3": "Avocado",
    # Banana
    "FT_HRB_Banana_Var1": "Banana",
    "FT_HRB_Banana_Var2": "Banana",
    "FT_HRB_Banana_Var3": "Banana",
    # Beans
    "FT_Beans": "Beans",
    "FT_LC_Beans": "Beans",
    # Berries
    "FT_BerryBush": "Berries",
    # Carrot
    "FT_Carrot": "Carrot",
    # Cocoa
    "FT_Cocoa": "Cocoa",
    "FT_LC_Cocoa": "Cocoa",
    # Coffee
    "FT_Coffee": "Coffee",
    # Corn
    "FT_CornCob_01": "Corn",
    # Garlic
    "FT_Garlic_Var1": "Garlic",
    "FT_Garlic_Var2": "Garlic",
    "FT_Garlic_Var3": "Garlic",
    # Kiwi
    "FT_KiwiFruit_Var1": "Kiwi",
    "FT_KiwiFruit_Var2": "Kiwi",
    "FT_KiwiFruit_Var3": "Kiwi",
    # Kumara
    "FT_Kumara_Var1": "Kumara",
    "FT_Kumara_Var2": "Kumara",
    "FT_Kumara_Var3": "Kumara",
    "FT_LC_Kumara": "Kumara",
    # Mushroom
    "FT_MushroomA": "Mushroom",
    # Onion
    "FT_Onion_Var1": "Onion",
    "FT_Onion_Var2": "Onion",
    "FT_Onion_Var3": "Onion",
    # Potato
    "FT_SW_Potato_Wild": "Potato",
    "FT_TU_Potato_Wild": "Potato",
    "FT_SW_Potato": "Potato",
    "FT_TU_Potato": "Potato",
    # Prickly Pear
    "FT_PricklyPear_Var1": "PricklyPear",
    "FT_PricklyPear_Var2": "PricklyPear",
    "FT_PricklyPear_Var3": "PricklyPear",
    "FT_PricklyPear_Var4": "PricklyPear",
    "FT_DC_Prickly_Pear_Var1": "PricklyPear",
    "FT_DC_Prickly_Pear_Var2": "PricklyPear",
    "FT_DC_Prickly_Pear_Var3": "PricklyPear",
    "FT_DC_Prickly_Pear_Var4": "PricklyPear",
    # Pumpkin
    "FT_Pumpkin": "Pumpkin",
    "FT_TU_Pumpkin": "Pumpkin",
    # Reed
    "FT_ReedFlower_01": "Reed",
    # Rhubarb
    "FT_Rhubarb_Var1": "Rhubarb",
    "FT_Rhubarb_Var2": "Rhubarb",
    "FT_Rhubarb_Var3": "Rhubarb",
    # Squash
    "FT_Squash": "Squash",
    "FT_LC_Squash": "Squash",
    # Sugar Cane
    "FT_HRB_Sugar_Cane_Var1": "SugarCane",
    "FT_HRB_Sugar_Cane_Var2": "SugarCane",
    "FT_HRB_Sugar_Cane_Var3": "SugarCane",
    # Tea
    "FT_WildTea": "Tea",
    "FT_LC_WildTea": "Tea",
    "FT_GreenTea": "Tea",
    # Tomato
    "FT_Tomatoes_Wild": "Tomato",
    # Truffle
    "FT_Truffle_Var1": "Truffle",
    "FT_Truffle_Var2": "Truffle",
    "FT_Truffle_Var3": "Truffle",
    "FT_DC_TrufflePlant_Var1_Temp": "Truffle",
    "FT_DC_TrufflePlant_Var2_Temp": "Truffle",
    "FT_DC_TrufflePlant_Var3_Temp": "Truffle",
    # Watermelon
    "FT_Watermelon": "Watermelon",
    # Wheat
    "FT_Wheat_03": "Wheat",
    # Yeast
    "FT_YeastPlant_01": "Yeast",
    # Lily
    "FT_Alpine_Lily_01": "Lily",
    # Sponge
    "FT_Sponge_01": "Sponge",
}

# Icarus UE4 engine version for Ue4Export
ENGINE_VERSION = "UE4_27"

# Sublevel filename patterns for the main Elysium map tiles
SUBLEVEL_PATTERNS = [
    "T021_Generated_x?_y?",
    "T021_Generated_Vista",
    "T021_Developer_*",
]


def fail(msg: str) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Ue4Export helpers
# ---------------------------------------------------------------------------

def run_ue4export(
    ue4export_exe: str,
    paks_dir: str,
    asset_list_path: str,
    output_dir: str,
    *,
    quiet: bool = True,
) -> None:
    cmd = [ue4export_exe]
    if quiet:
        cmd.append("--quiet")
    cmd.append("--mix-output")
    cmd.extend([paks_dir, ENGINE_VERSION, asset_list_path, output_dir])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Ue4Export stderr: {result.stderr}", file=sys.stderr)
        fail(f"Ue4Export failed with code {result.returncode}")


def check_terrain_021_in_pak(ue4export_exe: str, pak_dir: str, work_dir: str) -> bool:
    """Quick probe: try to export one known sublevel to see if this pak has T021."""
    probe_list = os.path.join(work_dir, "probe_list.txt")
    probe_out = os.path.join(work_dir, "probe_out")
    os.makedirs(probe_out, exist_ok=True)

    with open(probe_list, "w") as f:
        f.write("[Text]\n")
        f.write("Icarus/Content/Maps/Terrain_021_DLC2/Sublevels/Generated/T021_Generated_x0_y0\n")

    cmd = [ue4export_exe, "--quiet", "--mix-output", pak_dir, ENGINE_VERSION, probe_list, probe_out]
    subprocess.run(cmd, capture_output=True, text=True)

    found = any(
        fname.endswith(".json")
        for fname in glob.glob(os.path.join(probe_out, "**", "*.json"), recursive=True)
    )
    return found


# ---------------------------------------------------------------------------
# Binary instance extraction
# ---------------------------------------------------------------------------

def extract_positions_from_binary(
    bindata: bytes, count: int, bounds: dict,
) -> list[tuple[float, float, float]] | None:
    """Search binary for an FMatrix instance array with the given count.

    Each instance is a 4x4 float matrix (64 bytes). Translation sits at
    floats [12,13,14] (byte offset 48 within each 64-byte record).
    Float [15] is always 1.0 — used as a validation check.
    """
    count_bytes = struct.pack("<I", count)
    bmin = bounds.get("Min", {})
    bmax = bounds.get("Max", {})

    pos = 0
    while True:
        pos = bindata.find(count_bytes, pos)
        if pos < 0:
            return None
        if pos + 4 + count * 64 > len(bindata):
            pos += 4
            continue

        t_off = pos + 4 + 48
        tx = struct.unpack("<f", bindata[t_off : t_off + 4])[0]
        ty = struct.unpack("<f", bindata[t_off + 4 : t_off + 8])[0]
        tz = struct.unpack("<f", bindata[t_off + 8 : t_off + 12])[0]
        w = struct.unpack("<f", bindata[t_off + 12 : t_off + 16])[0]

        if abs(w - 1.0) >= 0.01 or not (-400000 < tx < 400000 and -400000 < ty < 400000):
            pos += 4
            continue

        if bmin and bmax:
            margin = 2000
            if not (
                bmin.get("X", -1e9) - margin < tx < bmax.get("X", 1e9) + margin
                and bmin.get("Y", -1e9) - margin < ty < bmax.get("Y", 1e9) + margin
            ):
                pos += 4
                continue

        positions: list[tuple[float, float, float]] = []
        for i in range(count):
            i_off = pos + 4 + i * 64 + 48
            ix = struct.unpack("<f", bindata[i_off : i_off + 4])[0]
            iy = struct.unpack("<f", bindata[i_off + 4 : i_off + 8])[0]
            iz = struct.unpack("<f", bindata[i_off + 8 : i_off + 12])[0]
            positions.append((ix, iy, iz))
        return positions


# ---------------------------------------------------------------------------
# Sublevel processing
# ---------------------------------------------------------------------------

def process_sublevel(
    json_path: str, uexp_path: str,
) -> dict[str, list[tuple[float, float, float]]]:
    """Parse one sublevel's JSON + binary and return plant positions."""
    with open(json_path, "r", encoding="utf-8-sig") as f:
        try:
            jdata = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    with open(uexp_path, "rb") as f:
        bindata = f.read()

    foliage_actor = None
    components: dict[str, dict] = {}
    for obj in jdata:
        if not isinstance(obj, dict):
            continue
        if obj.get("Name", "").startswith("InstancedFoliageActor") and "FoliageInfos" in obj:
            foliage_actor = obj
        name = obj.get("Name", "")
        if "FISMComponent" in name or "FoliageInstanced" in name:
            components[name] = obj

    if foliage_actor is None:
        return {}

    positions: dict[str, list[tuple[float, float, float]]] = defaultdict(list)

    for ft_path, info in foliage_actor["FoliageInfos"].items():
        ft_name = ft_path.split("/")[-1].split(".")[0]
        if ft_name not in PLANT_GROUPS:
            continue
        plant_group = PLANT_GROUPS[ft_name]

        comp_ref = (info.get("Implementation") or {}).get("Component")
        if not comp_ref:
            continue
        comp_name = comp_ref.get("ObjectName", "").split(".")[-1].rstrip("'")
        comp = components.get(comp_name)
        if not comp:
            continue
        props = comp.get("Properties", {})

        # Standard components: PerInstanceSMData already in JSON
        if "PerInstanceSMData" in comp:
            for inst in comp["PerInstanceSMData"]:
                trans = inst.get("TransformData", {}).get("Translation", {})
                positions[plant_group].append(
                    (trans.get("X", 0.0), trans.get("Y", 0.0), trans.get("Z", 0.0))
                )
            continue

        # FLOD components: search binary for FMatrix array
        count = props.get("NumBuiltInstances", 0)
        if count is None or count <= 0:
            count = props.get("InstanceCountToRender", 0)
        if count is None or count <= 0:
            count = len(props.get("SortedInstances", []))
        if count <= 0:
            continue

        bounds = props.get("BuiltInstanceBounds", {})
        result = extract_positions_from_binary(bindata, count, bounds)
        if result:
            positions[plant_group].extend(result)

    return dict(positions)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    pak_file = os.environ.get("ICARUS_PAK_FILE", "")
    work_root = os.environ.get("ICARUS_PAK_WORK_ROOT", "")
    pak_name = os.environ.get("ICARUS_PAK_NAME", "unknown")
    ue4export_exe = os.environ.get("UE4EXPORT_EXE", "")

    if not pak_file or not work_root:
        fail("ICARUS_PAK_FILE and ICARUS_PAK_WORK_ROOT must be set")
    if not ue4export_exe:
        fail("UE4EXPORT_EXE must be set")
    if not os.path.isfile(ue4export_exe):
        fail(f"Ue4Export not found: {ue4export_exe}")

    pak_dir = os.path.dirname(pak_file)

    # Step 1: Quick probe — does this pak contain Terrain_021_DLC2?
    print(f"[elysium-plants] Probing {pak_name} for Terrain_021_DLC2...")
    if not check_terrain_021_in_pak(ue4export_exe, pak_dir, work_root):
        print(f"[elysium-plants] {pak_name} does not contain Terrain_021 data, skipping.")
        return

    print(f"[elysium-plants] {pak_name} contains Terrain_021 data, extracting...")

    # Step 2: Export sublevels as Text (JSON) and Raw (binary)
    text_dir = os.path.join(work_root, "elysium_text")
    raw_dir = os.path.join(work_root, "elysium_raw")
    os.makedirs(text_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    text_list = os.path.join(work_root, "elysium_text_list.txt")
    raw_list = os.path.join(work_root, "elysium_raw_list.txt")

    with open(text_list, "w") as f:
        f.write("[Text]\nIcarus/Content/Maps/Terrain_021_DLC2/Sublevels/*\n")
    with open(raw_list, "w") as f:
        f.write("[Raw]\nIcarus/Content/Maps/Terrain_021_DLC2/Sublevels/*\n")

    run_ue4export(ue4export_exe, pak_dir, text_list, text_dir)
    run_ue4export(ue4export_exe, pak_dir, raw_list, raw_dir)

    # Step 3: Process each main sublevel
    sublevels_root = os.path.join(
        text_dir, "Icarus", "Content", "Maps", "Terrain_021_DLC2", "Sublevels",
    )
    raw_sublevels_root = os.path.join(
        raw_dir, "Icarus", "Content", "Maps", "Terrain_021_DLC2", "Sublevels",
    )

    json_files: list[str] = []
    for pattern in SUBLEVEL_PATTERNS:
        for subdir in ("Generated", "Developer"):
            json_files.extend(
                glob.glob(os.path.join(sublevels_root, subdir, pattern + ".json"))
            )

    all_positions: dict[str, list[list[float]]] = defaultdict(list)

    for json_path in sorted(json_files):
        rel = os.path.relpath(json_path, sublevels_root)
        uexp_path = os.path.join(raw_sublevels_root, rel.replace(".json", ".uexp"))
        if not os.path.isfile(uexp_path):
            continue

        positions = process_sublevel(json_path, uexp_path)
        for group, pos_list in positions.items():
            all_positions[group].extend(
                [round(x, 2), round(y, 2), round(z, 2)] for x, y, z in pos_list
            )

    total = sum(len(v) for v in all_positions.values())
    print(f"[elysium-plants] Extracted {total} plant instances across {len(all_positions)} types from {pak_name}")

    if total == 0:
        print("[elysium-plants] No plant instances found, skipping artifact.")
        return

    # Step 4: Write partial results
    # Write outside ICARUS_PAK_WORK_ROOT (which gets cleaned up by
    # process_single_pak.py).  Prefer ELYSIUM_PLANTS_PARTIAL_DIR if set,
    # otherwise fall back to RUNNER_TEMP (persists across composite action
    # steps), and only use work_root as a last resort for local testing.
    partial_dir = (
        os.environ.get("ELYSIUM_PLANTS_PARTIAL_DIR")
        or os.path.join(os.environ.get("RUNNER_TEMP", work_root), "elysium-plants-partials")
    )
    os.makedirs(partial_dir, exist_ok=True)

    # Use pak name in filename to avoid collisions
    safe_name = pak_name.replace(" ", "_").replace("/", "_")
    partial_path = os.path.join(partial_dir, f"elysium-plants-{safe_name}.json")

    with open(partial_path, "w") as f:
        json.dump(all_positions, f, separators=(",", ":"))

    print(f"[elysium-plants] Wrote partial results to {partial_path}")


if __name__ == "__main__":
    main()
