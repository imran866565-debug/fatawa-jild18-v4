#!/usr/bin/env python3
"""Merge Fatawa Razviya Jild 18 easy-Urdu tashreeh v4 chunks into jild-18.json.

Reads the 13 leaf chunk files listed in chunks-manifest-v4-jild18.json,
concatenates them in order 01 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07 -> 08
-> 09 -> 10 -> 11 -> 12 -> 13, JOINS the 9 simple duplicate-id pairs
(masala-42-43, 59, 76, 83, 112, 114, 126, 141, 142) and the 2 cross-id
groups (masala-90: head + ansah-sawal-1 + ansah-sawal-2; masala-94:
head + 12 p-items) into single items, derives start_page = min(pages)
where the field is absent, normalises page markers, validates, and
writes {"masail": [...]} to jild-18.json.

ORDER -- chunk order is the order:
  The v3 merged file ~/workspace/fatawa-app/content/tashreeh/jild-18.json
  has ID order == concatenation order of the v4 leaf chunks (verified
  locally by the coordinator: all 126 ids match the v3 sequence exactly
  after the 11 joins). Do NOT sort or reorder. Within each chunk,
  items keep file order. CHUNK_ORDER is an explicit 13-leaf list.

SIMPLE CONSECUTIVE-DUPLICATE JOINS (9): a mas'ala was written in two
  leaves because it is too big for one worker, so concatenation gives
  the same id twice consecutively. Each pair is joined into ONE item:
  pages = sorted union, sawal = the single non-empty one (it must be
  in the FIRST part — verified on the leaves for all 9), jawab = parts
  concatenated with a single space, notes = concatenated in order,
  title / volume_id / start_page from the FIRST part. The v3 chunk
  files already carry different descriptive titles for the two halves;
  the merged-v3 title equals the FIRST part's title for all 9 (and
  for both cross-id heads) — verified by the coordinator — so the
  join asserts the joined title against the embedded merged-v3 title
  (V3_TITLES). title_must_match is deliberately NOT used: the two
  halves' titles differ by design in the v3 chunks.
  masala-42-43 [183-194], masala-59 [237-241], masala-76 [289,290],
  masala-83 [318-357], masala-112 [487-500], masala-114 [509-564],
  masala-126 [589,590], masala-141 [624-657], masala-142 [657-708].
CROSS-ID JOINS (2):
  masala-90 (3-way): c06 head jild18-masala-90 [389] (sawal only,
    jawab EMPTY) + c07 jild18-ansah-sawal-1 [390-391] (sawal EMPTY)
    + c07 jild18-ansah-sawal-2 [391-404] -> ONE item; sawal = head
    sawal + sawal-2 sawal concatenated (Jild 12 masala-289
    precedent); jawab = ansah-1 + ansah-2 concatenated; notes =
    concatenated in order; title from the head. Net -2 items.
  masala-94 (13-way): c07 head jild18-masala-94 [414-439] (sawal)
    + ALL 12 of c08's p-items [440-465] (sawals EMPTY) -> ONE item
    [414-465]; sawal = head only; jawab = all 13 parts concatenated;
    notes = concatenated in order; title from the head. Net -12.
  After the joins there are 126 items and 126 unique ids — the
  duplicate-id assertion runs AFTER the joins, not before.
  NO rename quirk in Jild 18's merge: the v3 merged file has 126
  UNIQUE ids. NO folds: every leaf item is written once, in its own
  leaf. Shared pages p.657 (masala-141 + masala-142) and pp.509-510
  (masala-113 + masala-114) sit WITHIN single leaves (c12, c09), so
  present pages sum == unique pages == 644. Source gaps in the DB
  itself (not merge gaps): pp. 8-92, 138, 254, 501.
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = "chunks-manifest-v4-jild18.json"

# Concatenation order: chunk files 01 -> 18, in order.
CHUNK_ORDER = ["jild-18-chunk-01.json", "jild-18-chunk-02.json", "jild-18-chunk-03.json", "jild-18-chunk-04.json", "jild-18-chunk-05.json", "jild-18-chunk-06.json", "jild-18-chunk-07.json", "jild-18-chunk-08.json", "jild-18-chunk-09.json", "jild-18-chunk-10.json", "jild-18-chunk-11.json", "jild-18-chunk-12.json", "jild-18-chunk-13.json"]

EXPECTED_ITEMS = 126
EXPECTED_PAGES = 644
EXPECTED_MIN_PAGE = 5
EXPECTED_MAX_PAGE = 736
EXPECTED_VOLUME_IDS = {46}
EXPECTED_FIRST_ID = "jild18-pesh-lafz"
EXPECTED_LAST_ID = "jild18-khatima"

# v3 ID sequence, verbatim from content/tashreeh/jild-18.json.
V3_IDS = ["jild18-pesh-lafz", "jild18-masala-1", "jild18-masala-2", "jild18-masala-3", "jild18-masala-4", "jild18-masala-5", "jild18-masala-6", "jild18-masala-7", "jild18-masala-8", "jild18-masala-9-12", "jild18-masala-13", "jild18-masala-14-15", "jild18-masala-16", "jild18-masala-17", "jild18-masala-18", "jild18-masala-18-19", "jild18-masala-20", "jild18-masala-21", "jild18-masala-22", "jild18-masala-23", "jild18-masala-24", "jild18-masala-25", "jild18-masala-26", "jild18-masala-27", "jild18-masala-28", "jild18-masala-29", "jild18-masala-30", "jild18-masala-31", "jild18-masala-32-35", "jild18-masala-36", "jild18-masala-37", "jild18-masala-38", "jild18-masala-39", "jild18-masala-40-41", "jild18-masala-42-43", "jild18-masala-44", "jild18-masala-45", "jild18-masala-46", "jild18-masala-47", "jild18-masala-48", "jild18-masala-49", "jild18-masala-50", "jild18-masala-51", "jild18-masala-52", "jild18-masala-53", "jild18-masala-54", "jild18-masala-55", "jild18-masala-56", "jild18-masala-57-58", "jild18-masala-59", "jild18-masala-60", "jild18-masala-61-62", "jild18-masala-63", "jild18-masala-64", "jild18-masala-65", "jild18-masala-66", "jild18-masala-67", "jild18-masala-68", "jild18-masala-69", "jild18-masala-70", "jild18-masala-71", "jild18-masala-72", "jild18-masala-73", "jild18-masala-74", "jild18-masala-75", "jild18-masala-76", "jild18-masala-77", "jild18-masala-78", "jild18-masala-79", "jild18-masala-80", "jild18-masala-81", "jild18-masala-86", "jild18-masala-83", "jild18-masala-84", "jild18-p359", "jild18-masala-87", "jild18-masala-88", "jild18-masala-90", "jild18-masala-92", "jild18-masala-93", "jild18-masala-94", "jild18-masala-95-101", "jild18-masala-102", "jild18-masala-103", "jild18-masala-104", "jild18-masala-105", "jild18-masala-106", "jild18-masala-107", "jild18-masala-108", "jild18-masala-109", "jild18-masala-110", "jild18-masala-111", "jild18-masala-112", "jild18-masala-113", "jild18-masala-114", "jild18-masala-115", "jild18-masala-116-118", "jild18-masala-119", "jild18-masala-120", "jild18-masala-121", "jild18-masala-122", "jild18-masala-123", "jild18-masala-124", "jild18-masala-125", "jild18-masala-126", "jild18-masala-127", "jild18-masala-128", "jild18-masala-129", "jild18-masala-130", "jild18-masala-131", "jild18-masala-132", "jild18-masala-133", "jild18-masala-134-137", "jild18-masala-138", "jild18-masala-139", "jild18-masala-140", "jild18-masala-141", "jild18-masala-142", "jild18-masala-143", "jild18-masala-144", "jild18-masala-145", "jild18-masala-146", "jild18-masala-150", "jild18-masala-151", "jild18-masala-152", "jild18-khatima"]

# v3 pages per item id, verbatim from content/tashreeh/jild-18.json.
V3_PAGES = {
    "jild18-pesh-lafz": [5, 6, 7],
    "jild18-masala-1": [93, 94],
    "jild18-masala-2": [94, 95, 96, 97, 98, 99, 100, 101, 102],
    "jild18-masala-3": [103, 104],
    "jild18-masala-4": [104, 105, 106],
    "jild18-masala-5": [106, 107, 108],
    "jild18-masala-6": [108, 109, 110],
    "jild18-masala-7": [111, 112, 113, 114, 115, 116],
    "jild18-masala-8": [116, 117, 118, 119, 120],
    "jild18-masala-9-12": [120, 121, 122],
    "jild18-masala-13": [122, 123],
    "jild18-masala-14-15": [123, 124],
    "jild18-masala-16": [124, 125],
    "jild18-masala-17": [125, 126],
    "jild18-masala-18": [126],
    "jild18-masala-18-19": [126, 127],
    "jild18-masala-20": [127, 128, 129, 130],
    "jild18-masala-21": [130, 131, 132, 133, 134, 135, 136],
    "jild18-masala-22": [136],
    "jild18-masala-23": [137],
    "jild18-masala-24": [139, 140],
    "jild18-masala-25": [140, 141, 142, 143, 144],
    "jild18-masala-26": [144, 145],
    "jild18-masala-27": [145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155],
    "jild18-masala-28": [155, 156, 157, 158, 159, 160, 161, 162],
    "jild18-masala-29": [163, 164, 165],
    "jild18-masala-30": [165],
    "jild18-masala-31": [165, 166, 167, 168, 169],
    "jild18-masala-32-35": [169, 170, 171, 172, 173, 174],
    "jild18-masala-36": [174, 175, 176, 177],
    "jild18-masala-37": [177, 178],
    "jild18-masala-38": [178, 179],
    "jild18-masala-39": [180, 181, 182],
    "jild18-masala-40-41": [182, 183],
    "jild18-masala-42-43": [183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194],
    "jild18-masala-44": [194, 195],
    "jild18-masala-45": [194, 195],
    "jild18-masala-46": [195, 196, 197, 198, 199, 200],
    "jild18-masala-47": [200, 201, 202, 203, 204, 205],
    "jild18-masala-48": [205, 206],
    "jild18-masala-49": [206, 207],
    "jild18-masala-50": [207, 208, 209, 210, 211, 212],
    "jild18-masala-51": [212, 213, 214],
    "jild18-masala-52": [214, 215, 216],
    "jild18-masala-53": [216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232],
    "jild18-masala-54": [232],
    "jild18-masala-55": [233, 234],
    "jild18-masala-56": [234, 235],
    "jild18-masala-57-58": [235, 236, 237],
    "jild18-masala-59": [237, 238, 239, 240, 241],
    "jild18-masala-60": [241, 242],
    "jild18-masala-61-62": [242, 243, 244, 245, 246],
    "jild18-masala-63": [246, 247, 248, 249, 250],
    "jild18-masala-64": [250, 251, 252],
    "jild18-masala-65": [252, 253],
    "jild18-masala-66": [255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276],
    "jild18-masala-67": [276, 277],
    "jild18-masala-68": [277, 278],
    "jild18-masala-69": [279, 280],
    "jild18-masala-70": [281, 282, 283, 284],
    "jild18-masala-71": [284, 285, 286],
    "jild18-masala-72": [286],
    "jild18-masala-73": [287],
    "jild18-masala-74": [287, 288],
    "jild18-masala-75": [288, 289],
    "jild18-masala-76": [289, 290],
    "jild18-masala-77": [290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301],
    "jild18-masala-78": [301, 302],
    "jild18-masala-79": [302, 303],
    "jild18-masala-80": [303, 304, 305],
    "jild18-masala-81": [306, 307, 308],
    "jild18-masala-86": [308, 309, 310, 311, 312, 313, 314, 315, 316, 317],
    "jild18-masala-83": [318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357],
    "jild18-masala-84": [358, 359],
    "jild18-p359": [359, 360, 361, 362, 363, 364],
    "jild18-masala-87": [364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377],
    "jild18-masala-88": [377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388],
    "jild18-masala-90": [389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404],
    "jild18-masala-92": [404, 405, 406, 407, 408, 409, 410],
    "jild18-masala-93": [410, 411, 412, 413, 414],
    "jild18-masala-94": [414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465],
    "jild18-masala-95-101": [465, 466, 467, 468, 469],
    "jild18-masala-102": [469, 470, 471],
    "jild18-masala-103": [471, 472],
    "jild18-masala-104": [472, 473],
    "jild18-masala-105": [473, 474],
    "jild18-masala-106": [474, 475],
    "jild18-masala-107": [475, 476, 477, 478, 479, 480, 481],
    "jild18-masala-108": [481, 482, 483],
    "jild18-masala-109": [483, 484],
    "jild18-masala-110": [484],
    "jild18-masala-111": [484, 485, 486],
    "jild18-masala-112": [487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500],
    "jild18-masala-113": [500, 502, 503, 504, 505, 506, 507, 508, 509, 510],
    "jild18-masala-114": [509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564],
    "jild18-masala-115": [564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578],
    "jild18-masala-116-118": [578, 579, 580, 581, 582, 583],
    "jild18-masala-119": [583, 584],
    "jild18-masala-120": [583, 584, 585],
    "jild18-masala-121": [585, 586],
    "jild18-masala-122": [586, 587],
    "jild18-masala-123": [587],
    "jild18-masala-124": [587, 588],
    "jild18-masala-125": [588, 589],
    "jild18-masala-126": [589, 590],
    "jild18-masala-127": [590, 591, 592, 593, 594],
    "jild18-masala-128": [594, 595, 596],
    "jild18-masala-129": [596, 597, 598],
    "jild18-masala-130": [598, 599],
    "jild18-masala-131": [599, 600],
    "jild18-masala-132": [600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611],
    "jild18-masala-133": [611, 612, 613, 614, 615, 616, 617],
    "jild18-masala-134-137": [617, 618, 619, 620],
    "jild18-masala-138": [620],
    "jild18-masala-139": [620, 621, 622],
    "jild18-masala-140": [622, 623, 624],
    "jild18-masala-141": [624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 655, 656, 657],
    "jild18-masala-142": [657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708],
    "jild18-masala-143": [708, 709],
    "jild18-masala-144": [709, 710, 711, 712, 713],
    "jild18-masala-145": [713, 714, 715, 716],
    "jild18-masala-146": [716, 717, 718, 719, 720, 721, 722, 723, 724, 725],
    "jild18-masala-150": [725, 726, 727, 728, 729, 730],
    "jild18-masala-151": [730, 731, 732],
    "jild18-masala-152": [732, 733, 734, 735, 736],
    "jild18-khatima": [736],
}

# The Urdu word for "page" is safha; the canonical marker is [safha N].
# Lenticular brackets are U+3010 (open) and U+3011 (close); the short
# form is "[safha-initial N]" with the single-letter abbreviation.
# All three patterns are built from escapes so this file never contains
# a literal lenticular-bracket pair.
SAFHA = "\u0635\u0641\u062d\u06c1"  # the Urdu word for page
RE_LENTICULAR = re.compile("\u3010" + SAFHA + " (\\d+)\u3011")
RE_SHORT = re.compile("\\[\u0635 (\\d+)\\]")
RE_FULL = re.compile("\\[" + SAFHA + " \\d+\\]")
LENTICULAR_PREFIX = "\u3010" + SAFHA  # for the "none remain" check
SHORT_PREFIX_RE = re.compile("\\[\u0635 \\d+\\]")

# Canonical output key order for every item (the JSON contract).
ITEM_KEYS = ["id", "title", "volume_id", "pages", "start_page",
             "tashreeh_sawal", "tashreeh_jawab", "notes"]


def normalise_markers(text):
    """Normalise page markers in one string. Returns (new_text, counts)."""
    counts = {"lenticular": 0, "short": 0, "full_before": 0}
    counts["full_before"] = len(RE_FULL.findall(text))
    text, n1 = RE_LENTICULAR.subn("[" + SAFHA + " \\1]", text)
    counts["lenticular"] = n1
    text, n2 = RE_SHORT.subn("[" + SAFHA + " \\1]", text)
    counts["short"] = n2
    return text, counts


def normalise_item(item):
    """Normalise markers in every string field of one item (recursively
    through lists/dicts). Returns per-form counts for this item."""
    totals = {"lenticular": 0, "short": 0, "full_before": 0}

    def walk(value):
        if isinstance(value, str):
            new, c = normalise_markers(value)
            for k in totals:
                totals[k] += c[k]
            return new
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in list(item.keys()):
        item[key] = walk(item[key])
    return totals


def main():
    assert len(V3_IDS) == EXPECTED_ITEMS, "embedded V3_IDS length mismatch"
    assert set(V3_PAGES) == set(V3_IDS), "embedded V3_PAGES keys != V3_IDS"
    assert sum(len(v) for v in V3_PAGES.values()) >= EXPECTED_PAGES

    manifest = json.load(open(os.path.join(BASE, MANIFEST), encoding="utf-8"))
    entries = manifest["chunks"]
    assert len(entries) == 13, f"expected 13 manifest entries, got {len(entries)}"
    assert all(e.get("status") == "done" for e in entries), "not all chunks done"
    by_file = {e["file"]: e for e in entries}
    assert set(by_file) == set(CHUNK_ORDER), (
        f"manifest files != CHUNK_ORDER: "
        f"missing={set(CHUNK_ORDER) - set(by_file)} "
        f"extra={set(by_file) - set(CHUNK_ORDER)}"
    )

    all_items = []
    derived = []
    for fname in CHUNK_ORDER:
        path = os.path.join(BASE, fname)
        d = json.load(open(path, encoding="utf-8"))
        items = d["masail"]
        entry = by_file[fname]
        assert len(items) == entry["items"], (
            f"{fname}: {len(items)} items, manifest says {entry['items']}"
        )
        # Marker normalisation, with before/after counts per chunk.
        before = {"lenticular": 0, "short": 0, "full_before": 0}
        for it in items:
            c = normalise_item(it)
            for k in before:
                before[k] += c[k]
        after_full = sum(
            len(RE_FULL.findall(json.dumps(it, ensure_ascii=False)))
            for it in items
        )
        print(
            f"{fname}: {len(items)} items | markers before: "
            f"lenticular={before['lenticular']} short={before['short']} "
            f"full={before['full_before']} | after: full={after_full}"
        )
        assert after_full == (
            before["lenticular"] + before["short"] + before["full_before"]
        ), f"{fname}: marker count changed by normalisation!"
        # Derive start_page = min(pages) where absent (none expected: all chunks carry start_page).
        for it in items:
            if "start_page" not in it:
                it["start_page"] = min(it["pages"])
                derived.append(it["id"])
        all_items.extend(items)

        # SIMPLE CONSECUTIVE-DUPLICATE JOINS (9): two parts each,
    # sawal only in the FIRST part; title from the first part,
    # asserted against the embedded merged-v3 title (V3_TITLES).
    # CROSS-ID JOINS (2): masala-90 (3-way, sawal = head + tail-2)
    # and masala-94 (13-way, sawal = head only).

    def join_consecutive(item_id, expected_parts, seam_checks):
        idxs = [i for i, it in enumerate(all_items) if it["id"] == item_id]
        assert len(idxs) == expected_parts, (
            f"{item_id}: expected {expected_parts} parts, found {len(idxs)}"
        )
        assert idxs == list(range(idxs[0], idxs[0] + expected_parts)), (
            f"{item_id}: parts not consecutive: {idxs}"
        )
        parts = [all_items[i] for i in idxs]
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{item_id}: volume mismatch {vols}"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{item_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{item_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        sawals = [p.get("tashreeh_sawal") for p in parts if p.get("tashreeh_sawal")]
        assert len(sawals) <= 1, f"{item_id}: {len(sawals)} non-empty sawals"
        if sawals:
            assert parts[0].get("tashreeh_sawal"), f"{item_id}: non-empty sawal not in first part"
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[item_id], f"{item_id}: joined pages != v3"
        title = parts[0]["title"]
        assert title == V3_TITLES[item_id], f"{item_id}: joined title != v3 title"
        joined = {
            "id": item_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawals[0] if sawals else "",
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        all_items[idxs[0]:idxs[0] + expected_parts] = [joined]
        print(f"joined {item_id}: {expected_parts} parts -> 1 item, {len(pages)} pages")

    def join_cross_id(head_id, tail_ids, sawal_mode, seam_checks):
        i = next(i for i, it in enumerate(all_items) if it["id"] == head_id)
        seq = [all_items[i + k]["id"] for k in range(1 + len(tail_ids))]
        assert seq == [head_id] + tail_ids, (
            f"{head_id}: cross-id sequence mismatch: {seq}"
        )
        parts = [all_items[i + k] for k in range(1 + len(tail_ids))]
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{head_id}: volume mismatch {vols}"
        if sawal_mode == "concat_head_tail2":
            assert parts[0].get("tashreeh_sawal"), f"{head_id}: head sawal empty"
            assert not parts[1].get("tashreeh_sawal"), f"{head_id}: tail-1 sawal must be empty"
            assert parts[2].get("tashreeh_sawal"), f"{head_id}: tail-2 sawal empty"
            sawal = parts[0]["tashreeh_sawal"].strip() + " " + parts[2]["tashreeh_sawal"].strip()
        elif sawal_mode == "head_only":
            assert parts[0].get("tashreeh_sawal"), f"{head_id}: head sawal empty"
            assert all(not p.get("tashreeh_sawal") for p in parts[1:]), (
                f"{head_id}: a tail sawal is non-empty"
            )
            sawal = parts[0]["tashreeh_sawal"]
        else:
            raise AssertionError(f"{head_id}: unknown sawal_mode {sawal_mode}")
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{head_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{head_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[head_id], f"{head_id}: joined pages != v3"
        title = parts[0]["title"]
        assert title == V3_TITLES[head_id], f"{head_id}: joined title != v3 title"
        joined = {
            "id": head_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        all_items[i:i + 1 + len(tail_ids)] = [joined]
        print(f"joined {head_id}: {1 + len(tail_ids)} parts -> 1 item, {len(pages)} pages")

    V3_TITLES = {
        "jild18-masala-42-43": "مسئلہ ۴۲ تا ۴۳ — زنا کے اقرار سے حرمت مصاہرت اور عالم کا فیصلہ",
        "jild18-masala-59": "بیوہ کا مہر اور مقروض کے مقروض پر دعویٰ",
        "jild18-masala-76": "مسئلہ ۷۶: گواہ موجود ہوں تو مدعا علیہ سے حلف کا مطالبہ (جواب جاری ہے)",
        "jild18-masala-83": "نکاح کے دعوے پر مفتی کے فیصلے کی تنقید — عصمت جہاں بیگم کا مقدمہ",
        "jild18-masala-112": "مسئلہ ۱۱۲: مشورے کے بعد صدر کو کثرتِ رائے کے خلاف اپنی رائے پر عمل کا اختیار (جواب جاری ہے — بقیہ اگلے حصے میں)",
        "jild18-masala-114": "مسئلہ ۱۱۴: غیر مسلم جج کا قاضی شرع ہونا — رسالہ الھبۃ الاحمدیۃ (پہلا حصہ)",
        "jild18-masala-126": "مسئلہ ۱۲۶: گزرگاہ کی زمین پر کھڑکی کھولنا اور پڑوسی کا اسے اپنے مکان میں ملا لینا",
        "jild18-masala-141": "مسئلہ ۱۴۱: تین طلاق کا دعویٰ — عدالت کی وجوہ نامقبولی کی جانچ (پہلا حصہ)",
        "jild18-masala-142": "مسئلہ ۱۴۲: بیع بالوفاء کا مقدمہ — رامپور (سوال اور جواب کا پہلا حصہ)",
        "jild18-masala-90": "مسئلہ ۹۰ تا ۹۱: مکان کی ہبہ اور قبضے کا دعویٰ (سوال — جواب اگلے حصے میں)",
        "jild18-masala-94": "مسئلہ ۹۴ — پیر صدر الدین کی اولاد کا نسب اور وراثت (جاری)",
    }

    join_consecutive(
        "jild18-masala-42-43", 2,
        [
            (0, "end", "س سے صحبت ہو چکی ہو۔ یہاں میرا حصہ ختم ہوتا ہے اور جواب صفحہ ۱۸۹ سے آگے جاری ہے۔"),
            (1, "start", "یہ مسئلہ پچھلے حصے سے چلا آ رہا ہے، جہاں ایک شخص نے اپنی ساس سے زنا کا اقرار کیا"),
        ],
    )
    join_consecutive(
        "jild18-masala-59", 2,
        [
            (0, "end", "ضدار شخص یہ اقرار کرے — (یہاں صفحہ 238 ختم ہوتا ہے، عبارت اگلے حصے میں جاری ہے۔)"),
            (1, "start", "یہ مسئلہ ۵۹ کے جواب کا بقیہ حصہ ہے جس میں اعلیٰ حضرت رحمۃ اللہ علیہ میت کے قرض ک"),
        ],
    )
    join_consecutive(
        "jild18-masala-76", 2,
        [
            (0, "end", "لب ہے سفر کی وہ مسافت جس پر قصر جائز ہو، — اور اس عبارت کا بقیہ اگلے حصے میں ہے۔"),
            (1, "start", "جواب کے اس بقیہ حصے میں پہلے اسی بات کی تکمیل ہے کہ گواہوں کے غائب ہونے کا معیار"),
        ],
    )
    join_consecutive(
        "jild18-masala-83", 2,
        [
            (0, "end", " مقرر، یعنی پہچانی ہوئی، تعیین کا مطلب ہے مقرر کرنا، اور چارہ کار کا مطلب ہے حل۔"),
            (1, "start", "یہ مسئلہ ۸۳ کے جواب کا بقیہ حصہ ہے۔ اس میں اعلیٰ حضرت رحمۃ اللہ علیہ فیصلے میں گ"),
        ],
    )
    join_consecutive(
        "jild18-masala-112", 2,
        [
            (0, "end", "روایات کیا ہے۔ جواب صفحہ ۴۸۹ پر جاری ہے — بقیہ اگلے حصے میں اسی id سے مکمل ہوگا۔"),
            (1, "start", "جواب کے اس حصے میں پہلے مشورے کی برکت اور کثرت رائے کی حیثیت بیان ہوئی ہے۔ مشورہ"),
        ],
    )
    join_consecutive(
        "jild18-masala-114", 2,
        [
            (0, "end", "اور نہ ہی کوئی ایسا حاکم جس کی طرف سے قاضی کی تقرری ہو سکے جیسا کہ بعض بلاد میں۔"),
            (1, "start", "یہ رسالے کا دوسرا حصہ ہے۔ اس میں پہلے یہ ثابت کیا گیا کہ قضائے شرعی کے لیے — شرع"),
        ],
    )
    join_consecutive(
        "jild18-masala-126", 2,
        [
            (0, "end", "نا اس پر واجب ہے — جواب کا باقی حصہ اگلے صفحے (۵۹۰) پر ہے، جو چنک ۱۱ میں آئے گا۔"),
            (1, "start", "یہ مسئلہ ۱۲۶ کے جواب کا بقیہ حصہ ہے۔ اس میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں"),
        ],
    )
    join_consecutive(
        "jild18-masala-141", 2,
        [
            (0, "end", " کا یہ حصہ ختم ہوتا ہے، آگے صفحہ ۶۴۰ سے تنبیہ اور اختلافات کا بقیہ جواب جاری ہے۔"),
            (1, "start", "اعلیٰ حضرت پہلے ایک تنبیہ فرماتے ہیں — تنبیہ کا مطلب ہے خبردار کرنا اور آگاہ کرن"),
        ],
    )
    join_consecutive(
        "jild18-masala-142", 2,
        [
            (0, "end", " رقم اخیر بھی مختلف ہے — مشاہدے کا مطلب ہے دیکھنا، رقم اخیر کا مطلب ہے آخری رقم۔"),
            (1, "start", "اس حصے میں اعلیٰ حضرت رحمۃ اللہ علیہ گواہیوں کے رد کی باقی وجوہ بیان فرماتے ہیں۔"),
        ],
    )
    join_cross_id(
        "jild18-masala-90",
        ["jild18-ansah-sawal-1", "jild18-ansah-sawal-2"],
        "concat_head_tail2",
        [
            (1, "end", "کور میراث کے طور پر وارثان رسول خاں پر تقسیم کیا جائے، فقط دستخط مولوی محمد جمل۔"),
            (2, "start", "جواب میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ اس مقدمے میں حکم شرعی کی تحقیق "),
        ],
    )
    join_cross_id(
        "jild18-masala-94",
        ["jild18-p440", "jild18-p442", "jild18-p444", "jild18-p446", "jild18-p447", "jild18-p450", "jild18-p452", "jild18-p454", "jild18-p456", "jild18-p459", "jild18-p461", "jild18-p463"],
        "head_only",
        [
            (0, "end", "اس حصے کی تشریح ختم ہوتی ہے اور بحث صفحہ چار سو چالیس سے چنک ۰۸ میں جاری رہے گی۔"),
            (1, "start", "یہ صفحات رسالے کی اسی بحث کا تسلسل ہیں جو پچھلے حصے سے چلی آ رہی ہے — بات یہ ہے "),
            (11, "end", " بات کہیں اور یہی شہادت دیں تو یہ شہادت قبول کر لی جائے گی جیسا کہ ذخیرہ میں ہے۔"),
            (12, "start", "نقطہ چالیس کے تتمے میں اسی عالمگیری کے صفحہ ایک سو تریپن پر اس کی تشریح یوں فرما"),
        ],
    )
    assert len(all_items) == len({it["id"] for it in all_items}), (
        f"DUPLICATE IDS after joins: "
        f"{[i for i in [it['id'] for it in all_items] if [it['id'] for it in all_items].count(i) > 1]}"
    )
    print("renames applied: [] (no duplicate masala numbers in Jild 18's merge; the 9 simple joins + masala-90 + masala-94 are joins, not renames)")

    print(f"after joins: {len(all_items)} items (149 concatenated)")
    print(f"start_page derived for {len(derived)} items (expected 0: all chunks carry start_page)")
    assert len(all_items) == EXPECTED_ITEMS, (
        f"expected {EXPECTED_ITEMS} items, got {len(all_items)}"
    )

    # Rebuild each item in the canonical key order (the JSON contract).
    canonical = []
    for it in all_items:
        assert set(it.keys()) == set(ITEM_KEYS), (
            f"{it.get('id')}: keys {sorted(it.keys())} != {sorted(ITEM_KEYS)}"
        )
        canonical.append({k: it[k] for k in ITEM_KEYS})
    all_items = canonical

    # Validation.
    ids = [it["id"] for it in all_items]
    assert ids == V3_IDS, (
        f"ID sequence != v3: first diff at "
        f"{next((i for i, (a, b) in enumerate(zip(ids, V3_IDS)) if a != b), min(len(ids), len(V3_IDS)))}"
    )
    assert len(ids) == len(set(ids)), (
        f"DUPLICATE IDS: {[i for i in ids if ids.count(i) > 1]}"
    )
    empty = [it["id"] for it in all_items if not it.get("tashreeh_jawab")]
    assert not empty, f"EMPTY JAWAB: {empty}"
    vols = set(it.get("volume_id") for it in all_items)
    assert vols == EXPECTED_VOLUME_IDS, f"volume_ids {vols} != {EXPECTED_VOLUME_IDS}"
    pages = set()
    for it in all_items:
        assert it["pages"] == V3_PAGES[it["id"]], (
            f"{it['id']}: pages {it['pages']} != v3 {V3_PAGES[it['id']]}"
        )
        pages.update(it["pages"])
    assert len(pages) == EXPECTED_PAGES, (
        f"expected {EXPECTED_PAGES} unique pages, got {len(pages)}"
    )
    assert min(pages) == EXPECTED_MIN_PAGE, f"min page {min(pages)}"
    assert max(pages) == EXPECTED_MAX_PAGE, f"max page {max(pages)}"
    exceptions = {
        it["id"] for it in all_items if it["start_page"] != min(it["pages"])
    }
    assert not exceptions, f"start_page != min(pages): {sorted(exceptions)}"
    assert ids[0] == EXPECTED_FIRST_ID, f"first id {ids[0]}"
    assert ids[-1] == EXPECTED_LAST_ID, f"last id {ids[-1]}"

    out = {"masail": all_items}
    out_text = json.dumps(out, ensure_ascii=False, indent=1)
    assert LENTICULAR_PREFIX not in out_text, "lenticular markers remain in output!"
    assert not SHORT_PREFIX_RE.search(out_text), "short markers remain in output!"
    total_markers = len(RE_FULL.findall(out_text))
    print(
        f"VALIDATION PASSED: {len(all_items)} items, {len(pages)} unique pages "
        f"(min {min(pages)}, max {max(pages)}), volume_ids {sorted(vols)}, "
        f"first {ids[0]}, last {ids[-1]}, start_page == min(pages) for all, "
        f"derived start_page for {len(derived)} items, markers {total_markers}"
    )

    with open(os.path.join(BASE, "jild-18.json"), "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"WROTE jild-18.json: {len(out_text.encode('utf-8'))} bytes")


if __name__ == "__main__":
    main()
