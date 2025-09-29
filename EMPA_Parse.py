#%%
import re
import json
import argparse
from collections import OrderedDict
import pandas as pd
import os
import csv
from pathlib import Path

KV_RE = re.compile(r'^\s*([^:]+?)\s*:\s*(.*)$')

def read_lines(path):
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        return [ln.rstrip('\n') for ln in f]

def is_kv_line(line):
    return bool(KV_RE.match(line))

def is_top_level_kv_line(line):
    """Return True if line looks like a top-level key:value (no leading indent)."""
    if not line:
        return False
    # only consider a KV a top-level KV if it has no leading whitespace
    return KV_RE.match(line) and (line[0] not in (' ', '\t'))

def split_section_lines(lines):
    # split into key:value and multiline sections (value may be empty)
    i = 0
    out = OrderedDict()
    n = len(lines)
    while i < n:
        ln = lines[i]
        m = KV_RE.match(ln)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            if val != '':
                out[key] = val
                i += 1
            else:
                # collect following lines until next top-level key:value or a 'DataSet/Point' table start
                i += 1
                block = []
                while i < n and not is_top_level_kv_line(lines[i]) and not lines[i].startswith('DataSet/Point'):
                    # include lines that may contain colons (e.g., "Si : 32.13%") as part of the block
                    block.append(lines[i])
                    i += 1
                out[key] = '\n'.join(block).strip()
        else:
            i += 1
    return out

def parse_standard_composition(block):
    """
    Parse blocks like:
      Wakefield = Si : 25.94%, O : 44.43%, ...
    Returns:
      {
        'standard_to_composition': { standard_name: {element: percent, ...}, ... },
        'element_to_standard': { element: standard_name, ... }
      }
    """
    out = {'standard_to_composition': {}, 'element_to_standard': {}}
    if not block:
        return out
    for ln in block.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        # guard against weird spacing around '='
        if '=' not in ln:
            continue
        name, rest = [x.strip() for x in ln.split('=', 1)]
        comps = {}
        # split by commas but avoid splitting inside things that are not element separators
        parts = [p.strip() for p in re.split(r',\s*(?=[A-Za-z])', rest) if p.strip()]
        for part in parts:
            # expect "El : value%" or "El : value"
            if ':' not in part:
                continue
            el, val = [p.strip() for p in part.split(':', 1)]
            val = val.rstrip('%').strip()
            try:
                comps[el] = float(val)
            except:
                comps[el] = val
            # map element -> standard (last occurrence wins)
            out['element_to_standard'][el] = name
        out['standard_to_composition'][name] = comps
    return out

def parse_calibration_block(block):
    # lines like "Mg ,Si : Other\Wakefield diopside_15kV... (Mg : 349.7 cps/nA, Si : 559.4 cps/nA)"
    out = {}
    if not block:
        return out
    for ln in block.splitlines():
        ln = ln.strip()
        if not ln: continue
        if ':' in ln:
            left, right = [x.strip() for x in ln.split(':',1)]
            elements = [e.strip() for e in left.split(',') if e.strip()]
            # try to extract path and parenthetical cps info
            path = right
            cps = {}
            m = re.search(r'\((.*)\)', right)
            if m:
                path = right[:m.start()].strip()
                inside = m.group(1)
                for part in re.split(r',\s*', inside):
                    if ':' in part:
                        el, val = [p.strip() for p in part.split(':',1)]
                        # remove units like cps/nA
                        val_num = re.sub(r'[^\d\.\-eE]', '', val)
                        try:
                            cps[el] = float(val_num)
                        except:
                            cps[el] = val
            for el in elements:
                out[el] = {'cal_file': path, 'cps_info': cps.get(el) if cps else None}
    return out

def parse_analysis_parameters(block):
    # attempt to parse tabular analysis parameters into list of dicts
    out = []
    if not block: return out
    lines = [ln for ln in block.splitlines() if ln.strip()]
    if not lines: return out
    # header guessed from first line if contains multiple column names
    header = re.split(r'\s{2,}|\t', lines[0].strip())
    for ln in lines[1:]:
        cols = re.split(r'\s{2,}|\t', ln.strip())
        # pad/truncate to header length
        if len(cols) < len(header):
            cols += [''] * (len(header) - len(cols))
        row = {header[i].strip(): cols[i].strip() for i in range(min(len(header), len(cols)))}
        out.append(row)
    return out


def parse_calibration_block(block):
    # lines like "Mg ,Si : Other\Wakefield diopside_15kV... (Mg : 349.7 cps/nA, Si : 559.4 cps/nA)"
    out = {}
    if not block:
        return out
    for ln in block.splitlines():
        ln = ln.strip()
        if not ln: continue
        if ':' in ln:
            left, right = [x.strip() for x in ln.split(':',1)]
            elements = [e.strip() for e in left.split(',') if e.strip()]
            # try to extract path and parenthetical cps info
            path = right
            cps = {}
            m = re.search(r'\((.*)\)', right)
            if m:
                path = right[:m.start()].strip()
                inside = m.group(1)
                for part in re.split(r',\s*', inside):
                    if ':' in part:
                        el, val = [p.strip() for p in part.split(':',1)]
                        # remove units like cps/nA
                        val_num = re.sub(r'[^\d\.\-eE]', '', val)
                        try:
                            cps[el] = float(val_num)
                        except:
                            cps[el] = val
            for el in elements:
                out[el] = {'cal_file': path, 'cps_info': cps.get(el) if cps else None}
    return out

def parse_standard_name_block(block):
    """
    Parse lines like:
      Mg ,Si ,Ca On Wakefield diopside
      Fe On RKFAYb7
    Returns { 'standard_to_elements': {std: [els]}, 'element_to_standard': {el: std} }
    """
    out = {'standard_to_elements': {}, 'element_to_standard': {}}
    if not block:
        return out
    for ln in block.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        m = re.match(r'^(.*?)\s+On\s+(.*)$', ln, re.IGNORECASE)
        if m:
            left, right = m.group(1).strip(), m.group(2).strip()
            # split elements by commas (handles "Mg ,Si ,Ca" and "Mg")
            elems = [e.strip() for e in re.split(r',\s*', left) if e.strip()]
            out['standard_to_elements'].setdefault(right, []).extend(elems)
            for e in elems:
                out['element_to_standard'][e] = right
        else:
            # fallback: if line looks like a single standard name, create empty list
            if ln:
                out['standard_to_elements'].setdefault(ln, [])
    return out

def parse_column_conditions(block):
    """
    Parse Column Conditions block. Returns:
      { 'conds': {'Cond 1': {'desc':desc, 'elements':[...]}}, 'element_to_condition': {el: 'Cond 1'} }
    Handles lines like:
      Cond 1 : 15keV 10nA , Cond 2 : 15keV 100nA
      , Cond 2 : Al Ka, Ca Ka, ...
    and comma-leading element lists.
    """
    out = {'conds': {}, 'element_to_condition': {}}
    if not block:
        return out
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    last_cond = None
    for ln in lines:
        # direct "Cond N : description"
        m = re.match(r'Cond\s*(\d+)\s*:\s*(.*)$', ln, re.IGNORECASE)
        if m:
            num = m.group(1)
            desc = m.group(2).strip().rstrip(',')
            key = f'Cond {num}'
            out['conds'].setdefault(key, {'desc': desc, 'elements': []})
            last_cond = key
            # if description itself contains "Cond X : elems", fall through to extract elements below
        # find any "Cond N : elem1, elem2, ..."
        found = re.findall(r'Cond\s*(\d+)\s*:\s*([^,]+(?:,\s*[^,]+)*)', ln, re.IGNORECASE)
        if found:
            for num, elems_str in found:
                key = f'Cond {num}'
                elems = [e.strip() for e in elems_str.split(',') if e.strip()]
                out['conds'].setdefault(key, {'desc': '', 'elements': []})['elements'].extend(elems)
                for e in elems:
                    out['element_to_condition'][e] = key
                last_cond = key
            continue
        # line may be a leading-comma element list for the last seen condition
        if ln.startswith(',') or (not ln.lower().startswith('cond') and last_cond):
            elems = [e.strip() for e in re.split(r',\s*', ln.lstrip(', ')) if e.strip()]
            out['conds'].setdefault(last_cond, {'desc': '', 'elements': []})['elements'].extend(elems)
            for e in elems:
                out['element_to_condition'][e] = last_cond
    return out

# ...existing code...

def parse_file(path):
    lines = read_lines(path)
    # only look at header part (before 'DataSet/Point' table). Find index of DataSet/Point and cut.
    ds_idx = next((i for i,ln in enumerate(lines) if ln.startswith('DataSet/Point')), len(lines))
    header_lines = lines[:ds_idx]
    sections = split_section_lines(header_lines)

    parsed = {}
    # simple copy of KV
    parsed.update(sections)

    # parse specific blocks (case-insensitive lookup)
    for k in list(sections.keys()):
        kl = k.lower()
        if kl.startswith('standard composition'):
            parsed['Standard composition parsed'] = parse_standard_composition(sections[k])
            break

    for k in list(sections.keys()):
        if k.lower().startswith('calibration file'):
            parsed['Calibration parsed'] = parse_calibration_block(sections[k])
            break

    for k in list(sections.keys()):
        if k.lower().startswith('analysis param'):
            parsed['Analysis Parameters parsed'] = parse_analysis_parameters(sections[k])
            break

    # New: parse 'Standard Name' mappings (case-insensitive)
    for k in list(sections.keys()):
        if k.lower().startswith('standard name'):
            parsed['Standard Name parsed'] = parse_standard_name_block(sections[k])
            break

    # New: parse 'Column Conditions' (case-insensitive)
    for k in list(sections.keys()):
        if k.lower().startswith('column conditions'):
            parsed['Column Conditions parsed'] = parse_column_conditions(sections[k])
            break

    return parsed

# Code for walking directories and writing to csv
def _safe_name(s):
    """Make a filename-safe string from a section name."""
    return re.sub(r'[^0-9A-Za-z._-]+', '_', s).strip('_')

def _is_primitive(v):
    return isinstance(v, (str, int, float, bool)) or v is None

def _write_section_csv_flat(section, rows, outpath):
    """Write rows where each row is {'file':..., **keys} and keys set is known."""
    fieldnames = ['file'] + sorted(k for k in rows[0].keys() if k != 'file')
    with open(outpath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def _write_section_csv_json(section, rows, outpath):
    """Write simple two-column CSV file + json value."""
    with open(outpath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['file', 'value'])
        writer.writeheader()
        for r in rows:
            writer.writerow({'file': r['file'], 'value': r.get('value', '')})

def walk_parse_and_export(root_dir, out_dir=None, exts=('.txt', '.qtidat', '.qtidat')):
    """
    Walk directories under root_dir, parse files with matching extensions,
    and save CSVs. Produces wide (column-per-element) CSVs for:
      - standard_by_element.csv  (file + one column per element -> standard name)
      - xtal_by_element.csv      (file + one column per element -> xtal/crystal used)
    Keeps a standard_compositions.csv (standard, element, value) as before.
    Returns parsed_by_file dict.
    """
    import csv
    import json
    import re
    from pathlib import Path

    root = Path(root_dir)
    if out_dir is None:
        out_dir = root / 'parsed_csvs'
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parsed_by_file = {}
    for p in root.rglob('*'):
        if p.is_file() and p.suffix.lower() in exts:
            try:
                parsed = parse_file(str(p))
            except Exception as e:
                print(f"Failed to parse {p}: {e}")
                continue
            parsed_by_file[str(p)] = parsed

    # --- Build wide standard_by_element table ---
    all_elements_for_standards = set()
    file_to_element_standard = {}  # file -> { element: standard }
    for fp, parsed in parsed_by_file.items():
        file_to_element_standard[fp] = {}
        # find the parsed standard-name structure regardless of exact key variant
        sn_block = None
        for k in parsed.keys():
            if k.lower().startswith('standard name'):
                sn_block = parsed[k]
                break
        if not sn_block:
            # some files may have element_to_standard directly at top-level
            if 'element_to_standard' in parsed and isinstance(parsed['element_to_standard'], dict):
                sn_block = {'element_to_standard': parsed['element_to_standard']}
        if sn_block and isinstance(sn_block, dict):
            etos = sn_block.get('element_to_standard') or {}
            for el, std in etos.items():
                file_to_element_standard[fp][el] = std
                all_elements_for_standards.add(el)

    # write wide CSV for standards
    std_fields = ['file'] + sorted(all_elements_for_standards)
    with open(out_dir / 'standard_by_element.csv', 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=std_fields)
        w.writeheader()
        for fp in sorted(parsed_by_file.keys()):
            row = {'file': fp}
            esm = file_to_element_standard.get(fp, {})
            for el in all_elements_for_standards:
                row[el] = esm.get(el, '')
            w.writerow(row)

    # --- Build wide xtal_by_element table (best-effort from Analysis Parameters) ---
    all_elements_for_xtal = set()
    file_to_element_xtal = {}  # file -> { element: xtal }
    for fp, parsed in parsed_by_file.items():
        file_to_element_xtal[fp] = {}
        ap_block = None
        for k in parsed.keys():
            if k.lower().startswith('analysis param'):
                ap_block = parsed[k]
                break
        if not ap_block:
            # also accept 'Analysis Parameters parsed' produced earlier
            ap_block = parsed.get('Analysis Parameters parsed') or ap_block
        if ap_block and isinstance(ap_block, list):
            for row in ap_block:
                element = None
                xtal = None
                for key, val in row.items():
                    kl = key.lower()
                    if kl in ('element', 'el', 'analyte', 'name'):
                        element = val
                    if 'xtal' in kl or 'cryst' in kl or 'crystal' in kl:
                        xtal = val
                # fallback: first non-empty column looks like element
                if not element:
                    for v in row.values():
                        if v:
                            element = v
                            break
                if element:
                    file_to_element_xtal[fp][element] = xtal or ''
                    all_elements_for_xtal.add(element)

    xtal_fields = ['file'] + sorted(all_elements_for_xtal)
    with open(out_dir / 'xtal_by_element.csv', 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=xtal_fields)
        w.writeheader()
        for fp in sorted(parsed_by_file.keys()):
            row = {'file': fp}
            emap = file_to_element_xtal.get(fp, {})
            for el in all_elements_for_xtal:
                row[el] = emap.get(el, '')
            w.writerow(row)

    # --- standard compositions (unchanged shape) ---
    standard_compositions = {}
    for fp, parsed in parsed_by_file.items():
        sc_block = None
        for k in parsed.keys():
            if k.lower().startswith('standard composition'):
                sc_block = parsed[k]
                break
        if sc_block:
            std_map = {}
            if isinstance(sc_block, dict) and 'standard_to_composition' in sc_block:
                std_map = sc_block.get('standard_to_composition', {})
            elif isinstance(sc_block, dict):
                std_map = sc_block
            for std, comps in std_map.items():
                if isinstance(comps, dict):
                    standard_compositions.setdefault(std, {}).update(comps)

    std_comp_rows = []
    for std, comps in standard_compositions.items():
        for el, val in comps.items():
            std_comp_rows.append({'standard': std, 'element': el, 'value': '' if val is None else val})
    if std_comp_rows:
        with open(out_dir / 'standard_compositions.csv', 'w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['standard', 'element', 'value'])
            w.writeheader()
            for r in std_comp_rows:
                w.writerow(r)

    return parsed_by_file
#%%
def main():
    ap = argparse.ArgumentParser(description='Parse Camca EMPA header metadata to JSON.')
    ap.add_argument('infile', nargs='?', help='path to .txt or .qtiDat file (file or directory)')
    ap.add_argument('-o','--out', help='output json file (defaults to stdout)')
    ap.add_argument('--dir', action='store_true', help='treat infile as directory and export CSVs (creates parsed_csvs subdir by default)')
    args = ap.parse_args()

    if args.dir:
        if not args.infile:
            ap.error('--dir requires an infile directory path')
        parsed_map = walk_parse_and_export(args.infile)
        # also optionally write a combined JSON summary
        summary_path = Path(args.infile) / 'parsed_csvs' / 'parsed_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_map, f, indent=2, ensure_ascii=False)
        print(f"Exported section CSVs to: {Path(args.infile) / 'parsed_csvs'}")
        return

    if not args.infile:
        ap.error('infile required (file path)')

    parsed = parse_file(args.infile)
    if args.out:
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(parsed, f, indent=2, ensure_ascii=False)
    else:
        print(json.dumps(parsed, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()
# %%
