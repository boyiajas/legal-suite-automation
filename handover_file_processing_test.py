#!/usr/bin/env python3
import argparse
import datetime as dt
import fnmatch
import os
import re
import shutil
import sys
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import requests

from ftp_download_today import (
    FTP_HOST,
    FTP_PASS,
    FTP_USER,
    FTPClient,
    LEGALSUITE_API_BASE,
    LEGALSUITE_API_KEY,
)


CLIENT_CODE_MAP = {
    "STA387": "150307",
    "DR387": "334695",
    "STD9": "155128",
    "DRR9": "334565",
    "STA482": "209250",
    "DR482": "334568",
    "STA822": "283850",
    "DR822": "334567",
    "STA614": "267742",
    "DR614": "334569",
}

XLSX_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
}


@dataclass(frozen=True)
class DateContext:
    date_str: str
    month_year: str


@dataclass(frozen=True)
class DownloadTarget:
    label: str
    remote_dirs: tuple[str, ...]
    filename_pattern: str


class LegalSuiteLookupClient:
    def __init__(self, api_base: str, api_key: str) -> None:
        self._api_base = api_base.rstrip("/")
        self._api_key = api_key

    def get_matters_by_clientid_and_prefix(self, client_id: str, prefix: str) -> list[dict]:
        url = f"{self._api_base}/matter/get"
        data = [
            ("where[]", f"Matter.ClientID,=,{client_id}"),
            ("where[]", f"Matter.FileRef,like,{prefix}/%"),
        ]
        response = requests.post(url, headers=self._headers(), data=data, timeout=120)
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", [])

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/x-www-form-urlencoded",
        }


def resolve_date(date_arg: str | None, days_ago: int) -> DateContext:
    if date_arg:
        try:
            date_value = dt.datetime.strptime(date_arg, "%Y%m%d")
        except ValueError as exc:
            raise ValueError("Date must be in YYYYMMDD format.") from exc
    else:
        if days_ago < 0:
            raise ValueError("days_ago must be 0 or greater.")
        date_value = dt.datetime.now() - dt.timedelta(days=days_ago)

    return DateContext(
        date_str=date_value.strftime("%Y%m%d"),
        month_year=date_value.strftime("%b %Y"),
    )


def build_targets(date_ctx: DateContext) -> list[DownloadTarget]:
    return [
        DownloadTarget(
            label="Debt Review handover",
            remote_dirs=(
                "SBSA/Debt Review/Debt_Review_Handover_APT_LWS",
                "SBSA/Debt Review/Debt_Review_ Handover_APT_LWS",
            ),
            filename_pattern=f"Standard_Bank_Panel_L_Handover_{date_ctx.date_str}_DR.xlsx",
        ),
        DownloadTarget(
            label="Panel L handover",
            remote_dirs=(f"SBSA/Panel L/Handover_APT_LSW/{date_ctx.month_year}",),
            filename_pattern=f"*_{date_ctx.date_str}.xlsx",
        ),
    ]


def ensure_local_path(base_dir: str, remote_dir: str, filename: str) -> str:
    local_dir = os.path.join(base_dir, *remote_dir.split("/"))
    os.makedirs(local_dir, exist_ok=True)
    return os.path.join(local_dir, filename)


def download_handover_files(
    date_ctx: DateContext,
    download_dir: str,
    timeout: int,
) -> list[str]:
    downloaded_files: list[str] = []
    targets = build_targets(date_ctx)
    client = FTPClient(FTP_HOST, FTP_USER, FTP_PASS, timeout)

    print("Connecting to FTP...")
    client.connect()
    try:
        for target in targets:
            downloaded = False
            last_reason = "missing_dir"
            for remote_dir in target.remote_dirs:
                resolved_name, reason = client.resolve_remote_file(remote_dir, target.filename_pattern)
                if resolved_name:
                    remote_path = f"{remote_dir}/{resolved_name}"
                    local_path = ensure_local_path(download_dir, remote_dir, resolved_name)
                    client.download_file(remote_path, local_path)
                    downloaded_files.append(local_path)
                    downloaded = True
                    print(f"Downloaded {target.label}: {remote_path} -> {local_path}")
                    break
                if reason:
                    last_reason = reason

            if not downloaded:
                if last_reason == "missing_file":
                    print(f"Missing {target.label} file for {date_ctx.date_str}")
                else:
                    tried = ", ".join(target.remote_dirs)
                    print(f"Missing {target.label} directory on FTP: {tried}")
    finally:
        client.close()

    return downloaded_files


def resolve_local_handover_files(date_ctx: DateContext, download_dir: str) -> list[str]:
    resolved_files: list[str] = []

    for target in build_targets(date_ctx):
        found_path = None
        for remote_dir in target.remote_dirs:
            local_dir = os.path.join(download_dir, *remote_dir.split("/"))
            if not os.path.isdir(local_dir):
                continue

            try:
                names = sorted(
                    name for name in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, name))
                )
            except OSError:
                continue

            if any(ch in target.filename_pattern for ch in ["*", "?", "["]):
                matches = [name for name in names if fnmatch.fnmatch(name, target.filename_pattern)]
                if matches:
                    found_path = os.path.join(local_dir, matches[-1])
                    break
            elif target.filename_pattern in names:
                found_path = os.path.join(local_dir, target.filename_pattern)
                break

        if found_path:
            resolved_files.append(found_path)
            print(f"Using existing {target.label}: {found_path}")
        else:
            print(f"Existing {target.label} file not found for {date_ctx.date_str} in {download_dir}")

    return resolved_files


def normalize_header(value: object) -> str:
    if value is None:
        return ""
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def cell_value(row: tuple[object, ...], col_idx: int) -> object | None:
    if col_idx >= len(row):
        return None
    return row[col_idx]


def normalize_reference(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("'"):
        text = text[1:].strip()
    return text or None


def digits_only(value: object) -> str | None:
    if value in (None, ""):
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def find_column_index(header_row: tuple[object, ...], expected_name: str) -> int | None:
    expected_key = normalize_header(expected_name)
    for idx, value in enumerate(header_row):
        if normalize_header(value) == expected_key:
            return idx
    return None


def find_header_row(
    rows,
    expected_name: str,
    max_scan_rows: int = 10,
) -> tuple[tuple[object, ...], int, int]:
    scanned_rows: list[tuple[object, ...]] = []

    for row_number, row in enumerate(rows, start=1):
        row_tuple = tuple(row)
        scanned_rows.append(row_tuple)
        column_idx = find_column_index(row_tuple, expected_name)
        if column_idx is not None:
            return row_tuple, column_idx, row_number
        if row_number >= max_scan_rows:
            break

    sample_rows = [
        ", ".join(str(value or "") for value in row[:8])
        for row in scanned_rows[:3]
    ]
    sample_text = " | ".join(sample_rows) if sample_rows else "<no rows scanned>"
    raise ValueError(
        f"{expected_name} column not found in first {max_scan_rows} row(s). "
        f"Header sample: {sample_text}"
    )


def read_client_codes_from_file(path: str) -> tuple[dict[str, int], list[str], dict[str, list[str]]]:
    rows = iter_excel_rows(path)
    header_row, client_code_idx, header_row_number = find_header_row(rows, "Client Code")
    reference_idx = find_column_index(header_row, "Reference")
    if reference_idx is None:
        raise ValueError(f"Reference column not found in {path}")

    counts: dict[str, int] = {}
    unknown_codes: list[str] = []
    references_by_code: dict[str, list[str]] = {}
    for row in rows:
        raw_value = cell_value(row, client_code_idx)
        if raw_value in (None, ""):
            continue
        client_code = str(raw_value).strip().upper()
        if not client_code or normalize_header(client_code) == "clientcode":
            continue
        counts[client_code] = counts.get(client_code, 0) + 1
        reference = normalize_reference(cell_value(row, reference_idx))
        if reference:
            references = references_by_code.setdefault(client_code, [])
            if reference not in references:
                references.append(reference)
        if client_code not in CLIENT_CODE_MAP and client_code not in unknown_codes:
            unknown_codes.append(client_code)

    if header_row_number > 1:
        print(f"Found header row at line {header_row_number} in {path}")

    return counts, unknown_codes, references_by_code


def clean_handover_files(paths: list[str], download_dir: str, cleaned_dir: str) -> list[str]:
    cleaned_paths: list[str] = []

    for source_path in paths:
        rel_path = os.path.relpath(source_path, download_dir)
        destination_path = os.path.join(cleaned_dir, rel_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        clean_handover_file(source_path, destination_path)
        cleaned_paths.append(destination_path)
        print(f"Cleaned handover file: {source_path} -> {destination_path}")

    return cleaned_paths


def clean_handover_file(source_path: str, destination_path: str) -> None:
    try:
        from openpyxl import load_workbook
    except ImportError:
        shutil.copy2(source_path, destination_path)
        return

    workbook = load_workbook(source_path)
    try:
        if not workbook.worksheets:
            workbook.save(destination_path)
            return

        worksheet = workbook.worksheets[0]
        header_values: tuple[object, ...] | None = None
        header_row_number = None

        for row_number, row in enumerate(
            worksheet.iter_rows(min_row=1, max_row=min(10, worksheet.max_row), values_only=True),
            start=1,
        ):
            if find_column_index(tuple(row), "Client Code") is not None:
                header_values = tuple(row)
                header_row_number = row_number
                break

        if not header_values or not header_row_number:
            workbook.save(destination_path)
            return

        reference_idx = find_column_index(header_values, "Reference")
        account_idx = find_column_index(header_values, "Account number")

        for row_number in range(header_row_number + 1, worksheet.max_row + 1):
            reference_value = None
            if reference_idx is not None:
                reference_cell = worksheet.cell(row=row_number, column=reference_idx + 1)
                reference_value = normalize_reference(reference_cell.value)
                if reference_value is not None:
                    reference_cell.value = reference_value

            if account_idx is not None:
                account_cell = worksheet.cell(row=row_number, column=account_idx + 1)
                normalized_account = digits_only(account_cell.value)
                if not normalized_account and reference_value is not None:
                    normalized_account = digits_only(reference_value)
                if normalized_account is not None:
                    account_cell.value = normalized_account

        workbook.save(destination_path)
    finally:
        workbook.close()


def iter_excel_rows(path: str):
    if path.lower().endswith(".xlsx"):
        try:
            yield from iter_xlsx_rows_stdlib(path)
            return
        except (zipfile.BadZipFile, KeyError, ET.ParseError, ValueError):
            pass

    try:
        from openpyxl import load_workbook
    except ImportError:
        yield from iter_xlsx_rows_stdlib(path)
        return

    workbook = load_workbook(path, read_only=False, data_only=True)
    try:
        if not workbook.worksheets:
            return
        worksheet = workbook.worksheets[0]
        for row in worksheet.iter_rows(values_only=True):
            yield tuple(row)
    finally:
        workbook.close()


def iter_xlsx_rows_stdlib(path: str):
    with zipfile.ZipFile(path) as workbook_zip:
        shared_strings = read_shared_strings(workbook_zip)
        sheet_path = first_sheet_path(workbook_zip)
        sheet_root = ET.fromstring(workbook_zip.read(sheet_path))
        for row in sheet_root.findall("a:sheetData/a:row", XLSX_NS):
            yield read_xlsx_row(row, shared_strings)


def read_shared_strings(workbook_zip: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook_zip.namelist():
        return []

    root = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall("a:si", XLSX_NS):
        text = "".join(node.text or "" for node in item.findall(".//a:t", XLSX_NS))
        values.append(text)
    return values


def first_sheet_path(workbook_zip: zipfile.ZipFile) -> str:
    workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
    first_sheet = workbook_root.find("a:sheets/a:sheet", XLSX_NS)
    if first_sheet is None:
        raise ValueError("Workbook contains no sheets")

    rel_id = first_sheet.attrib.get(
        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    )
    if not rel_id:
        raise ValueError("Workbook sheet relationship missing")

    rel_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
    target = None
    for rel in rel_root:
        if rel.attrib.get("Id") == rel_id:
            target = rel.attrib.get("Target")
            break
    if not target:
        raise ValueError("Workbook sheet target missing")
    if target.startswith("xl/"):
        return target
    return f"xl/{target.lstrip('/')}"


def read_xlsx_row(row_node: ET.Element, shared_strings: list[str]) -> tuple[object, ...]:
    values: list[object] = []
    last_col = 0
    for cell in row_node.findall("a:c", XLSX_NS):
        ref = cell.attrib.get("r", "A1")
        col_idx = column_index(ref)
        while last_col + 1 < col_idx:
            values.append(None)
            last_col += 1
        values.append(read_xlsx_cell(cell, shared_strings))
        last_col = col_idx
    return tuple(values)


def read_xlsx_cell(cell_node: ET.Element, shared_strings: list[str]) -> object | None:
    cell_type = cell_node.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell_node.findall(".//a:t", XLSX_NS))

    value_node = cell_node.find("a:v", XLSX_NS)
    if value_node is None:
        return None

    value = value_node.text
    if cell_type == "s" and value is not None:
        return shared_strings[int(value)]
    if cell_type == "b":
        return value == "1"
    return value


def column_index(cell_ref: str) -> int:
    letters = ""
    for char in cell_ref:
        if char.isalpha():
            letters += char
        else:
            break

    result = 0
    for char in letters.upper():
        result = (result * 26) + (ord(char) - 64)
    return result


def find_latest_fileref(matters: list[dict], prefix: str) -> tuple[str | None, str]:
    best_number = 0
    best_ref = None
    best_width = 4
    pattern = re.compile(rf"^{re.escape(prefix)}/(\d+)$", re.IGNORECASE)

    for matter in matters:
        file_ref = str(matter.get("fileref") or "").strip()
        match = pattern.match(file_ref)
        if not match:
            continue
        digits = match.group(1)
        number = int(digits)
        if number >= best_number:
            best_number = number
            best_ref = file_ref
            best_width = max(4, len(digits))

    next_ref = f"{prefix}/{best_number + 1:0{best_width}d}"
    return best_ref, next_ref


def process_handover_files(paths: list[str], api_base: str, api_key: str) -> int:
    code_counts: dict[str, int] = {}
    unknown_codes: list[str] = []
    code_references: dict[str, list[str]] = {}

    for path in paths:
        file_counts, file_unknown_codes, file_references = read_client_codes_from_file(path)
        total_rows = sum(file_counts.values())
        print(f"Processed {path}: {total_rows} row(s), {len(file_counts)} unique client code(s)")
        for code, count in file_counts.items():
            code_counts[code] = code_counts.get(code, 0) + count
            refs = code_references.setdefault(code, [])
            for reference in file_references.get(code, []):
                if reference not in refs:
                    refs.append(reference)
        for code in file_unknown_codes:
            if code not in unknown_codes:
                unknown_codes.append(code)

    if not code_counts:
        print("No Client Code values found in the downloaded handover files.")
        return 0

    if unknown_codes:
        print("Unknown client codes in Excel:", ", ".join(sorted(unknown_codes)))

    client = LegalSuiteLookupClient(api_base, api_key)
    print("\nLegalSuite latest matter lookup:")
    for client_code in sorted(code_counts):
        client_id = CLIENT_CODE_MAP.get(client_code)
        if not client_id:
            print(f"- {client_code}: no client-id mapping found")
            continue

        matters = client.get_matters_by_clientid_and_prefix(client_id, client_code)
        latest_ref, next_ref = find_latest_fileref(matters, client_code)
        if latest_ref:
            print(
                f"- {client_code} -> clientid {client_id} | rows {code_counts[client_code]} | "
                f"latest {latest_ref} | next {next_ref}"
            )
        else:
            print(
                f"- {client_code} -> clientid {client_id} | rows {code_counts[client_code]} | "
                f"latest none | next {next_ref}"
            )
        references = code_references.get(client_code, [])
        if references:
            print(f"  Reference values: {', '.join(references)}")

    return 0


def normalize_cli_args(argv: list[str]) -> list[str]:
    normalized: list[str] = []
    for arg in argv:
        match = re.fullmatch(r"--days-(\d+)", arg)
        if match:
            normalized.extend(["--days-ago", match.group(1)])
            continue
        normalized.append(arg)
    return normalized


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Download handover files and display the next LegalSuite file ref per Client Code."
    )
    parser.add_argument("--date", help="Date in YYYYMMDD format.")
    parser.add_argument(
        "--days-ago",
        type=int,
        default=0,
        help="Download files from N days ago (default: 0). Also accepts shorthand like --days-15. Ignored if --date is provided.",
    )
    parser.add_argument(
        "--download-dir",
        default="downloads",
        help="Local base directory for downloads (default: downloads).",
    )
    parser.add_argument(
        "--cleaned-dir",
        default="cleaned_handover",
        help="Local base directory for cleaned handover files (default: cleaned_handover).",
    )
    parser.add_argument(
        "--clean-only",
        action="store_true",
        help="Skip FTP and use already-downloaded handover files for the selected date.",
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Use the selected handover files directly without creating cleaned working copies.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="FTP connection timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--api-base",
        default=LEGALSUITE_API_BASE,
        help="LegalSuite API base URL.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("LEGALSUITE_API_KEY") or LEGALSUITE_API_KEY,
        help="LegalSuite API key.",
    )
    return parser.parse_args(normalize_cli_args(argv))


def main() -> int:
    try:
        args = parse_args()
        date_ctx = resolve_date(args.date, args.days_ago)
        print(f"Target date: {date_ctx.date_str}")
        print(f"Panel month folder: {date_ctx.month_year}")
        if args.clean_only:
            source_files = resolve_local_handover_files(date_ctx, args.download_dir)
        else:
            source_files = download_handover_files(date_ctx, args.download_dir, args.timeout)

        if not source_files:
            print("No handover files were available.")
            return 1

        if args.skip_clean:
            working_files = source_files
        else:
            working_files = clean_handover_files(source_files, args.download_dir, args.cleaned_dir)

        return process_handover_files(working_files, args.api_base, args.api_key)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except requests.RequestException as exc:
        print(f"LegalSuite API error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
