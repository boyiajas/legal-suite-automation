# FTP Download Automation Notes

This script downloads LegalSuite FTP files for a given date, cleans them, and optionally updates closed matters or Matter ExtraScreen data in LegalSuite.

## How it works (step-by-step)

1. Resolve the target date.
   - Uses `--date YYYYMMDD` if provided.
   - Otherwise uses `--days-ago N` (default 0 = today).

2. Build the FTP target list.
   - Fills in date placeholders in each remote path and filename template.
   - Supports wildcard filenames (e.g., `*_{date}.xlsx`) and picks the newest match.

3. Connect to the FTP server and download files.
   - Logs missing directories or missing files.
   - Preserves the remote folder structure in `downloads/`.

4. Clean downloaded files (unless `--skip-clean`).
   - For every `.xlsx` in `downloads/`, creates a cleaned copy in `cleaned/`.
   - For `.csv`, converts to `.xlsx`, writes to `cleaned/`, and keeps the original CSV.
   - Also writes a header-preserving `.xlsx` copy next to the original CSV in `downloads/`.
   - Removes header rows from all files *except* claims files:
     - `Standard Bank Legal Claim Amount_Panel_L*.xlsx` keeps headers.
   - Cleaning rules:
     - `AccountNumber` -> digits only.
     - Claims file: clear the `Matter` column.
     - Handover files: copy `Reference` -> `AccountNumber` before cleaning.

5. Optional: Archive closed matters in LegalSuite (`--archive-closed`).
   - Reads the cleaned closed files:
     - `cleaned/SBSA/Panel L/Closed_APT_LSW/<Month Year>/*_{date}.xlsx`
     - `cleaned/SBSA/Debt Review/Debt_Review_Close_APT_LSW/Standard_Bank_Panel_L_Close_{date}_DR.xlsx`
   - Uses the **downloaded** file (with headers) to find the `File Reference` column.
   - For each File Reference:
     - Calls `matter/get` to fetch the matter.
     - Builds an update payload with all scalar fields + archive fields.
     - Calls `matter/update` (unless `--archive-dry-run`).
   - Defaults:
     - Employee ID = `1`
     - Archive Status = `2`
     - Archive Number = pulled from the matter response.

6. Optional: Update Matter ExtraScreen data (`--update-extrascreen`).
   - Reads feedback/PTP files from `cleaned/` for the target date.
   - Reads POC and summons file from `cleaned/SBSA POC AND SUMMONS/` for the target date.
   - Uses the **downloaded** file (with headers) to locate:
     - `File Reference`
     - `Desktop Extra ScreenID`
   - Maps the columns to `field1..field13` (feedback) or `field1..field29` (PTP).
   - Maps the POC/Summons columns:
     - `No. of Call Attempts` -> `field2`
     - `No of dispatched SMS's` -> `field3`
     - `No of dispatched Email's` -> `field4`
     - `No. of Broken PTPs` -> `field5`
   - Encodes date fields into LegalSuite integer date format.
     - Accepts Excel-style strings in `YYYY/MM/DD` or `DD/MM/YYYY`.
     - Strips any time portion (e.g., `2026/04/08 00:00:00`) before encoding.
   - Calls `matter/get` to find the record ID.
   - Calls `matdocsc/update` (unless `--extrascreen-dry-run`).
   - Use `--extrascreen-only feedback|ptp|poc-summons` to limit which extrascreen files run.

7. Optional: Update claim amounts (`--update-claim-amount`).
   - Reads claim files from `cleaned/Standard Bank_ClaimsAmount/` for the target date.
   - Uses `File Reference` and `Claim Amount` columns.
   - Calls `matter/get` to fetch the matter.
   - Updates only the `claimamount` field via `matter/update` (unless `--claim-amount-dry-run`).

8. Write a report log.
   - Default: `downloads/report_YYYYMMDD.txt`

## Common commands

- Download + clean for today:
  - `python3 ftp_download_today.py`

- Download for yesterday:
  - `python3 ftp_download_today.py --days-ago 1`

- Clean only (no FTP):
  - `python3 ftp_download_today.py --clean-only`
  - Reuses existing selected-date files already in `downloads/`

- Clean only with shorthand date offset:
  - `python3 ftp_download_today.py --days-28 --clean-only`

- Archive closed matters (dry-run):
  - `python3 ftp_download_today.py --clean-only --archive-closed --archive-dry-run`

- Archive closed matters (real update):
  - `python3 ftp_download_today.py --clean-only --archive-closed`

- Extrascreen update (dry-run):
  - `python3 ftp_download_today.py --clean-only --update-extrascreen --extrascreen-dry-run --extrascreen-verbose`

- Extrascreen update (real update):
  - `python3 ftp_download_today.py --clean-only --update-extrascreen --extrascreen-verbose`

- Extrascreen update (POC/Summons only, verbose):
  - `python3 ftp_download_today.py --clean-only --update-extrascreen --extrascreen-only poc-summons --extrascreen-verbose`

- Claim amount update (dry-run):
  - `python3 ftp_download_today.py --clean-only --update-claim-amount --claim-amount-dry-run --claim-amount-verbose`

- Claim amount update (real update):
  - `python3 ftp_download_today.py --clean-only --update-claim-amount --claim-amount-verbose`

- Run closed + summons + claims + debt review + panel l (using existing downloads):
  - `python3 ftp_download_today.py --clean-only --update-extrascreen --update-claim-amount --archive-closed`

- Run closed + summons + claims + debt review + panel l (fresh download):
  - `python3 ftp_download_today.py --update-extrascreen --update-claim-amount --archive-closed`

## Handover test script

- Standalone handover lookup test:
  - `python3 handover_file_processing_test.py --date 20260219`

- Use a date offset instead of a fixed date:
  - `python3 handover_file_processing_test.py --days-ago 3`
  - `python3 handover_file_processing_test.py --days-3`

- What it does:
  - Downloads the handover files from Debt Review and Panel L for the selected date.
  - Reads the `Client Code` column from the Excel files.
  - Maps the Excel client code to the LegalSuite `clientid`.
  - Fetches matching matters from LegalSuite and finds the highest `FileRef` suffix.
  - Prints the latest matter ref and the next ref that would be used.

- Notes:
  - It does not create or update matters in LegalSuite.
  - It includes a fallback for the current FTP Debt Review handover folder typo: `Debt_Review_ Handover_APT_LWS`.
