# FTP Download Automation

`ftp_download_today.py` is the main daily automation script for:

- downloading LegalSuite source files from FTP
- cleaning and converting the downloaded files
- processing handover files
- updating matter extrascreens
- updating claim amounts
- archiving closed matters
- reopening matters from reopen files
- verifying that LegalSuite updates actually stuck after each update call

## Configuration

Credentials are loaded from `.env`.

Required keys:

```env
FTP_HOST=
FTP_USER=
FTP_PASS=
LEGALSUITE_API_KEY=
```

Use `.env.example` as the template. The real `.env` file is ignored by git.

Mail settings for handover report email:

```env
MAIL_MAILER=smtp
MAIL_HOST=
MAIL_PORT=2525
MAIL_USERNAME=
MAIL_PASSWORD=
MAIL_ENCRYPTION=tls
MAIL_FROM_ADDRESS=
```

The script also accepts the older `SMTP_*` names, but `MAIL_*` is the preferred format.

## Main script

Run the daily script:

```bash
python3 ftp_download_today.py
```

By default it:

1. resolves the target date
2. downloads all expected FTP files for that date
3. cleans the downloaded files into `cleaned/`
4. processes handover files unless `--skip-handover` is used
5. optionally runs extrascreen, claim amount, archive, and reopen updates when those flags are supplied
6. writes verification workbooks into `verification/` for processed update files
7. writes a report log to `downloads/report_YYYYMMDD.txt` unless `--log-file` is supplied

## Date selection

- Today:
  - `python3 ftp_download_today.py`
- Specific date:
  - `python3 ftp_download_today.py --date 20260430`
- Relative date:
  - `python3 ftp_download_today.py --days-ago 1`
- Shorthand relative date:
  - `python3 ftp_download_today.py --days-15`

`--date` overrides `--days-ago`.

## Downloaded file types

The script looks for these source groups:

- Debt Review close
- Debt Review PTP
- Debt Review feedback
- Debt Review reopen
- Debt Review handover
- Panel L PTP
- Panel L feedback
- Panel L handover
- Panel L closed
- Panel L reopen
- Standard Bank claim amount
- SBSA POC and summons

It preserves the remote folder structure under `downloads/`.

## Cleaning behavior

Cleaning runs unless `--skip-clean` is supplied.

What cleaning does:

- converts `.csv` files to `.xlsx`
- writes cleaned files into `cleaned/`
- strips non-digits from `AccountNumber`
- clears the `Matter` column in claim amount files
- for handover files, copies `Reference` into `AccountNumber` before digit cleanup
- preserves handover headers so handover processing can read the columns correctly
- removes the first row header from the other cleaned files where required by the older downstream logic

Useful modes:

- reuse existing downloaded files without FTP:
  - `python3 ftp_download_today.py --clean-only`
- reuse and skip cleaning:
  - `python3 ftp_download_today.py --clean-only --skip-clean`

Verification workbook output:

- default folder:
  - `verification/`
- override folder:
  - `python3 ftp_download_today.py --verification-dir audit_verification`

## Handover processing

Handover processing is already integrated into `ftp_download_today.py`.

It runs automatically unless you pass:

```bash
python3 ftp_download_today.py --skip-handover
```

What the handover flow does:

1. finds Debt Review and Panel L handover files for the selected date
2. reads `Client Code` and `Reference`
3. maps client codes to LegalSuite client IDs
4. fetches the latest LegalSuite file reference per client code
5. generates the next file reference
6. creates the matter
7. updates the matter description and related fields
8. creates or reuses the debtor party
9. creates or reuses the MatParty link
10. updates Desktop Extra Screen data from the handover row when present
11. generates an Excel report for newly created matters
12. emails the handover report with the Excel attached

Handover report columns:

- `Matter File Reference`
- `Their Reference`
- `Matter Description`

Live handover report recipients:

- To:
  - `tnxumalo@straussdaly.co.za`
  - `areddy@straussdaly.co.za`
  - `gharris@straussdaly.co.za`
  - `defbloem@straussdaly.co.za`
- Cc:
  - `agashnee.pillay@iconis.co.za`
  - `thileshnee.chinnasamy@iconis.co.za`

Handover options:

- dry-run only:
  - `python3 ftp_download_today.py --handover-dry-run`
- limit rows:
  - `python3 ftp_download_today.py --handover-create-limit 5`
- override handover employee ID:
  - `python3 ftp_download_today.py --handover-logged-in-employee-id 174`
- test report email mode:
  - `python3 ftp_download_today.py --handover-email-test`

`--handover-email-test` behavior:

- does not connect to LegalSuite
- does not check for existing matters
- does not create matters
- reads the handover rows directly
- generates a preview Excel report
- sends the report to test recipients only

Test recipients:

- To:
  - `dev@iconis.co.za`
- Cc:
  - `boyiajas@gmail.com`

## Matter extrascreen updates

Enable with:

```bash
python3 ftp_download_today.py --update-extrascreen
```

What it reads:

- cleaned feedback files
- cleaned PTP files
- cleaned POC and summons file

What it does:

- locates `FileRef`
- locates `Desktop Extra ScreenID`
- maps source columns to `field1..fieldN`
- encodes date fields into LegalSuite date integers
- fetches the matter record ID
- updates `matdocsc`
- fetches the extrascreen back
- compares returned values against the sent payload
- writes a verification copy of the processed workbook into `verification/`
- stores the fetched extrascreen row in that verification workbook

Important behavior:

- if there is no extrascreen ID, it skips
- if there is no extrascreen field data, it skips
- it only updates and verifies when both screen ID and field data exist
- if the file contains `PTPCaptureDate`, rows are processed from oldest to newest by that column before updates are sent

Options:

- dry-run:
  - `python3 ftp_download_today.py --update-extrascreen --extrascreen-dry-run`
- verbose:
  - `python3 ftp_download_today.py --update-extrascreen --extrascreen-verbose`
- limit to one file type:
  - `python3 ftp_download_today.py --update-extrascreen --extrascreen-only feedback`
  - `python3 ftp_download_today.py --update-extrascreen --extrascreen-only ptp`
  - `python3 ftp_download_today.py --update-extrascreen --extrascreen-only poc-summons`

## Claim amount updates

Enable with:

```bash
python3 ftp_download_today.py --update-claim-amount
```

What it does:

- reads the cleaned claim file for the selected date
- finds `FileRef` and `Claim Amount`
- fetches each matter
- updates `claimamount`
- fetches the matter again
- verifies that the fetched `claimamount` matches the payload
- writes a verification copy of the processed claim workbook into `verification/`
- stores the fetched matter data in that verification workbook

Options:

- dry-run:
  - `python3 ftp_download_today.py --update-claim-amount --claim-amount-dry-run`
- verbose:
  - `python3 ftp_download_today.py --update-claim-amount --claim-amount-verbose`

## Archive closed matters

Enable with:

```bash
python3 ftp_download_today.py --archive-closed
```

What it does:

- reads the cleaned closed files for the selected date
- collects `FileRef` values
- fetches each matter
- attempts to set the matter to `Archived`
- includes `actual`, `reserved`, `invested`, `transfer`, and `batchednormal` in the archive payload
- fetches the matter back to confirm the archive status
- writes a verification copy of each processed closed workbook into `verification/`
- stores the fetched archive result in that verification workbook

Archive fallback behavior:

- if LegalSuite returns an error like `You cannot archive a matter...`, the script updates the matter to `Pending Deletion`
- if LegalSuite accepts the update but the fetched matter still comes back as `Live`, the script also updates the matter to `Pending Deletion`
- after that fallback it fetches the matter again and prints the final returned archive fields

Options:

- dry-run:
  - `python3 ftp_download_today.py --archive-closed --archive-dry-run`
- verbose:
  - `python3 ftp_download_today.py --archive-closed --archive-verbose`
- override archive status:
  - `python3 ftp_download_today.py --archive-closed --archive-status 2`

## Reopen matters

Enable with:

```bash
python3 ftp_download_today.py --reopen-matters
```

What it does:

- reads the cleaned reopen files for the selected date
- collects `Matter Ref` or `FileRef` values
- fetches each matter
- updates the matter back to `Live`
- fetches the matter back again
- verifies the returned archive/live fields
- writes a verification copy of each processed reopen workbook into `verification/`

Options:

- dry-run:
  - `python3 ftp_download_today.py --reopen-matters --reopen-dry-run`
- verbose:
  - `python3 ftp_download_today.py --reopen-matters --reopen-verbose`

## Verification system

The script now verifies updates after they are made.

Verification currently exists for:

- handover matter description updates
- handover desktop extrascreen updates
- daily extrascreen updates
- claim amount updates
- archive updates
- reopen updates
- pending deletion fallback updates

Verification pattern:

1. send update payload
2. fetch the updated LegalSuite record
3. compare returned fields against the payload
4. write the fetched verification data into a copied workbook under `verification/`
5. print either verified fields or mismatch details

Verification workbook behavior for the file-based update flows:

- the script creates a copy of the processed source workbook inside `verification/`
- it preserves the original workbook content
- it appends verification columns such as status, notes, timestamp, fetched GET response, and returned field values
- it writes one verification row back to the matching Excel row that was processed

This keeps the terminal output, but also leaves a saved Excel audit trail of what LegalSuite returned during verification.

## Common commands

- Full daily run:
  - `python3 ftp_download_today.py`

- Yesterday’s full daily run:
  - `python3 ftp_download_today.py --days-ago 1`

- Reuse existing downloads and run the full daily flow:
  - `python3 ftp_download_today.py --clean-only`

- Full daily run except handover:
  - `python3 ftp_download_today.py --skip-handover --update-extrascreen --update-claim-amount --archive-closed --reopen-matters`

- Full daily run including handover, extrascreen, claims, archive, and reopen:
  - `python3 ftp_download_today.py --update-extrascreen --update-claim-amount --archive-closed --reopen-matters`

- Handover only, using existing files, preview mode:
  - `python3 ftp_download_today.py --clean-only --handover-dry-run --handover-create-limit 1`

- Handover email test only, using existing files and no LegalSuite writes:
  - `python3 ftp_download_today.py --date 20260319 --clean-only --handover-create-limit 1 --handover-dry-run --handover-email-test`

- Full live daily run including handover creation and live handover report email:
  - `python3 ftp_download_today.py --update-extrascreen --update-claim-amount --archive-closed --reopen-matters`

- Extrascreen only:
  - `python3 ftp_download_today.py --clean-only --skip-handover --update-extrascreen`

- Claims only:
  - `python3 ftp_download_today.py --clean-only --skip-handover --update-claim-amount`

- Archive only:
  - `python3 ftp_download_today.py --clean-only --skip-handover --archive-closed`

- Reopen only:
  - `python3 ftp_download_today.py --clean-only --skip-handover --reopen-matters`

## Output and logs

Console output shows:

- files downloaded or missing
- cleaning stage
- handover actions
- extrascreen actions
- claim updates
- archive updates
- reopen updates
- verification results
- verification workbook summary
- handover report generation
- handover email send status

Report log:

- default path:
  - `downloads/report_YYYYMMDD.txt`
- custom path:
  - `python3 ftp_download_today.py --log-file logs/report.txt`

## Notes

- Missing file means the FTP directory exists but the expected file for that date was not found.
- Missing directory means the target FTP folder itself was not present.
- If a source file type is missing, that section is skipped naturally.
- `--handover-email-test` is the safe way to test the report email path before using the live handover creation flow.
- `handover_file_processing_test.py` remains useful as a standalone test harness, but the main daily behavior should now be driven through `ftp_download_today.py`.
