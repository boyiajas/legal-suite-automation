#!/usr/bin/env python3
import os
import zipfile
from xml.sax.saxutils import escape


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "Handover file process flow2.docx")


CONTENT_TYPES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""


ROOT_RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""


DOCUMENT_RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""


APP_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
</Properties>
"""


CORE_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Handover Process Flow 2</dc:title>
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
</cp:coreProperties>
"""


STYLES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:docDefaults>
    <w:rPrDefault>
      <w:rPr>
        <w:rFonts w:ascii="Arial" w:hAnsi="Arial" w:cs="Arial"/>
        <w:sz w:val="22"/>
        <w:szCs w:val="22"/>
      </w:rPr>
    </w:rPrDefault>
  </w:docDefaults>
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal">
    <w:name w:val="Normal"/>
    <w:qFormat/>
    <w:pPr>
      <w:spacing w:after="100" w:line="276" w:lineRule="auto"/>
    </w:pPr>
    <w:rPr>
      <w:rFonts w:ascii="Arial" w:hAnsi="Arial" w:cs="Arial"/>
      <w:sz w:val="22"/>
      <w:szCs w:val="22"/>
      <w:color w:val="1F2937"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Title">
    <w:name w:val="Title"/>
    <w:basedOn w:val="Normal"/>
    <w:qFormat/>
    <w:pPr>
      <w:spacing w:after="180"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="0F172A"/>
      <w:sz w:val="36"/>
      <w:szCs w:val="36"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/>
    <w:basedOn w:val="Normal"/>
    <w:qFormat/>
    <w:pPr>
      <w:spacing w:before="220" w:after="120"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="0F4C81"/>
      <w:sz w:val="28"/>
      <w:szCs w:val="28"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/>
    <w:basedOn w:val="Normal"/>
    <w:qFormat/>
    <w:pPr>
      <w:spacing w:before="160" w:after="80"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="1D3557"/>
      <w:sz w:val="24"/>
      <w:szCs w:val="24"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/>
    <w:basedOn w:val="Normal"/>
    <w:qFormat/>
    <w:pPr>
      <w:spacing w:before="120" w:after="60"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="334155"/>
      <w:sz w:val="22"/>
      <w:szCs w:val="22"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="CodeBlock">
    <w:name w:val="CodeBlock"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr>
      <w:spacing w:before="40" w:after="40"/>
      <w:ind w:left="260" w:right="260"/>
      <w:shd w:val="clear" w:color="auto" w:fill="EAF1FB"/>
    </w:pPr>
    <w:rPr>
      <w:rFonts w:ascii="Consolas" w:hAnsi="Consolas" w:cs="Consolas"/>
      <w:color w:val="0B1220"/>
      <w:sz w:val="18"/>
      <w:szCs w:val="18"/>
    </w:rPr>
  </w:style>
</w:styles>
"""


def preserve(text: str) -> str:
    return escape(text)


def paragraph(text: str, style: str = "Normal", bold: bool = False) -> str:
    run_pr = "<w:b/>" if bold else ""
    return (
        "<w:p>"
        f'<w:pPr><w:pStyle w:val="{style}"/></w:pPr>'
        f"<w:r><w:rPr>{run_pr}</w:rPr><w:t xml:space=\"preserve\">{preserve(text)}</w:t></w:r>"
        "</w:p>"
    )


def bullet(text: str) -> str:
    return paragraph(f"• {text}")


def code_block(text: str) -> str:
    lines = text.rstrip("\n").splitlines()
    return "".join(paragraph(line or " ", style="CodeBlock") for line in lines)


def cell_paragraph(text: str, bold: bool = False) -> str:
    run_pr = "<w:b/>" if bold else ""
    return (
        "<w:p>"
        "<w:pPr><w:spacing w:after=\"40\"/></w:pPr>"
        f"<w:r><w:rPr>{run_pr}</w:rPr><w:t xml:space=\"preserve\">{preserve(text)}</w:t></w:r>"
        "</w:p>"
    )


def table(rows: list[list[str]], widths: list[int]) -> str:
    tbl = [
        "<w:tbl>",
        "<w:tblPr>",
        "<w:tblW w:w=\"0\" w:type=\"auto\"/>",
        "<w:tblBorders>",
        "<w:top w:val=\"single\" w:sz=\"8\" w:color=\"9FB3C8\"/>",
        "<w:left w:val=\"single\" w:sz=\"8\" w:color=\"9FB3C8\"/>",
        "<w:bottom w:val=\"single\" w:sz=\"8\" w:color=\"9FB3C8\"/>",
        "<w:right w:val=\"single\" w:sz=\"8\" w:color=\"9FB3C8\"/>",
        "<w:insideH w:val=\"single\" w:sz=\"6\" w:color=\"C7D2E0\"/>",
        "<w:insideV w:val=\"single\" w:sz=\"6\" w:color=\"C7D2E0\"/>",
        "</w:tblBorders>",
        "</w:tblPr>",
    ]
    for row_index, row in enumerate(rows):
        tbl.append("<w:tr>")
        for col_index, value in enumerate(row):
            fill = "DCE9F7" if row_index == 0 else ("F8FAFC" if row_index % 2 == 0 else "FFFFFF")
            tbl.append("<w:tc>")
            tbl.append(
                "<w:tcPr>"
                f"<w:tcW w:w=\"{widths[col_index]}\" w:type=\"dxa\"/>"
                f"<w:shd w:val=\"clear\" w:color=\"auto\" w:fill=\"{fill}\"/>"
                "</w:tcPr>"
            )
            pieces = value.split("\n")
            for piece_index, piece in enumerate(pieces):
                tbl.append(cell_paragraph(piece, bold=row_index == 0 and piece_index == 0))
            tbl.append("</w:tc>")
        tbl.append("</w:tr>")
    tbl.append("</w:tbl>")
    return "".join(tbl)


def build_document_xml() -> str:
    blocks: list[str] = []

    blocks.append(paragraph("FTP Download Automation Process Flow", style="Title"))
    blocks.append(paragraph("Updated from current ftp_download_today.py behavior."))
    blocks.append(paragraph("Target document: Handover file process flow2.docx"))
    blocks.append(paragraph("Revision basis: script reviewed on 02 June 2026"))
    blocks.append(paragraph("This version replaces the older handover-only narrative and includes the current FTP, cleaning, handover, extrascreen, claim amount, archive, reopen, verification, reporting, SMTP, and FTP-upload features."))

    blocks.append(paragraph("1. High-Level Runtime Flow", style="Heading1"))
    blocks.append(code_block(
        "[Start]\n"
        "  |\n"
        "  v\n"
        "[Parse CLI arguments]\n"
        "  |\n"
        "  +--> [--send-report-log-only?] -- yes --> [Email existing daily log] --> [End]\n"
        "  |\n"
        "  no\n"
        "  |\n"
        "  v\n"
        "[Resolve target date from --date / --days-ago / --days-N]\n"
        "  |\n"
        "  v\n"
        "[Download from FTP or reuse existing downloads]\n"
        "  |\n"
        "  v\n"
        "[Clean files unless --skip-clean]\n"
        "  |\n"
        "  v\n"
        "[Process handover unless --skip-handover]\n"
        "  |\n"
        "  +--> [Update extrascreens if enabled]\n"
        "  +--> [Update claim amounts if enabled]\n"
        "  +--> [Archive closed matters if enabled]\n"
        "  +--> [Reopen matters if enabled]\n"
        "  |\n"
        "  v\n"
        "[Write verification workbooks]\n"
        "  |\n"
        "  v\n"
        "[Write report log and send completion email]\n"
        "  |\n"
        "  v\n"
        "[End]"
    ))

    blocks.append(paragraph("2. FTP Input Sources", style="Heading1"))
    blocks.append(table([
        ["FTP Folder", "Filename Pattern", "Purpose"],
        ["SBSA/Debt Review/Debt_Review_Close_APT_LSW", "Standard_Bank_Panel_L_Close_{date}_DR.xlsx", "Closed matter archive source (Debt Review)"],
        ["SBSA/Debt Review/Debt_Review_PTP_APT_LSW", "Standard_Bank_Panel_L_PTP_{date}_DR.xlsx", "Extrascreen PTP source (Debt Review)"],
        ["SBSA/Debt Review/Debt_Review_Feedback_APT_LSW", "Standard_Bank_Panel_L_Update_{date}_DR.xlsx", "Extrascreen feedback source (Debt Review)"],
        ["SBSA/Debt Review/Debt_Review_Reopen_APT_LSW", "Standard_Bank_Panel_L_Reopen_{date}_DR.xlsx", "Reopen source (Debt Review)"],
        ["SBSA/Debt Review/Debt_Review_Handover_APT_LWS", "Standard_Bank_Panel_L_Handover_{date}_DR.xlsx", "Handover source (Debt Review)"],
        ["SBSA/Panel L/PTP_APT_LSW/{month_year}", "*_{date}.xlsx", "Extrascreen PTP source (Panel L)"],
        ["SBSA/Panel L/Feedback_APT_LSW/{month_year}", "*_{date}.xlsx", "Extrascreen feedback source (Panel L)"],
        ["SBSA/Panel L/Handover_APT_LSW/{month_year}", "*_{date}.xlsx", "Handover source (Panel L)"],
        ["SBSA/Panel L/Closed_APT_LSW/{month_year}", "*_{date}.xlsx", "Closed matter archive source (Panel L)"],
        ["SBSA/Panel L/Reopen_APT_LSW/{month_year}", "*_{date}.xlsx", "Reopen source (Panel L)"],
        ["Standard Bank_ClaimsAmount", "Standard Bank Legal Claim Amount_Panel_L{year}_{month}_{day}_*.xlsx", "Claim amount update source"],
        ["SBSA POC AND SUMMONS", "{day}{month}{year}.csv", "POC and Summons extrascreen source"],
    ], [3500, 2500, 3000]))
    blocks.append(bullet("The handover debt-review folder supports an alias with an extra space: SBSA/Debt Review/Debt_Review_ Handover_APT_LWS."))
    blocks.append(bullet("Wildcard targets use the newest matching file."))
    blocks.append(bullet("--clean-only reuses the selected-date files already in downloads/ and does not connect to FTP."))

    blocks.append(paragraph("3. Cleaning Stage", style="Heading1"))
    blocks.append(table([
        ["Rule", "Current Behavior"],
        ["Excel files", "Saved into cleaned/ after business cleanup."],
        ["CSV files", "Converted to cleaned/.xlsx, original CSV kept, and a raw header-preserving .xlsx copy is also saved beside the CSV in downloads/."],
        ["AccountNumber", "Digits only."],
        ["Claim amount workbooks", "Keep the header row and blank the Matter column."],
        ["Handover workbooks", "Copy Reference into AccountNumber before cleaning and keep the header row."],
        ["Most other cleaned workbooks", "Header row is removed after cleaning."],
    ], [2600, 6400]))

    blocks.append(paragraph("4. Handover Matter Process", style="Heading1"))
    blocks.append(bullet("If --skip-clean is used, handover reads from downloaded workbooks. Otherwise it prefers cleaned handover workbooks and falls back to downloads if cleaned handover files are missing."))
    blocks.append(bullet("Two handover families are processed: Debt Review and Panel L."))
    blocks.append(bullet("Client Code values are mapped to LegalSuite ClientID values before FileRef sequencing starts."))
    blocks.append(table([
        ["Client Code", "Client ID", "Client Code", "Client ID"],
        ["STA387", "150307", "DR387", "334695"],
        ["STD9", "155128", "DRR9", "334565"],
        ["STA482", "209250", "DR482", "334568"],
        ["STA822", "283850", "DR822", "334567"],
        ["STA614", "267742", "DR614", "334569"],
    ], [1800, 1800, 1800, 1800]))
    blocks.append(code_block(
        "[Resolve handover files]\n"
        "  |\n"
        "  v\n"
        "[Read Client Code + Reference]\n"
        "  |\n"
        "  v\n"
        "[Lookup latest FileRef per client prefix]\n"
        "  |\n"
        "  v\n"
        "[For each row]\n"
        "  |\n"
        "  +--> unknown client code --> [Skip row]\n"
        "  |\n"
        "  v\n"
        "[Build next FileRef candidate]\n"
        "  |\n"
        "  v\n"
        "[Matter already exists?]\n"
        "  |\n"
        "  +--> yes --> [Resolve debtor party role 103] --> [Update party] --> [Update ParLang language 1] --> [Sync contacts] --> [Update handover extrascreens]\n"
        "  |\n"
        "  +--> no  --> [Create matter] --> [Update matter description/balances] --> [Create/reuse party]\n"
        "                  --> [Create MatParty if missing] --> [Resolve debtor party role 103]\n"
        "                  --> [Update ParLang language 1] --> [Sync contacts] --> [Update handover extrascreens]\n"
        "  |\n"
        "  v\n"
        "[Build handover report rows]\n"
        "  |\n"
        "  +--> [Dry-run?] -> no LegalSuite writes, no email, no FTP upload\n"
        "  |\n"
        "  v\n"
        "[Write handover report workbook]\n"
        "  |\n"
        "  +--> [Skip email?] -- yes --> [Upload report to FTP]\n"
        "  |\n"
        "  +--> [Email report] --> [Upload report to FTP]"
    ))
    blocks.append(bullet("Duplicate detection checks both the proposed FileRef and the row reference (TheirRef)."))
    blocks.append(bullet("Claim Amount is copied into claimamount, debtorsbalance, debtorsopeningbalance, interestonamount, and debtorscapitalbalance during handover matter creation."))
    blocks.append(bullet("The script can reuse an existing debtor party by ID number, create missing MatParty role 103 links, and create missing home, cell, work, and email contacts."))
    blocks.append(bullet("After the party step, the script explicitly fetches the ParLang row for partyid and languageid 1, then updates that row through parlang/update."))
    blocks.append(bullet("--handover-debug-stop-row dumps the payloads for the selected row and stops before the LegalSuite call for that row."))
    blocks.append(bullet("--handover-email-test builds a report preview for the test email recipients without creating LegalSuite records."))

    blocks.append(paragraph("4.1 Handover Matter Field Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "LegalSuite Field"],
        ["Reference", "theirref"],
        ["Claim Amount", "claimamount"],
        ["Interest Rate", "interestrate"],
        ["EmployerID", "employerid"],
        ["TracingAgentID", "tracingagentid"],
        ["SheriffAreaID", "sheriffareaid"],
        ["SheriffID", "sheriffid"],
        ["BranchID", "branchid"],
        ["EmployeeID", "employeeid"],
        ["StageGroupID", "stagegroupid"],
        ["MatterTypeID", "mattertypeid"],
        ["DebtorFeeSheetID", "debtorfeesheetid"],
        ["ClientFeeSheetID", "clientfeesheetid"],
        ["DebtorCollCommOption", "debtorcollcommoption"],
        ["DebtorCollCommPercent", "debtorcollcommpercent"],
        ["CollCommOption", "collcommoption"],
        ["ClientCollCommPercent", "clientcollcommpercent"],
        ["CostCentreID", "costcentreid"],
        ["DefendantEmail", "defendantemail"],
        ["MagCourtDistrict", "magcourtdistrict"],
        ["MagCourtHeldAt", "magcourtheldat"],
        ["ExtraScreenID", "extrascreenid"],
        ["In Duplum Amount", "induplumamount"],
        ["Maximum Interest Amount", "maximuminterestamount"],
        ["Alternate Reference", "alternateref"],
    ], [4300, 4700]))
    blocks.append(bullet("During matter creation, the script also derives description, partymatterprefix, internalcomment, dateinstructed, updatedbydate, and updatedbytime."))
    blocks.append(bullet("Claim Amount is also copied into debtorsbalance, debtorsopeningbalance, interestonamount, and debtorscapitalbalance."))

    blocks.append(paragraph("4.2 Handover Party and Contact Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "LegalSuite Party Field"],
        ["Debtor Surname + Debtor First Name", "name / parlang[name]"],
        ["Debtor Title", "parlang[salutation]"],
        ["ID Number", "identitynumber and parlang[identitynumber]"],
        ["Physical Address Line 1", "parlang[physicalline1]"],
        ["Physical Address Line 2", "parlang[physicalline2]"],
        ["Physical Address Line 3", "parlang[physicalline3]"],
        ["Physical Postal Code", "parlang[physicalcode]"],
        ["Postal Address Line 1", "parlang[postalline1]"],
        ["Postal Address Line 2", "parlang[postalline2]"],
        ["Postal Address Line 3", "parlang[postalline3]"],
        ["Postal Code", "parlang[postalcode]"],
    ], [4300, 4700]))
    blocks.append(table([
        ["Follow-up ParLang Update Source", "ParLang Field"],
        ["ID Number", "identitynumber"],
        ["Debtor First Name", "firstname"],
        ["Debtor Title", "title"],
        ["BirthDate / Birth Date / Date of Birth / DOB", "birthdate when present"],
    ], [4300, 4700]))
    blocks.append(table([
        ["Excel Column", "ParTele Type"],
        ["Telephone (Home)", "home / telephonetypeid 4"],
        ["Cell Phone", "cell / telephonetypeid 5"],
        ["Telephone (Work)", "work / telephonetypeid 8"],
        ["DefendantEmail", "email / telephonetypeid 7"],
    ], [4300, 4700]))

    blocks.append(paragraph("5. Extrascreen Update Process", style="Heading1"))
    blocks.append(table([
        ["File Type", "Source", "Current Behavior"],
        ["Feedback", "Debt Review and Panel L feedback workbooks", "Maps field1-field13 and updates matdocsc by FileRef and Desktop Extra ScreenID."],
        ["PTP", "Debt Review and Panel L PTP workbooks", "Maps field1-field29 and updates matdocsc by FileRef and Desktop Extra ScreenID."],
        ["POC/Summons", "SBSA POC AND SUMMONS CSV converted to XLSX", "Maps No. of Call Attempts to field2, dispatched SMS's to field3, dispatched Email's to field4, and Broken PTPs to field5."],
    ], [1800, 2800, 4600]))
    blocks.append(bullet("Row values are read from cleaned files, but header detection prefers the downloaded/original file because cleaned feedback/PTP files normally have their headers removed."))
    blocks.append(bullet("For CSV sources such as POC/Summons, the raw .xlsx copy in downloads/ preserves headers for later extrascreen processing."))
    blocks.append(bullet("--extrascreen-only feedback|ptp|poc-summons limits processing to one extrascreen family."))
    blocks.append(bullet("Verbose mode prints file type, payload, response, and verification output to the terminal."))
    blocks.append(paragraph("5.1 Feedback Extrascreen Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "ExtraScreen Field"],
        ["AccountNumber", "field1"],
        ["PTPCaptureDate", "field2"],
        ["PTPDueDate", "field3"],
        ["PTPAmount", "field4"],
        ["LastPaymentDate", "field5"],
        ["LastPaymentAmount", "field6"],
        ["LastQuickComment", "field7"],
        ["LastQuickCommentDate", "field8"],
        ["LastMemo", "field9"],
        ["LastMemoDate", "field10"],
        ["AccountClosedDate", "field11"],
        ["ReasonForClosure", "field12"],
        ["BranchID", "field13"],
    ], [4300, 4700]))
    blocks.append(bullet("The date fields in the feedback mapping are encoded into LegalSuite integer dates before submission."))

    blocks.append(paragraph("5.2 PTP Extrascreen Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "ExtraScreen Field"],
        ["AccountNumber", "field1"],
        ["PTPCaptureDate", "field2"],
        ["PTPDueDate", "field3"],
        ["PTPAmount", "field4"],
        ["LastPaymentDate", "field5"],
        ["BranchID", "field6"],
        ["LastQuickComment", "field7"],
        ["PTPAmount2", "field8"],
        ["PTPDueDate2", "field9"],
        ["PTPAmount3", "field10"],
        ["PTPDueDate3", "field11"],
        ["PTPAmount4", "field12"],
        ["PTPDueDate4", "field13"],
        ["PTPAmount5", "field14"],
        ["PTPDueDate5", "field15"],
        ["PTPAmount6", "field16"],
        ["PTPDueDate6", "field17"],
        ["PTPAmount7", "field18"],
        ["PTPDueDate7", "field19"],
        ["PTPAmount8", "field20"],
        ["PTPDueDate8", "field21"],
        ["PTPAmount9", "field22"],
        ["PTPDueDate9", "field23"],
        ["PTPAmount10", "field24"],
        ["PTPDueDate10", "field25"],
        ["PTPAmount11", "field26"],
        ["PTPDueDate11", "field27"],
        ["PTPAmount12", "field28"],
        ["PTPDueDate12", "field29"],
    ], [4300, 4700]))
    blocks.append(bullet("All PTP due-date and capture-date fields are date-encoded before submission."))

    blocks.append(paragraph("5.3 POC and Summons Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "ExtraScreen Field"],
        ["No. of Call Attempts", "field2"],
        ["No of dispatched SMS's", "field3"],
        ["No of dispatched Email's", "field4"],
        ["No. of Broken PTPs", "field5"],
    ], [4300, 4700]))

    blocks.append(paragraph("5.4 Handover Desktop ExtraScreen Mapping", style="Heading2"))
    blocks.append(bullet("Within handover files, the script looks for DesktopExtraScreenID1, DesktopExtraScreenID2, and DesktopExtraScreenID3."))
    blocks.append(bullet("For each detected screen ID, the row values in columns named Desktop Extra Field N are mapped to LegalSuite fieldN for that specific desktop extrascreen payload."))
    blocks.append(bullet("If a handover desktop extra field header contains date semantics, the value is LegalSuite date-encoded before update."))
    blocks.append(table([
        ["Date Encoding Rule", "Current Behavior"],
        ["LegalSuite offset", "36161"],
        ["Excel base date", "1899-12-30"],
        ["Accepted source text dates", "YYYY/MM/DD, DD/MM/YYYY, and equivalent dash-separated forms"],
        ["Time handling", "Time portions such as 2026/04/08 00:00:00 or ISO T values are stripped before encoding."],
        ["Numeric values", "Numeric Excel serials are shifted by the LegalSuite offset."],
    ], [2600, 6400]))

    blocks.append(paragraph("6. Claim Amount Update Process", style="Heading1"))
    blocks.append(code_block(
        "[Find claim amount workbook in cleaned/Standard Bank_ClaimsAmount]\n"
        "  |\n"
        "  v\n"
        "[Read File Ref and Claim Amount columns]\n"
        "  |\n"
        "  v\n"
        "[For each valid row]\n"
        "  |\n"
        "  v\n"
        "[GET matter by FileRef]\n"
        "  |\n"
        "  v\n"
        "[Build matter/update payload from existing matter]\n"
        "  |\n"
        "  v\n"
        "[Replace only claimamount]\n"
        "  |\n"
        "  +--> [Dry-run?] -- yes --> [Print payload only]\n"
        "  |\n"
        "  no\n"
        "  |\n"
        "  v\n"
        "[POST matter/update]\n"
        "  |\n"
        "  v\n"
        "[Re-fetch matter and verify claimamount]"
    ))
    blocks.append(bullet("Header detection supports normalized forms of File Ref and Claim Amount."))
    blocks.append(bullet("Money parsing strips commas and currency symbols before converting the value."))
    blocks.append(bullet("Terminal output now prints each FileRef that is updated or fails."))
    blocks.append(paragraph("6.1 Claim Amount Mapping", style="Heading2"))
    blocks.append(table([
        ["Excel Column", "LegalSuite Field / Use"],
        ["File Ref", "Matter lookup key via matter/get"],
        ["Claim Amount", "claimamount in matter/update payload"],
    ], [4300, 4700]))

    blocks.append(paragraph("7. Closed File Archive Process", style="Heading1"))
    blocks.append(bullet("Closed files are read from both cleaned Debt Review and cleaned Panel L locations."))
    blocks.append(bullet("FileRef values are collected from the original download when available; otherwise the cleaned file is used."))
    blocks.append(bullet("The script first attempts an archive update using archiveflag=1 and the configured archive status."))
    blocks.append(bullet("If LegalSuite rejects the archive, or if the fetched matter still comes back as Live, the script automatically falls back to Pending Deletion."))
    blocks.append(bullet("The final fetched values are verified and written into the verification workbook."))

    blocks.append(paragraph("8. Reopen Process", style="Heading1"))
    blocks.append(table([
        ["Field", "Reopen Value"],
        ["archiveflag", "0"],
        ["archivestatus", "0"],
        ["archiveno", "0"],
        ["archivestatusdescription", "Live"],
        ["archivedate", "blank"],
    ], [3200, 5800]))

    blocks.append(paragraph("9. Verification Workbook Process", style="Heading1"))
    blocks.append(bullet("The script creates verification copies under verification/ whenever a row-level LegalSuite update is verified."))
    blocks.append(bullet("Verification columns are appended dynamically, including status, timestamp, notes, and the GET response used for checking."))
    blocks.append(bullet("Extrascreen, claim amount, archive, and reopen branches all write verification results."))
    blocks.append(bullet("The verification workbook can be based on the downloaded/original file or the cleaned file, depending on which one still preserves the right row and header structure for traceability."))

    blocks.append(paragraph("10. Logging and Email Outputs", style="Heading1"))
    blocks.append(table([
        ["Output", "Current Behavior"],
        ["Daily report log", "Written to downloads/report_YYYYMMDD.txt unless overridden by --log-file."],
        ["At-a-glance summary", "Appended to the report log with enabled sections and the latest section summaries."],
        ["Completion log email", "If SMTP settings exist, the report log is emailed to helpdesk@iconis.co.za and dev@iconis.co.za."],
        ["Handover report email", "Sent only for the handover reporting branch when email is not skipped."],
        ["Handover FTP upload", "The handover report workbook is uploaded to FTP folder Matter Ref Updates."],
    ], [2600, 6400]))

    blocks.append(paragraph("11. Main Command-Line Features", style="Heading1"))
    blocks.append(table([
        ["Flag", "Meaning"],
        ["--clean-only", "Reuse only the existing selected-date files in downloads; do not connect to FTP."],
        ["--skip-clean", "Skip the cleaning stage."],
        ["--skip-handover", "Skip the handover matter, party, and MatParty branch."],
        ["--handover-dry-run", "Preview handover processing without creating or updating LegalSuite records."],
        ["--handover-email-test", "Generate a handover report preview for the test recipients."],
        ["--skip-handover-email", "Generate the handover report but do not email it; only upload it to FTP."],
        ["--update-extrascreen", "Run the extrascreen update branch."],
        ["--extrascreen-only poc-summons", "Run only the POC and Summons extrascreen branch."],
        ["--update-claim-amount", "Run the claim amount update branch."],
        ["--archive-closed", "Run the closed-file archive branch."],
        ["--reopen-matters", "Run the reopen branch."],
        ["--send-report-log-only", "Send the already-created daily log email for the selected date and exit."],
    ], [2800, 6200]))

    blocks.append(paragraph("12. Example Commands", style="Heading1"))
    blocks.append(table([
        ["Use Case", "Command"],
        ["Full selected-date processing from FTP", "python3 ftp_download_today.py --update-extrascreen --update-claim-amount --archive-closed --reopen-matters"],
        ["Reuse existing selected-date downloads only", "python3 ftp_download_today.py --clean-only --update-extrascreen --update-claim-amount --archive-closed --reopen-matters"],
        ["POC and Summons extrascreen only", "python3 ftp_download_today.py --clean-only --update-extrascreen --extrascreen-only poc-summons --extrascreen-verbose"],
        ["Claim amount dry-run with verbose payloads", "python3 ftp_download_today.py --clean-only --update-claim-amount --claim-amount-dry-run --claim-amount-verbose"],
        ["Handover dry-run with payload dump stop", "python3 ftp_download_today.py --clean-only --handover-dry-run --handover-debug-stop-row 2"],
    ], [3000, 6000]))

    blocks.append(paragraph("13. Operational Prerequisites", style="Heading1"))
    blocks.append(bullet("FTP_HOST, FTP_USER, and FTP_PASS are expected from environment variables."))
    blocks.append(bullet("LEGALSUITE_API_KEY is expected from the environment unless supplied on the command line."))
    blocks.append(bullet("SMTP settings are required for handover email and completion log email."))
    blocks.append(bullet("openpyxl is required for cleaning, claim updates, extrascreen updates, archive/reopen processing, and verification workbook output."))
    blocks.append(paragraph("In short: the current script is a multi-branch daily operations runner, not just a handover importer. It can reuse or download daily source files, normalize them, create or update LegalSuite records, verify the results back into workbook copies, and distribute operational evidence by log, email, and FTP.", bold=True))

    body = "".join(blocks)
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:wpc="http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas"
 xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
 xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math"
 xmlns:v="urn:schemas-microsoft-com:vml"
 xmlns:wp14="http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing"
 xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
 xmlns:w10="urn:schemas-microsoft-com:office:word"
 xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
 xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"
 xmlns:wpg="http://schemas.microsoft.com/office/word/2010/wordprocessingGroup"
 xmlns:wpi="http://schemas.microsoft.com/office/word/2010/wordprocessingInk"
 xmlns:wne="http://schemas.microsoft.com/office/2006/wordml"
 xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
 mc:Ignorable="w14 wp14">
  <w:body>
{body}
    <w:sectPr>
      <w:pgSz w:w="11906" w:h="16838"/>
      <w:pgMar w:top="1000" w:right="900" w:bottom="1000" w:left="900" w:header="708" w:footer="708" w:gutter="0"/>
    </w:sectPr>
  </w:body>
</w:document>
"""


def main() -> int:
    document_xml = build_document_xml()
    with zipfile.ZipFile(OUTPUT_PATH, "w", compression=zipfile.ZIP_DEFLATED) as docx:
        docx.writestr("[Content_Types].xml", CONTENT_TYPES_XML)
        docx.writestr("_rels/.rels", ROOT_RELS_XML)
        docx.writestr("docProps/app.xml", APP_XML)
        docx.writestr("docProps/core.xml", CORE_XML)
        docx.writestr("word/document.xml", document_xml)
        docx.writestr("word/styles.xml", STYLES_XML)
        docx.writestr("word/_rels/document.xml.rels", DOCUMENT_RELS_XML)
    print(OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
