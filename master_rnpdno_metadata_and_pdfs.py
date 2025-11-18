#!/usr/bin/env python3
"""
rnpdno_full_scraper.py

Full Playwright scraper for RNPDNO (list view -> metadata -> modal -> PDF download).

Usage examples:
  # dry-run test (visible browser) pages 1..1
  python rnpdno_full_scraper.py --start-page 1 --end-page 1 --debug --dry-run

  # real run pages 1..100
  python rnpdno_full_scraper.py --start-page 1 --end-page 100

Notes:
 - Test small ranges first.
 - Ensure enough disk space for many PDFs.
 - Respect terms of service / ethics.
"""

import argparse
import asyncio
import csv
import json
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from fpdf import FPDF
from io import BytesIO
from PIL import Image

import aiofiles
from playwright.async_api import (
    async_playwright,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
)

# ---------------- Config ----------------
BASE_URL = "https://consultapublicarnpdno.segob.gob.mx/consulta"
OUTPUT_DIR = Path("rnpdno_pdfs")
METADATA_CSV = Path("metadata.csv")
LOG_CSV = Path("download_log.csv")

# Selectors
ROW_SELECTOR = "table tbody tr"
LIST_VIEW_BUTTON_TEXT = "Ver Lista"
MODAL_TRIGGER_SELECTOR_IN_ROW = "a[data-bs-toggle='modal']"  # inside each row
POPUP_SELECTOR = 'div.p-dialog[aria-modal="true"]'  # active PrimeNG modal
PDF_ANCHOR_SELECTOR = "a.icon-footer-modal-pdf"
PDF_ANCHOR_TEXT_SELECTOR = "text=Descargar PDF"

PAGINATOR_PAGE_BUTTONS_SELECTOR = "span.p-paginator-pages button.p-paginator-page"
PAGINATOR_CURRENT_SELECTOR = "span.p-paginator-pages button.p-highlight"

# runtime params
DEFAULT_TIMEOUT = 30
PAGE_DELAY = 0.5
ROW_DELAY = 0.12

PDF_API_KEYWORD = "/api/pdf/"

# ---------------- Helpers (I/O + logging) ----------------
def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_filename(s: str, maxlen: int = 120) -> str:
    s = re.sub(r"[^\w\-_\. ]", "_", s)
    return s[:maxlen]

def init_logs():
    if not LOG_CSV.exists():
        with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["page", "row_index", "name", "folio", "filename", "api_url", "status", "notes"])
    if not METADATA_CSV.exists():
        with open(METADATA_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "folio_unico","nombre","primer_apellido","segundo_apellido",
                "edad_actual","sexo","estatus_desaparicion","fecha_hechos",
                "entidad_hechos","informacion_reservada","boletin",
                "pdf_filename","api_url","page","row_index"
            ])

def append_log(page_num:int, row_idx:int, name:str, folio:str, filename:str, api_url:str, status:str, notes:str=""):
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([page_num, row_idx, name, folio, filename, api_url, status, notes])

def append_metadata_row(meta: dict):
    fieldnames = [
        "folio_unico","nombre","primer_apellido","segundo_apellido",
        "edad_actual","sexo","estatus_desaparicion","fecha_hechos",
        "entidad_hechos","informacion_reservada","boletin",
        "pdf_filename","api_url","page","row_index"
    ]
    write_header = not METADATA_CSV.exists() or METADATA_CSV.stat().st_size == 0
    with open(METADATA_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {k: meta.get(k, "") for k in fieldnames}
        writer.writerow(row)

def already_done(page_num:int, row_idx:int) -> bool:
    if not LOG_CSV.exists():
        return False
    with open(LOG_CSV, newline="", encoding="utf-8") as f:
        for r in csv.reader(f):
            if len(r) >= 7 and r[0].isdigit():
                try:
                    if int(r[0]) == page_num and int(r[1]) == row_idx and r[6] == "success":
                        return True
                except:
                    continue
    return False

async def save_bytes(path: Path, data: bytes):
    async with aiofiles.open(path, "wb") as f:
        await f.write(data)

# ---------------- Helpers: extraction ----------------
async def extract_metadata_from_row(row_handle) -> dict:
    """
    Given a Playwright element handle for <tr>, extract metadata from cells.
    Mapping based on the tr sample the user provided.
    """
    meta = {
        "folio_unico": "", "nombre": "", "primer_apellido": "", "segundo_apellido": "",
        "edad_actual": "", "sexo": "", "estatus_desaparicion": "", "fecha_hechos": "",
        "entidad_hechos": "", "informacion_reservada": "", "boletin": ""
    }
    try:
        tds = await row_handle.query_selector_all("td")
        values = []
        for td in tds:
            txt = await (await td.get_property("innerText")).json_value()
            values.append((txt or "").strip())
        # positional mapping taken from provided <tr>
        if len(values) >= 1: meta["folio_unico"] = values[0]
        if len(values) >= 2: meta["nombre"] = values[1]
        if len(values) >= 3: meta["primer_apellido"] = values[2]
        if len(values) >= 4: meta["segundo_apellido"] = values[3]
        if len(values) >= 5: meta["edad_actual"] = values[4]
        if len(values) >= 6: meta["sexo"] = values[5]
        if len(values) >= 7: meta["estatus_desaparicion"] = values[6]
        if len(values) >= 8: meta["fecha_hechos"] = values[7]
        if len(values) >= 9: meta["entidad_hechos"] = values[8]
        if len(values) >= 10: meta["informacion_reservada"] = values[9]
        if len(values) >= 11: meta["boletin"] = values[10]
    except Exception as e:
        print("Warning: extract_metadata_from_row failed:", e)
    return meta

# ---------------- Response capture helper ----------------
async def capture_pdf_from_response(resp: Response) -> Optional[bytes]:
    """
    Try to extract pdf bytes from a Playwright Response object (pdf bytes or JSON containing base64).
    """
    try:
        if resp is None:
            return None
        ct = resp.headers.get("content-type", "")
        if resp.status != 200:
            return None
        if "application/pdf" in ct:
            return await resp.body()
        if "json" in ct or "application/json" in ct:
            txt = await resp.text()
            try:
                obj = json.loads(txt)
            except:
                obj = None
            if obj:
                # search for long base64 string anywhere
                def find_b64(o):
                    if isinstance(o, str):
                        s = o.strip()
                        if len(s) > 200 and re.match(r"^[A-Za-z0-9+/=\s]+$", s):
                            return s
                        return None
                    if isinstance(o, dict):
                        for v in o.values():
                            r = find_b64(v)
                            if r:
                                return r
                    if isinstance(o, list):
                        for v in o:
                            r = find_b64(v)
                            if r:
                                return r
                    return None
                b64 = find_b64(obj)
                if b64:
                    import base64
                    try:
                        return base64.b64decode(b64)
                    except:
                        return None
    except Exception as e:
        print("capture_pdf_from_response error:", e)
    return None

# ---------------- Core: processing a row (CDP PDF, single-page, auto-scale) ----------------
async def process_row(page: Page, context, page_num: int, row_idx: int, dry_run: bool=False):
    """
    Processes a single row:
    - Detects 'CONFIDENCIAL' modal and skips if present
    - Opens normal modal
    - Waits for full content to render
    - Resizes modal to fit content
    - Captures PDF via CDP
    - Saves metadata and logs
    """
    # ----------------------------------------------------------
    # A) ROW ALREADY DONE?
    # ----------------------------------------------------------
    if already_done(page_num, row_idx):
        return "skipped", "already_logged"

    rows = await page.query_selector_all(ROW_SELECTOR)
    if row_idx >= len(rows):
        return "error", "row_index_out_of_bounds"

    row = rows[row_idx]

    # ----------------------------------------------------------
    # B) Extract metadata from row
    # ----------------------------------------------------------
    meta = await extract_metadata_from_row(row)
    name_text = meta.get("nombre") or "unknown"
    folio = meta.get("folio_unico") or ""
    safe_label = sanitize_filename(f"{name_text}_{folio}")
    out_path = OUTPUT_DIR / f"{safe_label}.pdf"

    # ----------------------------------------------------------
    # C) Click modal trigger
    # ----------------------------------------------------------
    try:
        trigger = await row.query_selector(MODAL_TRIGGER_SELECTOR_IN_ROW)
        if not trigger:
            trigger = await row.query_selector("a[data-bs-toggle='modal']")
        if not trigger:
            append_log(page_num, row_idx, name_text, folio, "", "", "error", "modal_trigger_not_found")
            meta.update({"pdf_filename": "", "api_url": "", "page": page_num, "row_index": row_idx})
            append_metadata_row(meta)
            return "error", "modal_trigger_not_found"
        await trigger.click()
    except Exception as e:
        append_log(page_num, row_idx, name_text, folio, "", "", "error", f"modal_click_failed:{e}")
        meta.update({"pdf_filename": "", "api_url": "", "page": page_num, "row_index": row_idx})
        append_metadata_row(meta)
        return "error", f"modal_click_failed:{e}"

    # ----------------------------------------------------------
    # D) Detect CONFIDENTIAL modal
    # ----------------------------------------------------------
    confidential_selector = "div.modal-content:has-text('CONFIDENCIAL')"
    try:
        confidential_modal = await page.wait_for_selector(confidential_selector, timeout=3000)
        if await confidential_modal.is_visible():
            try:
                close_btn = await confidential_modal.query_selector(".icono-modal-cerrar")
                if close_btn:
                    await close_btn.click()
                else:
                    await page.keyboard.press("Escape")
            except:
                pass
            append_log(page_num, row_idx, name_text, folio, "", "", "confidential", "record_confidential")
            meta.update({"pdf_filename": "", "api_url": "", "page": page_num, "row_index": row_idx})
            append_metadata_row(meta)
            await asyncio.sleep(ROW_DELAY)
            return "confidential", "no_pdf"
    except PlaywrightTimeoutError:
        pass

    # ----------------------------------------------------------
    # E) Wait for normal modal
    # ----------------------------------------------------------
    try:
        modal = await page.wait_for_selector(POPUP_SELECTOR, timeout=20_000)
    except PlaywrightTimeoutError:
        append_log(page_num, row_idx, name_text, folio, "", "", "error", "modal_not_visible")
        meta.update({"pdf_filename": "", "api_url": "", "page": page_num, "row_index": row_idx})
        append_metadata_row(meta)
        return "error", "modal_not_visible"

    # ----------------------------------------------------------
    # F) Wait for modal content to finish rendering
    # ----------------------------------------------------------
    try:
        await page.wait_for_function(
            """(el) => {
                const content = el.querySelector('.p-dialog-content');
                return content && content.scrollHeight > 0;
            }""",
            modal,
            timeout=5000
        )
        await asyncio.sleep(4.0)  # extra time for JS rendering
    except Exception:
        # fallback: wait fixed time if function fails
        await asyncio.sleep(4.0)

    # ----------------------------------------------------------
    # G) Resize modal to fit content
    # ----------------------------------------------------------
    try:
        await page.evaluate("""
            el => {
                const content = el.querySelector('.p-dialog-content');
                if (content) {
                    el.style.width = (content.scrollWidth + 40) + 'px';
                    el.style.height = (content.scrollHeight + 40) + 'px';
                    el.style.overflow = 'visible';
                }
            }
        """, modal)
        await asyncio.sleep(3.0)  # allow layout to stabilize
    except Exception as e:
        print(f"Warning: could not scale modal for PDF: {e}")

    # ----------------------------------------------------------
    # H) Capture PDF via CDP on a temporary page with only the modal
    # ----------------------------------------------------------
    try:
        # 1) Create a temporary new page for PDF rendering
        tmp_page = await context.new_page()
        # Clone modal content into tmp_page
        modal_html = await modal.evaluate("el => el.outerHTML")
        await tmp_page.set_content(modal_html, wait_until="load")
    
        # 2) Wait for modal content to render (dynamic)
        try:
            await tmp_page.wait_for_selector("div.modal-body", timeout=15_000)
            # Wait until modal height stabilizes
            last_height = 0
            stable_count = 0
            while stable_count < 3:
                height = await tmp_page.evaluate(
                    "() => document.querySelector('div.modal-body')?.scrollHeight || 0"
                )
                if height == last_height:
                    stable_count += 1
                else:
                    stable_count = 0
                    last_height = height
                await asyncio.sleep(0.3)
        except PlaywrightTimeoutError:
            append_log(page_num, row_idx, name_text, folio, "", "", "warning", "modal_content_may_not_be_fully_loaded")
    
        # 3) Resize modal to fit content
        await tmp_page.evaluate(
            """() => {
                const modal = document.querySelector('div.modal-body');
                if(modal){
                    modal.style.width = '800px';
                    modal.style.height = modal.scrollHeight + 'px';
                }
            }"""
        )
    
        # 4) Generate PDF via CDP
        pdf_bytes = await tmp_page.pdf(
            format="A4",
            print_background=True,
            margin={"top": "10px", "bottom": "10px", "left": "10px", "right": "10px"},
            prefer_css_page_size=False
        )
        final_api_url = "CDP_PDF"
    
        # Close temp page
        await tmp_page.close()
    
    except Exception as e:
        pdf_bytes = None
        append_log(page_num, row_idx, name_text, folio, "", "", "error", f"pdf_generation_failed:{e}")

    # ----------------------------------------------------------
    # I) Save PDF
    # ----------------------------------------------------------
    status = "error"
    if pdf_bytes:
        try:
            await save_bytes(out_path, pdf_bytes)
            append_log(page_num, row_idx, name_text, folio, str(out_path), "", "success", "")
            meta.update({"pdf_filename": str(out_path), "api_url": "", "page": page_num, "row_index": row_idx})
            append_metadata_row(meta)
            status = "success"
        except Exception as e:
            append_log(page_num, row_idx, name_text, folio, "", "", "error", f"write_failed:{e}")
    else:
        append_log(page_num, row_idx, name_text, folio, "", "", "error", "no_pdf_bytes")

    # ----------------------------------------------------------
    # J) Close modal
    # ----------------------------------------------------------
    try:
        cb = await modal.query_selector("img[alt='Cerrar']")
        if cb:
            await cb.click()
        else:
            await page.keyboard.press("Escape")
    except:
        pass

    await asyncio.sleep(ROW_DELAY)
    return status, "done"

# ---------------- Pagination helper ----------------
async def switch_to_list_view(page: Page):
    """
    Click 'Ver Lista' button if present to show the table rows.
    """
    try:
        btn = page.locator(f"button:has-text('{LIST_VIEW_BUTTON_TEXT}')")
        count = await btn.count()
        if count > 0:
            print("Switching to list view by clicking button...")
            await btn.first.click()
            # wait for rows to appear
            await page.wait_for_selector(ROW_SELECTOR, timeout=10*1000)
            await asyncio.sleep(0.5)
        else:
            # maybe already in list view
            # check rows existence
            if not await page.query_selector_all(ROW_SELECTOR):
                print("Warning: list-view button not found and no rows detected.")
    except Exception as e:
        print("Warning switching to list view:", e)

async def get_total_pages(page: Page) -> int:
    try:
        btns = await page.query_selector_all(PAGINATOR_PAGE_BUTTONS_SELECTOR)
        max_page = 1
        for b in btns:
            txt = (await (await b.get_property("innerText")).json_value()).strip()
            if txt.isdigit():
                iv = int(txt)
                if iv > max_page:
                    max_page = iv
        return max_page
    except Exception:
        return 1

# ---------------- Main ----------------
async def main(args):
    ensure_dirs()
    init_logs()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not args.debug, args=["--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(DEFAULT_TIMEOUT * 1000)

        print("Opening:", BASE_URL)
        await page.goto(BASE_URL, wait_until="networkidle")

        # Switch to list view if necessary
        await switch_to_list_view(page)

        # Wait for rows
        try:
            await page.wait_for_selector(ROW_SELECTOR, timeout=10*1000)
        except PlaywrightTimeoutError:
            print("Warning: rows not detected quickly; continuing")

        total_pages = await get_total_pages(page)
        print("Detected total pages (best-effort):", total_pages)
        endp = args.end_page if args.end_page else total_pages

        # iterate pages
        for pnum in range(args.start_page, endp + 1):
            print(f"--- Page {pnum} ---")
            # navigate to page pnum if paginator exists
            if pnum != 1:
                try:
                    btns = await page.query_selector_all(PAGINATOR_PAGE_BUTTONS_SELECTOR)
                    clicked = False
                    for b in btns:
                        txt = (await (await b.get_property("innerText")).json_value()).strip()
                        if txt.isdigit() and int(txt) == pnum:
                            await b.click()
                            clicked = True
                            await asyncio.sleep(1.0)
                            break
                    if not clicked:
                        # try clicking next until current shows pnum (or rely on server)
                        pass
                except Exception as e:
                    print("Paginator click problem:", e)

            # ensure rows loaded
            try:
                await page.wait_for_selector(ROW_SELECTOR, timeout=10*1000)
            except PlaywrightTimeoutError:
                print("No rows found on page", pnum)

            rows = await page.query_selector_all(ROW_SELECTOR)
            nrows = len(rows)
            print(f"Found {nrows} rows on page {pnum}")

            for r_idx in range(nrows):
                try:
                    status, note = await process_row(page, context, pnum, r_idx, dry_run=args.dry_run)
                    print(f"Page {pnum} row {r_idx}: {status} ({note})")
                except Exception as e:
                    print("Unhandled exception processing row:", e)
                    append_log(pnum, r_idx, "", "", "", "", "error", f"unhandled:{e}")

            await asyncio.sleep(args.page_delay)

        await context.close()
        await browser.close()
        print("Finished run.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=None)
    parser.add_argument("--debug", action="store_true", help="run visible browser")
    parser.add_argument("--dry-run", action="store_true", help="do not actually download PDFs")
    parser.add_argument("--page-delay", type=float, default=PAGE_DELAY)
    parser.add_argument("--row-delay", type=float, default=ROW_DELAY)
    args = parser.parse_args()
    asyncio.run(main(args))
