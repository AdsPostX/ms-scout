"""
campaign_builder.py — MS Platform Campaign Builder
Automates the Create Offer form via Playwright (headless Chromium).

Setup (one-time):
  pip install playwright
  playwright install chromium
  python campaign_builder.py --login   ← opens browser, log in, saves session

Triggered by scout_bot.py when user says "@Scout launch this".
"""

import asyncio
import logging
import os
import pathlib
import sys
import tempfile
import threading
from datetime import datetime

import requests

log = logging.getLogger("campaign_builder")

MS_PLATFORM_URL = os.getenv("MS_PLATFORM_URL", "")
SESSION_FILE    = pathlib.Path(__file__).parent / "ms_session.json"

# ── In-memory approval state ──────────────────────────────────────────────────
_PENDING_APPROVALS: dict = {}  # thread_ts → {"brief_data": {...}, "copy": {...}}


def store_pending_approval(thread_ts: str, brief_data: dict, copy: dict):
    _PENDING_APPROVALS[thread_ts] = {"brief_data": brief_data, "copy": copy}


def get_pending_approval(thread_ts: str) -> dict:
    return _PENDING_APPROVALS.get(thread_ts)


def clear_pending_approval(thread_ts: str):
    _PENDING_APPROVALS.pop(thread_ts, None)


# ── Playwright helpers ────────────────────────────────────────────────────────

async def _fill_field(page, label: str, value: str):
    """Fill a text input by accessible label or placeholder."""
    if not value:
        return
    try:
        await page.get_by_label(label, exact=False).first.fill(value)
        return
    except Exception:
        pass
    try:
        await page.get_by_placeholder(label, exact=False).first.fill(value)
    except Exception:
        pass


async def _toggle_on(page, label: str):
    """Ensure a toggle is switched ON."""
    try:
        toggle = page.get_by_label(label, exact=False).first
        if not await toggle.is_checked():
            await toggle.click()
    except Exception:
        pass


async def _download_and_upload(page, upload_text: str, image_url: str):
    """Download image_url to a temp file then upload via Playwright file chooser."""
    if not image_url or not image_url.startswith("http"):
        return
    try:
        resp = requests.get(image_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        ext = ".png" if "png" in image_url.lower() else ".jpg"
        tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
        tmp.write(resp.content)
        tmp.close()

        async with page.expect_file_chooser(timeout=5000) as fc_info:
            await page.get_by_text(upload_text, exact=False).first.click()
        fc = await fc_info.value
        await fc.set_files(tmp.name)
        log.info(f"Uploaded creative: {upload_text} ← {image_url}")
    except Exception as e:
        log.warning(f"Creative upload skipped ({upload_text}): {e}")


# ── Tab fillers ───────────────────────────────────────────────────────────────

async def _fill_tab1_content(page, brief_data: dict, copy: dict):
    """Tab 1 — Content & Goals."""
    advertiser   = brief_data.get("advertiser", "")
    payout_num   = brief_data.get("payout_num") or 0
    payout_type  = (brief_data.get("payout_type") or "").lower()
    description  = (brief_data.get("description") or "")
    tracking_url = brief_data.get("tracking_url", "")
    titles       = copy.get("titles", [""])
    ctas         = copy.get("ctas", [{"yes": "Get Started", "no": "No Thanks"}])

    internal_name = f"{advertiser} — {brief_data.get('network','').title()} — {datetime.today().strftime('%Y-%m-%d')}"

    await _fill_field(page, "Internal Offer Name", internal_name[:100])
    await _fill_field(page, "Partner Offer Name",  advertiser[:80])
    await _fill_field(page, "Advertiser Name",     advertiser[:28])
    await _fill_field(page, "Destination",         tracking_url)
    await _fill_field(page, "Headline",            titles[0][:90] if titles else "")
    await _fill_field(page, "Short Headline",      titles[0][:60] if titles else "")
    await _fill_field(page, "Description",         description[:220])
    await _fill_field(page, "Short Description",   description[:140])
    await _fill_field(page, "Positive CTA",        ctas[0].get("yes", "")[:25])
    await _fill_field(page, "Negative CTA",        ctas[0].get("no", "No Thanks")[:25])

    # Goal Type
    goal_type = "CPC" if "click" in payout_type else "CPA"
    try:
        await page.locator("select").filter(has_text="CPA").select_option(goal_type)
    except Exception:
        pass

    # Goal Amount
    if payout_num and payout_num > 0:
        await _fill_field(page, "Goal", str(payout_num))


async def _fill_tab2_config(page, brief_data: dict):
    """Tab 2 — Configuration."""
    network_map = {
        "impact":     "Impact",
        "flexoffers": "FlexOffers",
        "maxbounty":  "MaxBounty",
    }
    network_label = network_map.get((brief_data.get("network") or "").lower(), "")
    offer_id      = str(brief_data.get("offer_id", ""))
    icon_url      = brief_data.get("icon_url", "")
    hero_url      = brief_data.get("hero_url", "")

    # Network dropdown
    if network_label:
        try:
            await page.get_by_label("Network", exact=False).first.select_option(label=network_label)
        except Exception:
            try:
                await page.locator(".network-select, select[aria-label*='Network']").select_option(label=network_label)
            except Exception:
                pass

    await _fill_field(page, "Network Offer ID", offer_id)

    # Toggles
    await _toggle_on(page, "Perkswall Enabled")
    await _toggle_on(page, "Test Offer")   # stays ON until human approves

    # Creative uploads
    if icon_url:
        await _download_and_upload(page, "Icon Creative", icon_url)
    if hero_url and hero_url != icon_url:
        await _download_and_upload(page, "Hero Creative", hero_url)


async def _fill_tab3_tracking(page, brief_data: dict):
    """Tab 3 — Tracking & Analytics."""
    note = (
        f"Created by Scout · "
        f"{(brief_data.get('network') or '').title()} offer {brief_data.get('offer_id', '')} · "
        f"{datetime.today().strftime('%Y-%m-%d %H:%M')}"
    )
    try:
        await page.locator("textarea").filter(has_placeholder_text="Internal Notes").fill(note)
    except Exception:
        try:
            await page.get_by_label("Internal Notes", exact=False).fill(note)
        except Exception:
            pass


async def _fill_tab4_targeting(page, brief_data: dict):
    """Tab 4 — Targeting. Sets geo whitelist from offer's geo field."""
    geo = brief_data.get("geo", "US Only")
    geo_to_countries = {
        "US Only":       ["United States"],
        "US + CA":       ["United States", "Canada"],
        "UK":            ["United Kingdom"],
        "North America": ["United States", "Canada"],
        "Global":        [],
    }
    countries = geo_to_countries.get(geo, ["United States"])

    # Set mode to Whitelist
    try:
        whitelist_locator = page.get_by_role("option", name="Whitelist")
        await whitelist_locator.click()
    except Exception:
        pass

    # Select each country
    for country in countries:
        try:
            country_input = page.locator("input[placeholder*='country'], .country-search").first
            await country_input.fill(country)
            await page.wait_for_timeout(600)
            await page.get_by_text(country, exact=True).first.click()
        except Exception:
            pass


# ── Core build flow ───────────────────────────────────────────────────────────

async def _build_offer_async(brief_data: dict, copy: dict) -> bytes:
    """
    Fill all 4 tabs of Create Offer form. Returns screenshot PNG bytes.
    Does NOT click Save — waits for human approval.
    """
    if not SESSION_FILE.exists():
        raise RuntimeError(
            "No MS platform session found. "
            "Run: python campaign_builder.py --login"
        )
    if not MS_PLATFORM_URL:
        raise RuntimeError("MS_PLATFORM_URL not set in .env")

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page    = await context.new_page()

        await page.goto(f"{MS_PLATFORM_URL}/offers/create")
        await page.wait_for_load_state("networkidle", timeout=15000)

        # Tab 1
        await _fill_tab1_content(page, brief_data, copy)

        # Tab 2
        await page.get_by_role("tab", name="Configuration").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab2_config(page, brief_data)

        # Tab 3
        await page.get_by_role("tab", name="Tracking").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab3_tracking(page, brief_data)

        # Tab 4
        await page.get_by_role("tab", name="Targeting").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab4_targeting(page, brief_data)

        # Screenshot the full form state
        screenshot = await page.screenshot(full_page=False)
        await browser.close()
        return screenshot


async def _submit_offer_async(brief_data: dict, copy: dict):
    """Fill and SAVE the offer. Called after human approves."""
    if not SESSION_FILE.exists():
        raise RuntimeError("No MS platform session found.")
    if not MS_PLATFORM_URL:
        raise RuntimeError("MS_PLATFORM_URL not set in .env")

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page    = await context.new_page()

        await page.goto(f"{MS_PLATFORM_URL}/offers/create")
        await page.wait_for_load_state("networkidle", timeout=15000)

        await _fill_tab1_content(page, brief_data, copy)

        await page.get_by_role("tab", name="Configuration").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab2_config(page, brief_data)

        await page.get_by_role("tab", name="Tracking").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab3_tracking(page, brief_data)

        await page.get_by_role("tab", name="Targeting").click()
        await page.wait_for_load_state("networkidle")
        await _fill_tab4_targeting(page, brief_data)

        # Click Save
        await page.get_by_role("button", name="Save").click()
        await page.wait_for_load_state("networkidle", timeout=10000)
        await browser.close()


# ── Public API (sync wrappers for scout_bot.py) ───────────────────────────────

def launch_offer_in_background(brief_data: dict, copy: dict,
                                slack_channel: str, slack_thread_ts: str,
                                web_client):
    """
    Run Playwright in a background thread.
    Posts screenshot + approval prompt to Slack when form is filled.
    """
    def _run():
        try:
            screenshot = asyncio.run(_build_offer_async(brief_data, copy))

            # Store for approval
            store_pending_approval(slack_thread_ts, brief_data, copy)

            advertiser = brief_data.get("advertiser", "Offer")
            web_client.files_upload_v2(
                channel=slack_channel,
                thread_ts=slack_thread_ts,
                content=screenshot,
                filename="campaign_preview.png",
                initial_comment=(
                    f"*{advertiser}* — form filled, Test Offer is ON.\n"
                    f"Review the preview above, then reply `@Scout approve` to save and go live."
                ),
            )
        except Exception as e:
            log.error(f"Campaign builder background error: {e}")
            web_client.chat_postMessage(
                channel=slack_channel,
                thread_ts=slack_thread_ts,
                text=f"Campaign builder error: {e}",
            )

    threading.Thread(target=_run, daemon=True).start()


def submit_pending_offer(thread_ts: str, slack_channel: str, web_client) -> bool:
    """Submit a pending offer after human approval. Returns False if nothing pending."""
    pending = get_pending_approval(thread_ts)
    if not pending:
        return False

    brief_data = pending["brief_data"]
    copy       = pending["copy"]
    advertiser = brief_data.get("advertiser", "Offer")

    def _run():
        try:
            asyncio.run(_submit_offer_async(brief_data, copy))
            clear_pending_approval(thread_ts)
            web_client.chat_postMessage(
                channel=slack_channel,
                thread_ts=thread_ts,
                text=(
                    f"*{advertiser}* saved to the MS platform. "
                    f"Test Offer is ON — review it in the platform, then turn Test Offer OFF to go fully live."
                ),
            )
        except Exception as e:
            log.error(f"Submit error: {e}")
            web_client.chat_postMessage(
                channel=slack_channel,
                thread_ts=thread_ts,
                text=f"Submit failed: {e}",
            )

    threading.Thread(target=_run, daemon=True).start()
    return True


# ── Login helper ──────────────────────────────────────────────────────────────

async def _login_and_save():
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page    = await context.new_page()
        await page.goto(MS_PLATFORM_URL or "https://app.momentscience.com")
        print("\nLog in to the MS platform in the browser window that just opened.")
        print("When you're fully logged in and on the main dashboard, press Enter here...")
        input()
        await context.storage_state(path=str(SESSION_FILE))
        await browser.close()
        print(f"Session saved to {SESSION_FILE}")


if __name__ == "__main__":
    if "--login" in sys.argv:
        asyncio.run(_login_and_save())
    else:
        print("Usage:")
        print("  python campaign_builder.py --login   # save MS platform session")
