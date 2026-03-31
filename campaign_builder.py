"""
campaign_builder.py — MS Platform Campaign Builder
Single-shot: fill all 4 tabs, save with Test Offer ON, return the offer URL.

Architecture
────────────
Fill once → save → post link to Slack.
Test Offer is ON so the offer is invisible to real users until ops reviews it
in the platform and flips it live. That IS the human approval gate — no
double-fill, no screenshot approval step, no in-memory state to lose.

Setup (one-time):
  python campaign_builder.py --login   ← opens browser, log in, saves session

Triggered by scout_bot.py when user says "@Scout launch this".
"""

import asyncio
import logging
import os
import pathlib
import re
import sys
import tempfile
import threading
from datetime import datetime

import requests

log = logging.getLogger("campaign_builder")

MS_PLATFORM_URL = os.getenv("MS_PLATFORM_URL", "https://app.momentscience.com")
SESSION_FILE    = pathlib.Path(__file__).parent / "ms_session.json"


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

    internal_name = (
        f"{advertiser} — {brief_data.get('network','').title()} "
        f"— {datetime.today().strftime('%Y-%m-%d')}"
    )

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

    # Goal Type — CPC for click-based, CPA otherwise
    goal_type = "CPC" if "click" in payout_type else "CPA"
    try:
        await page.locator("select").filter(has_text="CPA").select_option(goal_type)
    except Exception:
        pass

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

    # Network dropdown — try both native select and React custom select
    if network_label:
        try:
            await page.get_by_label("Network", exact=False).first.select_option(label=network_label)
        except Exception:
            try:
                await page.locator(".network-select, select[aria-label*='Network']").select_option(label=network_label)
            except Exception:
                log.warning(f"Could not set network dropdown to {network_label!r}")

    await _fill_field(page, "Network Offer ID", offer_id)

    # Toggles — Perkswall enabled, Test Offer ON (safety gate until ops reviews)
    await _toggle_on(page, "Perkswall Enabled")
    await _toggle_on(page, "Test Offer")

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

def _is_logged_out(url: str, title: str) -> bool:
    """Detect if Playwright landed on the login page."""
    low_url   = url.lower()
    low_title = title.lower()
    return (
        "sign in" in low_title
        or "login" in low_url
        or "sign-in" in low_url
        or "sign_in" in low_url
        or "auth" in low_url
    )


async def _open_create_offer(page):
    """Navigate to the Create Offer form, handling the session check."""
    base = (MS_PLATFORM_URL or "https://app.momentscience.com").rstrip("/")

    for path in ["/offers/create", "/offer/create", "/campaigns/create"]:
        await page.goto(f"{base}{path}", timeout=20_000)
        await page.wait_for_load_state("networkidle", timeout=15_000)
        title = await page.title()
        if not _is_logged_out(page.url, title):
            log.info(f"Landed on create form at {page.url}")
            return

    raise RuntimeError("SESSION_EXPIRED")


async def _fill_form_all_tabs(page, brief_data: dict, copy: dict):
    """Fill all 4 tabs in order."""
    await _fill_tab1_content(page, brief_data, copy)

    for tab_name in ["Configuration", "Tracking", "Targeting"]:
        tab = page.get_by_role("tab", name=tab_name)
        if await tab.count():
            await tab.click()
            await page.wait_for_load_state("networkidle", timeout=10_000)
        if tab_name == "Configuration":
            await _fill_tab2_config(page, brief_data)
        elif tab_name == "Tracking":
            await _fill_tab3_tracking(page, brief_data)
        elif tab_name == "Targeting":
            await _fill_tab4_targeting(page, brief_data)


async def _build_and_save_async(brief_data: dict, copy: dict) -> str:
    """
    Single-shot: fill all 4 tabs, click Save, return the new offer URL.

    Test Offer is forced ON — the offer is invisible to real users until
    ops reviews it in the platform and manually flips it live.
    That is the human approval gate. No second Playwright run needed.

    Raises:
        RuntimeError("SESSION_EXPIRED") if session needs refresh
        RuntimeError("SAVE_BUTTON_NOT_FOUND") if MS platform UI changed
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

        try:
            await _open_create_offer(page)
            await _fill_form_all_tabs(page, brief_data, copy)

            # Save — match "Save", "Submit", or "Create" buttons
            save_btn = page.get_by_role("button", name=re.compile(r"Save|Submit|Create", re.I))
            if not await save_btn.count():
                raise RuntimeError("SAVE_BUTTON_NOT_FOUND")

            await save_btn.first.click()
            await page.wait_for_load_state("networkidle", timeout=20_000)

            offer_url = page.url
            log.info(f"Offer saved — landed on: {offer_url}")
            return offer_url
        finally:
            await browser.close()


# ── Public API (sync wrapper for scout_bot.py) ────────────────────────────────

def build_offer_in_background(
    brief_data: dict,
    copy: dict,
    slack_channel: str,
    slack_thread_ts: str,
    web_client,
):
    """
    Run the single-shot Playwright build in a background thread.
    Posts a confirmation with a direct link to the saved offer when done.
    """
    def _run():
        advertiser = brief_data.get("advertiser", "Offer")
        try:
            offer_url = asyncio.run(_build_and_save_async(brief_data, copy))

            base = (MS_PLATFORM_URL or "https://app.momentscience.com").rstrip("/")
            if offer_url and not offer_url.startswith("http"):
                offer_url = f"{base}{offer_url}"

            link = f"<{offer_url}|View in MS Platform>" if offer_url else "Check the MS platform"

            web_client.chat_postMessage(
                channel=slack_channel,
                thread_ts=slack_thread_ts,
                text=(
                    f":white_check_mark: *{advertiser}* is saved.\n"
                    f"{link}\n"
                    f"_Test Offer is ON — review it in the platform, then flip Test Offer OFF to go live._"
                ),
            )

        except RuntimeError as e:
            err = str(e)
            if "SESSION_EXPIRED" in err:
                web_client.chat_postMessage(
                    channel=slack_channel,
                    thread_ts=slack_thread_ts,
                    text=(
                        ":lock: *MS platform session expired.*\n"
                        "Run this in your terminal to re-authenticate:\n"
                        "```python campaign_builder.py --login```\n"
                        "_The brief is saved — just say `@Scout launch this` again after logging in._"
                    ),
                )
            elif "SAVE_BUTTON_NOT_FOUND" in err:
                web_client.chat_postMessage(
                    channel=slack_channel,
                    thread_ts=slack_thread_ts,
                    text=(
                        ":warning: *Could not find the Save button on the MS platform form.*\n"
                        "The platform UI may have changed. Flagging for a selector update."
                    ),
                )
            else:
                log.error(f"Campaign builder error: {err}")
                web_client.chat_postMessage(
                    channel=slack_channel,
                    thread_ts=slack_thread_ts,
                    text=f":x: Campaign builder error: {err}",
                )
        except Exception as e:
            log.error(f"Campaign builder background error: {e}", exc_info=True)
            web_client.chat_postMessage(
                channel=slack_channel,
                thread_ts=slack_thread_ts,
                text=f":x: Campaign builder error: {e}",
            )

    threading.Thread(target=_run, daemon=True).start()


# ── Login helper ──────────────────────────────────────────────────────────────

async def _login_and_save():
    """
    Open a visible browser window to app.momentscience.com.
    Waits for you to log in, then saves the session automatically.
    """
    from playwright.async_api import async_playwright

    target = (MS_PLATFORM_URL or "https://app.momentscience.com").rstrip("/")
    print(f"\nOpening {target} ...")
    print("Log in with your MS credentials.")
    print("The window will close and session will save automatically once you reach the dashboard.\n")

    async with async_playwright() as p:
        # Let Playwright use its own managed Chromium — no executable_path override.
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox"])
        context = await browser.new_context()
        page    = await context.new_page()
        await page.goto(target)

        # Auto-detect successful login (up to 3 minutes)
        logged_in = False
        for _ in range(180):
            await asyncio.sleep(1)
            url   = page.url
            title = await page.title()
            if not _is_logged_out(url, title):
                print(f"Logged in — dashboard detected at {url}")
                logged_in = True
                break

        if not logged_in:
            print("Timed out waiting for login. Please try again.")
            await browser.close()
            return

        await context.storage_state(path=str(SESSION_FILE))
        print(f"\nSession saved → {SESSION_FILE}")
        print("You can now use '@Scout launch this' in Slack.\n")
        await browser.close()


if __name__ == "__main__":
    if "--login" in sys.argv:
        asyncio.run(_login_and_save())
    else:
        print("Usage:")
        print("  python campaign_builder.py --login   # save MS platform session")
