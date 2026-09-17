"""scout.offers — the affiliate-network offer scraper.

Moved from offer_scraper.py at the repo root (see DESIGN.md's target package
structure). offer_scraper.py is now a backward-compat shim aliasing this
module in sys.modules, kept for at least one deploy cycle so a Render
rollback to the prior commit doesn't hit an ImportError.
"""
