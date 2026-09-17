"""
offer_scraper.py — backward-compat shim.

The real module moved to scout/offers/scraper.py (see DESIGN.md's target
package structure). This file aliases `offer_scraper` in sys.modules to the
real module object, so every existing form of import keeps working
unchanged: `import offer_scraper`, `from offer_scraper import X`,
`offer_scraper.anything`, including private/underscore-prefixed names and
mutable module-level state (caches, etc.) — because after this runs,
`offer_scraper` IS `scout.offers.scraper`, not a separate copy of it.

Kept for at least one deploy cycle so a Render rollback to the prior commit
doesn't hit an ImportError. New code should import from scout.offers.scraper
directly; this shim is not the place to add anything new.
"""


import sys

from scout.offers import scraper as _scraper

sys.modules[__name__] = _scraper


# Direct CLI execution (`python3 offer_scraper.py ...`, what run_scraper.sh's
# cron job and README.md's manual test instructions both do) must keep working
# exactly as before the move. `sys.modules[__name__] = _scraper` above only
# fixes the "imported as a module" case — when this file runs as a script,
# `__name__` is "__main__" here (not "scout.offers.scraper"), so the real
# module's own `if __name__ == "__main__":` guard never fires and main() is
# silently never called. Call it explicitly in that case instead.
if __name__ == "__main__":
    _scraper.main()
