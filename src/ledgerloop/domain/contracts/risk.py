"""Rule-based risk flags — rules, not a model: every flag must be
explainable in an audit context."""

from __future__ import annotations

import datetime as dt
from typing import Any


def flag_risks(
    terms: dict[str, Any], today: dt.date | None = None
) -> list[dict[str, str]]:
    today = today or dt.date.today()
    flags: list[dict[str, str]] = []

    deadline = terms.get("renewal_notice_deadline")
    if terms.get("auto_renew") and deadline:
        d = dt.date.fromisoformat(deadline)
        if d <= today + dt.timedelta(days=90):
            flags.append(
                {
                    "rule": "auto_renew_notice_imminent",
                    "explanation": f"Auto-renews unless notice is given by {d}; "
                    f"that deadline is within 90 days.",
                }
            )

    if not terms.get("termination_for_convenience"):
        flags.append(
            {
                "rule": "no_termination_for_convenience",
                "explanation": "No termination-for-convenience clause; exit "
                "requires cause or term end.",
            }
        )

    if terms.get("liability_cap_amount") is None:
        flags.append(
            {
                "rule": "uncapped_liability",
                "explanation": "No liability cap stated; exposure is uncapped.",
            }
        )

    if (terms.get("initial_term_months") or 0) > 36 and not terms.get("has_price_cap"):
        flags.append(
            {
                "rule": "long_term_no_price_caps",
                "explanation": f"Term of {terms['initial_term_months']} months "
                "exceeds 3 years with no price-increase cap.",
            }
        )

    return flags
