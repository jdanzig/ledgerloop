"""Fetch real material agreements (EX-10 exhibits) from SEC EDGAR full-text
search into corpus/, messy formatting intact.

Usage:
    python scripts/seed_edgar.py [--query "master services agreement"] [--limit 3]

The bundled corpus/ also ships an agreement + its amendment (fixtures) so the
supersedes-chain demo works deterministically offline; EDGAR docs add realism.
"""

import argparse
import asyncio
import html
import json
import pathlib
import re

import httpx

CORPUS = pathlib.Path(__file__).parent.parent / "corpus"
FTS = "https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&forms=8-K,10-K,10-Q"
UA = {"User-Agent": "ledgerloop demo jon.danzig@gmail.com"}


def strip_html(raw: str) -> str:
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", raw, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"[ \t]+", " ", text).strip()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", default="master services agreement")
    ap.add_argument("--limit", type=int, default=3)
    args = ap.parse_args()

    CORPUS.mkdir(exist_ok=True)
    async with httpx.AsyncClient(headers=UA, timeout=30, follow_redirects=True) as client:
        r = await client.get(FTS.format(query=args.query.replace(" ", "+")))
        r.raise_for_status()
        hits = r.json()["hits"]["hits"]

        saved = 0
        for hit in hits:
            if saved >= args.limit:
                break
            src = hit["_source"]
            accession, _, filename = hit["_id"].partition(":")
            if not filename.endswith((".htm", ".html", ".txt")):
                continue
            cik = src["ciks"][0].lstrip("0")
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                f"{accession.replace('-', '')}/{filename}"
            )
            doc = await client.get(url)
            if doc.status_code != 200:
                continue
            text = strip_html(doc.text)
            if len(text) < 5000:  # exhibit stubs aren't interesting
                continue
            slug = re.sub(r"[^a-z0-9]+", "-", filename.lower()).strip("-")[:60]
            (CORPUS / f"edgar-{slug}.txt").write_text(text)
            (CORPUS / f"edgar-{slug}.json").write_text(
                json.dumps(
                    {
                        "title": src.get("display_names", [slug])[0],
                        "source": url,
                        "accession": accession,
                    },
                    indent=2,
                )
            )
            print(f"saved edgar-{slug}.txt ({len(text)} chars) from {url}")
            saved += 1
            await asyncio.sleep(0.5)  # SEC rate courtesy

        if saved == 0:
            print("no EDGAR documents saved (network/API issue?) — "
                  "the bundled fixtures in corpus/ still cover the demo")


if __name__ == "__main__":
    asyncio.run(main())
