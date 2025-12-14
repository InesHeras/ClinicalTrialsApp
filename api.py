from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://clinicaltrials.gov/api/v2"


def _session_with_retries() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_studies_raw(
    *,
    query_cond: Optional[str] = None,
    query_term: Optional[str] = None,
    query_intr: Optional[str] = None,
    query_locn: Optional[str] = None,
    query_titles: Optional[str] = None,
    query_spons: Optional[str] = None,
    filter_overall_status: Optional[Iterable[str]] = None,
    sort: str = "LastUpdatePostDate:desc",
    page_size: int = 200,
    max_pages: Optional[int] = None,
    max_records: Optional[int] = None,
    polite_sleep_s: float = 0.12,
    last_update_from: Optional[str] = None,
    last_update_to: Optional[str] = None, 
) -> List[Dict[str, Any]]:
    if page_size < 1 or page_size > 1000:
        raise ValueError("page_size debe estar entre 1 y 1000")

    session = _session_with_retries()
    url = f"{BASE_URL}/studies"

    params: Dict[str, Any] = {
        "pageSize": page_size,
        "sort": sort,
    }

    # Queries “rápidas”
    if query_cond:
        params["query.cond"] = query_cond
    if query_intr:
        params["query.intr"] = query_intr
    if query_locn:
        params["query.locn"] = query_locn
    if query_titles:
        params["query.titles"] = query_titles
    if query_spons:
        params["query.spons"] = query_spons

    
    date_expr = None
    if last_update_from or last_update_to:
        start = last_update_from or "1900-01-01"
        end = last_update_to or "MAX"
        date_expr = f"AREA[LastUpdatePostDate]RANGE[{start},{end}]"

    
    final_query_term = query_term
    if date_expr:
        if final_query_term:
            final_query_term = f"({final_query_term}) AND {date_expr}"
        else:
            final_query_term = date_expr

    if final_query_term:
        params["query.term"] = final_query_term

    
    if filter_overall_status:
        params["filter.overallStatus"] = ",".join(filter_overall_status)

    studies: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    page_count = 0

    while True:
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)

        r = session.get(url, params=params, timeout=60)

        if r.status_code == 429:
            time.sleep(2.0)
            continue

        r.raise_for_status()
        payload = r.json()

        batch = payload.get("studies", [])
        if not isinstance(batch, list):
            raise RuntimeError("Respuesta inesperada: 'studies' no es una lista.")

        studies.extend(batch)

        page_token = payload.get("nextPageToken")
        page_count += 1

        if polite_sleep_s:
            time.sleep(polite_sleep_s)

        if max_records is not None and len(studies) >= max_records:
            return studies[:max_records]

        if max_pages is not None and page_count >= max_pages:
            return studies

        if not page_token:
            return studies


def studies_to_flat_df(studies: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for s in studies:
        ps = (s.get("protocolSection") or {})
        idm = (ps.get("identificationModule") or {})
        st = (ps.get("statusModule") or {})
        design = (ps.get("designModule") or {})
        cond = (ps.get("conditionsModule") or {})
        sponsor = (ps.get("sponsorCollaboratorsModule") or {})
        contacts = (ps.get("contactsLocationsModule") or {})

        conditions_all = (cond.get("conditions") or [])
        condition_main = conditions_all[0] if isinstance(conditions_all, list) and len(conditions_all) else None

        rows.append(
            {
                "nctId": idm.get("nctId"),
                "briefTitle": idm.get("briefTitle"),
                "officialTitle": idm.get("officialTitle"),
                "overallStatus": st.get("overallStatus"),
                "startDate": st.get("startDateStruct", {}).get("date"),
                "primaryCompletionDate": st.get("primaryCompletionDateStruct", {}).get("date"),
                "completionDate": st.get("completionDateStruct", {}).get("date"),
                "studyType": design.get("studyType"),
                "phase": (design.get("phases") or [None])[0],
                "enrollmentCount": (design.get("enrollmentInfo") or {}).get("count"),

                
                "conditions": conditions_all,     # lista completa
                "condition": condition_main,      # principal (primera)

                "leadSponsor": (sponsor.get("leadSponsor") or {}).get("name"),
                "collaborators": [
                    c.get("name") for c in (sponsor.get("collaborators") or []) if isinstance(c, dict)
                ],
                "countries": list(
                    {
                        loc.get("country")
                        for loc in (contacts.get("locations") or [])
                        if isinstance(loc, dict) and loc.get("country")
                    }
                ),
            }
        )

    return pd.DataFrame(rows)
