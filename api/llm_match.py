"""
LLM-powered NPI matching for ambiguous cases.

When traditional matching fails or returns low-confidence results,
we assemble an evidence packet and ask GPT-4o-mini to make a judgment call.
"""

import os
import json
import logging
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"  # Cost-efficient: ~$0.002/call


async def llm_match_provider(
    place_name: str,
    place_address: str,
    candidate_npis: list[dict],
    conn
) -> Optional[dict]:
    """
    Use LLM to match a Google Places result to candidate NPI records.
    
    Args:
        place_name: The name from Google Places
        place_address: The address from Google Places
        candidate_npis: List of dicts with keys: npi, first_name, last_name, 
                        city, state, zip5, provider_type
        conn: DuckDB connection to fetch additional context
    
    Returns:
        dict with keys: npi, confidence (0-1), reasoning, method="llm_match"
        or None if LLM can't confidently match
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY not set, skipping LLM matching")
        return None
    
    if not candidate_npis:
        return None
    
    # Limit to top 5 candidates to keep token count down
    candidates = candidate_npis[:5]
    
    # Enrich candidates with NPPES details
    enriched_candidates = []
    for c in candidates:
        npi = str(c.get("npi", ""))
        try:
            nppes_row = conn.execute("""
                SELECT first_name, last_name, middle_name, credentials,
                       practice_address_1, practice_city, practice_state, practice_zip,
                       taxonomy_1, enumeration_date
                FROM raw_nppes
                WHERE CAST(npi AS VARCHAR) = ?
                LIMIT 1
            """, [npi]).fetchone()
            
            if nppes_row:
                enriched_candidates.append({
                    "npi": npi,
                    "first_name": nppes_row[0] or "",
                    "last_name": nppes_row[1] or "",
                    "middle_name": nppes_row[2] or "",
                    "credentials": nppes_row[3] or "",
                    "practice_address": nppes_row[4] or "",
                    "practice_city": nppes_row[5] or "",
                    "practice_state": nppes_row[6] or "",
                    "practice_zip": nppes_row[7] or "",
                    "taxonomy": nppes_row[8] or "",
                    "enumeration_date": str(nppes_row[9]) if nppes_row[9] else "",
                })
        except Exception as e:
            logger.error(f"Error enriching candidate {npi}: {e}")
            # Add basic info as fallback
            enriched_candidates.append({
                "npi": npi,
                "first_name": c.get("first_name", ""),
                "last_name": c.get("last_name", ""),
                "city": c.get("city", ""),
                "state": c.get("state", ""),
                "zip5": c.get("zip5", ""),
            })
    
    # Build prompt
    prompt = f"""You are an expert at matching healthcare provider listings to NPI records.

Given this Google Places listing:
- Name: {place_name}
- Address: {place_address}

And these candidate NPI records:
{json.dumps(enriched_candidates, indent=2)}

Which candidate (if any) is the same provider as the Google Places listing?

Consider:
- Name variations (nicknames, middle names, suffixes)
- Address proximity (same city/zip is strong signal)
- Credentials and specialty alignment
- Recent enumeration dates suggest active practice

Return JSON with this structure:
{{
  "npi": "1234567890" or null,
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation of why this is or isn't a match"
}}

Only return a match if confidence >= 0.7. If no good match, return {{"npi": null, "confidence": 0.0, "reasoning": "..."}}.
"""

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a healthcare provider matching expert. Always return valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 300,
                }
            )
            response.raise_for_status()
            result = response.json()
            
            # Parse LLM response
            content = result["choices"][0]["message"]["content"]
            # Remove markdown code blocks if present
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            match_result = json.loads(content)
            
            if match_result.get("npi") and match_result.get("confidence", 0) >= 0.7:
                return {
                    "npi": match_result["npi"],
                    "confidence": match_result["confidence"],
                    "reasoning": match_result["reasoning"],
                    "method": "llm_match"
                }
            else:
                logger.info(f"LLM declined to match: {match_result.get('reasoning')}")
                return None
                
    except Exception as e:
        logger.error(f"LLM matching error: {e}")
        return None
