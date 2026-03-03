# 🚀 CMS Provider Search - Quick Start

## Open Dashboard
```bash
open ~/Repo/cms-data/dashboard/index.html
```

## Test Searches (Click in Dashboard)
- Endocrinologist — Santa Monica
- Cardiologist — Beverly Hills
- Orthopedic Surgeon — Pasadena

## Run Tests
```bash
cd ~/Repo/cms-data && ./test_enhanced_matching.sh
```

## Enable LLM Matching (Optional)
```bash
cd ~/Repo/cms-data && ./add_openai_key.sh YOUR_API_KEY
```
Get key: https://platform.openai.com/api-keys

## API Examples
```bash
# Provider search
curl "http://5.78.148.70:8080/search/places?specialty=cardiologist&location=Beverly%20Hills,%20CA" \
  -H "X-API-Key: 1bb250cdd582258595a5d2bebd9493f2c74a7999"

# Direct match
curl "http://5.78.148.70:8080/match/search?name=John%20Smith&address=Los%20Angeles,%20CA" \
  -H "X-API-Key: 1bb250cdd582258595a5d2bebd9493f2c74a7999"
```

## Badge Meanings
- 🟢 Green = Exact CMS match (0.8+)
- 🟡 Yellow = AI-powered match
- 🟣 Purple = Organization (click to expand roster)
- ❌ Red = No match

## Status
✅ All features deployed and working  
⚠️ LLM matching ready (needs API key)  
📊 40% match rate (50-60% with LLM)

## Docs
- `README_ENHANCED_SEARCH.md` - Full guide
- `TEST_DEPLOYMENT.md` - Technical details
- `DEPLOYMENT_COMPLETE.md` - Summary
