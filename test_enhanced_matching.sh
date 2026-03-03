#!/bin/bash
# Test script for CMS Provider Search enhancements

API="http://5.78.148.70:8080"
KEY="1bb250cdd582258595a5d2bebd9493f2c74a7999"

echo "========================================"
echo "CMS Provider Search - Enhanced Matching"
echo "========================================"
echo

# Helper function for pretty output
test_match() {
    local name="$1"
    local address="$2"
    local description="$3"
    
    echo "TEST: $description"
    echo "Name: $name"
    echo "Address: $address"
    echo "---"
    
    curl -s "$API/match/search?name=$(echo "$name" | sed 's/ /%20/g')&address=$(echo "$address" | sed 's/ /%20/g' | sed 's/,/%2C/g')" \
        -H "X-API-Key: $KEY" | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"Input parsed as: {data['input_name']}\")
print(f\"Matches found: {data['match_count']}\")
if data['matches']:
    for i, m in enumerate(data['matches'][:3], 1):
        print(f\"  {i}. {m['first_name']} {m['last_name']}, {m['provider_type'] or 'Unknown'}\")
        print(f\"     {m['city']}, {m['state']} {m['zip5']}\")
        print(f\"     Match: {m['match_method']} (confidence: {m['confidence']})\")
        if m['total_medicare_payment']:
            print(f\"     Medicare: \${m['total_medicare_payment']:,.2f}\")
else:
    print('  No matches found')
"
    echo
    echo
}

# Test 1: Dr. prefix stripping
test_match "Dr. Smith" "Beverly Hills, CA" "Name Parser - Dr. prefix stripping"

# Test 2: Middle initial stripping
test_match "John A Smith" "Los Angeles, CA 90012" "Name Parser - Middle initial stripping"

# Test 3: Possessive extraction
test_match "Dr. Smith's Clinic" "Los Angeles, CA" "Name Parser - Possessive extraction"

# Test 4: Multi-address matching
test_match "Jennifer Lee" "Los Angeles, CA 90024" "Multi-address matching"

# Test 5: City + State matching
test_match "Robert Chen" "Beverly Hills, CA" "Fuzzy city matching"

# Test 6: Specialty hint boosting
echo "TEST: Specialty hint boosting"
echo "Name: Sarah Johnson"
echo "Address: Santa Monica, CA"
echo "Specialty: Internal Medicine"
echo "---"
curl -s "$API/match/search?name=Sarah%20Johnson&address=Santa%20Monica,%20CA&specialty=Internal%20Medicine" \
    -H "X-API-Key: $KEY" | \
    python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"Matches found: {data['match_count']}\")
if data['matches']:
    for i, m in enumerate(data['matches'][:2], 1):
        spec_match = '✓ SPECIALTY MATCH' if 'internal medicine' in (m['provider_type'] or '').lower() else ''
        print(f\"  {i}. {m['first_name']} {m['last_name']}, {m['provider_type']} {spec_match}\")
        print(f\"     Confidence: {m['confidence']}\")
"
echo
echo

# Test 7: Provider Search integration (if Google Places is working)
echo "TEST: Integrated Provider Search (Google Places + CMS)"
echo "Searching: endocrinologist in Santa Monica, CA"
echo "---"
response=$(curl -s "$API/search/places?specialty=endocrinologist&location=Santa%20Monica,%20CA" -H "X-API-Key: $KEY")
if echo "$response" | grep -q "DEADLINE_EXCEEDED\|REQUEST_DENIED"; then
    echo "⚠️  Google Places API timeout or error (this is expected occasionally)"
    echo "    Try running this search directly in the dashboard"
else
    echo "$response" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"Google Places results: {data['total_places']}\")
print(f\"CMS matched: {data['matched_count']}\")
print(f\"Match rate: {data['matched_count']/max(data['total_places'],1)*100:.1f}%\")
print(f\"\nSample results:\")
for i, r in enumerate(data['results'][:3], 1):
    p = r['place']
    m = r['cms_match']
    print(f\"  {i}. {p['name']}\")
    print(f\"     Google: {p['rating'] or 'N/A'} stars, {p['reviews_count'] or 0} reviews\")
    if m:
        print(f\"     CMS: {m['first_name']} {m['last_name']}, NPI {m['npi']}\")
        print(f\"     Match: {m['match_method']} (confidence: {m['confidence']})\")
        if m.get('llm_reasoning'):
            print(f\"     LLM: {m['llm_reasoning'][:60]}...\")
    elif r['match_type'] == 'organization' and r.get('provider_roster'):
        print(f\"     Organization with {len(r['provider_roster'])} providers in roster\")
    else:
        print(f\"     ❌ No CMS match found\")
"
fi
echo
echo

echo "========================================"
echo "All tests completed!"
echo "========================================"
echo
echo "Next steps:"
echo "1. Open dashboard: open ~/Repo/cms-data/dashboard/index.html"
echo "2. To enable LLM matching, add OPENAI_API_KEY to /etc/systemd/system/cms-api.service"
echo "3. See TEST_DEPLOYMENT.md for full documentation"
