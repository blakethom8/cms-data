# HealthcareDataAI.com Site Architecture

**Version:** 1.0  
**Date:** 2026-03-03  
**Purpose:** Transform single-page dashboard into multi-page portfolio + lead generation platform

---

## Site Map

```
healthcaredataai.com/
├── index.html                    ← Homepage (NEW)
├── dashboard/                    ← Interactive data explorer (EXISTING)
│   └── index.html               
├── projects/                     ← Project showcases (NEW)
│   ├── index.html                   Portfolio overview
│   ├── healthcare-cost/             Cost analysis project
│   │   ├── index.html                  Overview
│   │   └── methodology.html            Deep-dive (Phase 2)
│   └── fraud-analysis/              Fraud detection project
│       ├── index.html                  Overview
│       └── methodology.html            Deep-dive (Phase 2)
├── access/                       ← Data access & API docs (NEW)
│   └── index.html
├── about/                        ← About the project (NEW)
│   └── index.html
└── api/                          ← API endpoints (EXISTING, backend)
```

---

## User Journeys

### Journey 1: Researcher / Data Scientist
**Entry:** Google search "CMS Medicare data API"  
**Path:** Homepage → Dashboard (explore) → Access page (request data)  
**Goal:** Download dataset for research  
**Conversion:** Email captured via data request form

### Journey 2: Healthcare Executive / Consultant
**Entry:** LinkedIn post or referral  
**Path:** Homepage → Projects (see expertise) → About → Contact  
**Goal:** Evaluate Blake's capabilities, potentially hire  
**Conversion:** Direct email or schedule call

### Journey 3: Health System Data Team
**Entry:** Google search "Medicare provider fraud detection"  
**Path:** Homepage → Fraud Analysis project → Dashboard (explore) → Contact  
**Goal:** Understand methodology, consider hiring for internal project  
**Conversion:** Email via "Want this for your organization?" CTA

### Journey 4: Student / Learner
**Entry:** Twitter or blog mention  
**Path:** Homepage → Dashboard (explore) → Projects (learn)  
**Goal:** Learn about healthcare data analysis  
**Conversion:** Low priority, but good for SEO/traffic

---

## Global Navigation

**Primary nav bar** (present on all pages):
```
┌─────────────────────────────────────────────────────────────┐
│ 🏥 HealthcareDataAI                                          │
│                                                              │
│  Home  |  Dashboard  |  Projects  |  Data Access  |  About  │
└─────────────────────────────────────────────────────────────┘
```

**Footer** (present on all pages):
```
┌─────────────────────────────────────────────────────────────┐
│ HealthcareDataAI.com                                         │
│                                                              │
│ Navigation          Resources           Connect              │
│ • Dashboard         • API Docs          • blake@...          │
│ • Projects          • GitHub            • blakethomson.studio│
│ • Data Access       • Documentation     • LinkedIn           │
│ • About                                                      │
│                                                              │
│ Data provided by CMS via data.cms.gov                        │
│ Not affiliated with CMS. Independent analysis.              │
│ © 2026 Blake Thomson                                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Page Details

### 1. Homepage (`/index.html`) — NEW

**Purpose:** Welcome visitors, explain what the site is, direct to key areas

**Sections:**
1. **Hero**
   - Headline: "CMS Healthcare Data Intelligence"
   - Subhead: "Free access to 100M+ Medicare records. Query, analyze, and build on the largest public healthcare dataset."
   - CTA buttons: [Explore Dashboard] [Request Data Access]

2. **What's Inside** (Data Overview)
   - 4 cards showing dataset highlights
   - Numbers: 107M records, 30 datasets, 7.1M providers
   - Icons for provider data, financial transparency, hospitals, prescribing

3. **Built on Real Data** (Projects Showcase)
   - 2 project cards (Healthcare Cost, Fraud Analysis)
   - Brief description + "Learn more →" link
   - Positioning: "See what's possible with this data"

4. **Who Uses This?** (Use Cases)
   - 4 personas: Researchers, Health Systems, Startups, Consultants
   - One-liner for each

5. **About** (Credibility + CTA)
   - Blake's background (Cedars-Sinai, 2+ years, "data guru")
   - Positioning: "Need custom healthcare data solutions?"
   - CTA: [Contact Us] button

**Wireframe:**
```
┌────────────────────────────────────────────┐
│  NAVIGATION BAR                            │
├────────────────────────────────────────────┤
│                                            │
│  CMS Healthcare Data Intelligence          │
│  Free access to 100M+ Medicare records     │
│                                            │
│  [Explore Dashboard] [Request Data]        │
│                                            │
├────────────────────────────────────────────┤
│  What's Inside                             │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐     │
│  │  🏥  │ │  💰  │ │  🏨  │ │  💊  │     │
│  └──────┘ └──────┘ └──────┘ └──────┘     │
├────────────────────────────────────────────┤
│  Built on Real Data                        │
│  ┌─────────────────┐ ┌─────────────────┐  │
│  │ Healthcare Cost │ │ Fraud Detection │  │
│  │ Analysis        │ │ & Anomalies     │  │
│  └─────────────────┘ └─────────────────┘  │
├────────────────────────────────────────────┤
│  Who Uses This?                            │
│  🔬 Researchers  🏢 Health Systems         │
│  🚀 Startups     💼 Consultants            │
├────────────────────────────────────────────┤
│  About / Contact CTA                       │
└────────────────────────────────────────────┘
│  FOOTER                                    │
└────────────────────────────────────────────┘
```

---

### 2. Dashboard (`/dashboard/index.html`) — UPDATE EXISTING

**Changes needed:**
- ✅ Keep current functionality (7 tabs, working queries)
- ✅ Add navigation bar at top (Home | Dashboard | Projects | Data Access | About)
- ✅ Add footer
- ✅ Update header: add tagline below "CMS Provider Intelligence"
  - "Interactive data explorer • 107M records • Real-time queries"

**Current tabs (keep as-is):**
1. Overview
2. Data Sources
3. Explore
4. SQL Query
5. Match Engine
6. Places Search
7. Unified Search

---

### 3. Projects Overview (`/projects/index.html`) — NEW

**Purpose:** Portfolio page showcasing data analysis projects

**Structure:**
1. **Hero**
   - "Healthcare Data Projects"
   - "Real analysis on real data. See what's possible."

2. **Featured Projects** (2 cards, expandable to more)
   - Healthcare Cost Analysis
   - Fraud Detection & Anomaly Analysis
   - Each card: thumbnail image, title, 2-3 sentence description, [Learn more] button

3. **What These Showcase**
   - Domain expertise (claims data, provider networks, cost accounting)
   - Technical skills (SQL, Python, network analysis, statistical modeling)
   - Business value (policy insights, fraud detection, cost reduction)

4. **CTA**
   - "Want custom analysis for your organization?"
   - [Contact us] button

**Wireframe:**
```
┌────────────────────────────────────────────┐
│  NAVIGATION BAR                            │
├────────────────────────────────────────────┤
│  Healthcare Data Projects                   │
│  Real analysis on real data                │
├────────────────────────────────────────────┤
│  ┌─────────────────────────────────────┐  │
│  │  Healthcare Cost Analysis            │  │
│  │  [thumbnail image]                   │  │
│  │  What does healthcare actually cost? │  │
│  │  Multi-method calculation...         │  │
│  │  [Learn more →]                      │  │
│  └─────────────────────────────────────┘  │
│  ┌─────────────────────────────────────┐  │
│  │  Fraud Detection & Anomaly Analysis  │  │
│  │  [thumbnail image]                   │  │
│  │  Finding outliers and suspicious...  │  │
│  │  [Learn more →]                      │  │
│  └─────────────────────────────────────┘  │
├────────────────────────────────────────────┤
│  What These Showcase                       │
│  • Domain expertise in claims data         │
│  • Technical analysis skills               │
│  • Business value delivered                │
├────────────────────────────────────────────┤
│  CTA: Want custom analysis?                │
└────────────────────────────────────────────┘
│  FOOTER                                    │
└────────────────────────────────────────────┘
```

---

### 4. Healthcare Cost Project (`/projects/healthcare-cost/index.html`) — NEW

**Purpose:** Showcase cost analysis project

**Structure:**
1. **Hero**
   - "Healthcare Cost Analysis"
   - "What does it actually cost to deliver healthcare in the United States?"

2. **The Question**
   - We spend $4.5 trillion/year
   - How much is direct care vs. overhead/waste?
   - Why this matters

3. **Our Approach** (4 methods)
   - Provider capacity model
   - Facility cost model
   - Claims bottom-up
   - Top-down national accounting
   - Why multiple methods? (Triangulation = confidence)

4. **Data Sources**
   - Hospital cost reports
   - Medicare utilization
   - NHEA national spending tables
   - Literature sources

5. **Status & Next Steps**
   - Research phase: Data collected, methodology designed
   - Coming soon: Initial findings, interactive calculator
   - [Follow the project] (link to personal site blog, or newsletter signup)

6. **CTA**
   - "Want this analysis for your region or health system?"
   - [Contact us] button

**Visual elements:**
- Infographic: The $4.5T breakdown (where money goes)
- Diagram: 4 calculation approaches
- Chart: US vs. peer country spending (placeholder, will fill in later)

---

### 5. Fraud Analysis Project (`/projects/fraud-analysis/index.html`) — NEW

**Purpose:** Showcase fraud detection project

**Structure:**
1. **Hero**
   - "Fraud Detection & Anomaly Analysis"
   - "Finding the needles in the haystack: $60-100B in fraud annually"

2. **Why This Matters**
   - Scale: 3-10% of total healthcare spending
   - Techniques: Statistical, geographic, network-based
   - Value: Early detection saves billions

3. **Five Detection Approaches**
   - Statistical outlier detection (billing anomalies)
   - Prescribing pattern analysis (pill mills)
   - Geographic clustering (fraud hotspots)
   - Network analysis (referral rings)
   - Time series anomalies (COVID scams)
   - Each with icon + 2-sentence description

4. **Data Sources**
   - Medicare Part B & Part D utilization
   - Open Payments (Sunshine Act)
   - Provider affiliations
   - LEIE exclusion list (validation)

5. **Status & Next Steps**
   - Design phase: Methodology complete
   - Coming soon: Case studies, interactive explorer
   - [Follow the project]

6. **CTA**
   - "Want fraud detection for your claims data?"
   - [Contact us] button

**Visual elements:**
- Heatmap placeholder: "Medicare fraud hotspots" (coming soon)
- Network diagram sketch: Referral ring example
- Chart: Outlier detection illustration

---

### 6. Data Access (`/access/index.html`) — NEW

**Purpose:** Explain how to access the data, capture leads via request form

**Structure:**
1. **Hero**
   - "Access CMS Healthcare Data"
   - "Three ways to work with 100M+ Medicare records"

2. **Access Tiers**
   
   **Tier 1: Public API (Free)**
   - Query our database via API
   - Rate limited: 100 requests/min
   - Max 1,000 rows per query
   - [View API Documentation] button (links to /api/docs)
   
   **Tier 2: Research Access (Email required)**
   - Larger queries (up to 10K rows)
   - Bulk table exports (CSV/Parquet)
   - Database snapshot download (6GB)
   - [Request Access] button → Form
   
   **Tier 3: Enterprise API (Contact sales)**
   - Unlimited queries
   - Real-time updates
   - Custom transformations
   - Azure deployment
   - [Contact Us] button

3. **Request Form** (for Tier 2)
   ```
   Name: _______________
   Email: _______________
   Organization: _______________
   Use case: [dropdown: Research, Product, Healthcare Org, Student, Other]
   Description: [textarea] Tell us what you're working on
   
   [Submit Request]
   ```
   
   On submit: 
   - Sends to Slack webhook
   - Shows confirmation: "Thanks! We'll review and respond within 24-48 hours."

4. **API Quick Start**
   - Code sample: curl command to query API
   - Link to full API documentation
   - Example query results

5. **Data Attribution**
   - "Data provided by CMS via data.cms.gov"
   - "We aggregate, transform, and serve public CMS datasets"
   - "Not affiliated with CMS"

**Wireframe:**
```
┌────────────────────────────────────────────┐
│  NAVIGATION BAR                            │
├────────────────────────────────────────────┤
│  Access CMS Healthcare Data                │
│  Three ways to work with 100M+ records     │
├────────────────────────────────────────────┤
│  ┌────────────────────────────────────┐   │
│  │  Tier 1: Public API (Free)         │   │
│  │  • Query via API                   │   │
│  │  • 100 req/min                     │   │
│  │  [View API Docs]                   │   │
│  └────────────────────────────────────┘   │
│  ┌────────────────────────────────────┐   │
│  │  Tier 2: Research Access (Email)   │   │
│  │  • Bulk exports                    │   │
│  │  • Database download               │   │
│  │  [Request Access]                  │   │
│  └────────────────────────────────────┘   │
│  ┌────────────────────────────────────┐   │
│  │  Tier 3: Enterprise (Contact)      │   │
│  │  • Unlimited queries               │   │
│  │  • Custom deployment               │   │
│  │  [Contact Us]                      │   │
│  └────────────────────────────────────┘   │
├────────────────────────────────────────────┤
│  Request Form (appears on Tier 2 click)   │
│  Name: _______                             │
│  Email: _______                            │
│  ...                                       │
└────────────────────────────────────────────┘
```

---

### 7. About (`/about/index.html`) — NEW

**Purpose:** Credibility, positioning, contact info

**Structure:**
1. **Hero**
   - "About HealthcareDataAI"
   - "Free and open-source healthcare data platform"

2. **The Mission**
   - Make CMS data accessible
   - Showcase what's possible with healthcare data
   - Help organizations build intelligence systems

3. **The Builder**
   - Blake Thomson
   - 2+ years at Cedars-Sinai Business Development (the "data guru")
   - MS in Biomedical Engineering
   - Background: biotech startup → consulting → health system strategy

4. **The Platform**
   - 107M records across 30 datasets
   - Updated quarterly (following CMS release schedule)
   - Built with: DuckDB, FastAPI, Python, nginx
   - Open-source approach (GitHub repo linked)

5. **Services** (lead gen)
   - Bespoke AI agents for healthcare organizations
   - Custom intelligence platforms
   - Data strategy consulting
   - [Schedule a call] or [Email: blake@blakethomson.com]

6. **FAQ**
   - Where does the data come from? (data.cms.gov public use files)
   - Is this affiliated with CMS? (No, independent)
   - Can I use this commercially? (Yes, within CMS terms)
   - How often is it updated? (Quarterly)

**Wireframe:**
```
┌────────────────────────────────────────────┐
│  NAVIGATION BAR                            │
├────────────────────────────────────────────┤
│  About HealthcareDataAI                    │
│  Free healthcare data platform             │
├────────────────────────────────────────────┤
│  The Mission                               │
│  • Make CMS data accessible                │
│  • Showcase possibilities                  │
├────────────────────────────────────────────┤
│  The Builder                               │
│  Blake Thomson                             │
│  [Photo]                                   │
│  Cedars-Sinai BD • 2+ years                │
├────────────────────────────────────────────┤
│  The Platform                              │
│  107M records • 30 datasets                │
│  DuckDB + FastAPI + Python                 │
├────────────────────────────────────────────┤
│  Services                                  │
│  Custom AI agents & intelligence platforms │
│  [Contact Us]                              │
├────────────────────────────────────────────┤
│  FAQ                                       │
│  Common questions...                       │
└────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Core Structure (This Week)
- [ ] Create `/index.html` (homepage)
- [ ] Create `/projects/index.html` (portfolio overview)
- [ ] Create `/projects/healthcare-cost/index.html`
- [ ] Create `/projects/fraud-analysis/index.html`
- [ ] Create `/access/index.html` (data access + form)
- [ ] Create `/about/index.html`
- [ ] Update `/dashboard/index.html` (add nav + footer)
- [ ] Create shared components:
  - [ ] `components/nav.html` (navigation bar)
  - [ ] `components/footer.html` (footer)
  - [ ] `styles/global.css` (shared styles)

### Phase 2: Content & Polish (Next Week)
- [ ] Write full content for each page (expand outlines)
- [ ] Add placeholder images/charts
- [ ] Implement request form (Netlify Forms or Formspree)
- [ ] Set up Slack webhook for form submissions
- [ ] Test all navigation flows
- [ ] Mobile responsiveness check

### Phase 3: Interactivity (Week 3)
- [ ] API documentation page (auto-generated from FastAPI /docs)
- [ ] Code samples for API access
- [ ] Interactive elements (collapsible sections, tabs)
- [ ] Analytics integration (Plausible or Google Analytics)

---

## Design System (Maintain Current Style)

**Colors** (from current dashboard):
- Background: `#0f172a` (midnight blue)
- Surface: `#1e293b` (slate)
- Border: `#334155` (gray)
- Text: `#e2e8f0` (light gray)
- Accent: `#38bdf8` (cyan)
- Green: `#4ade80`
- Orange: `#fb923c`
- Purple: `#a78bfa`

**Typography:**
- Sans-serif: -apple-system, BlinkMacSystemFont, 'Segoe UI'
- Monospace: 'SF Mono', 'Fira Code', monospace (for code)

**Components:**
- Cards with rounded corners (10px)
- Buttons: accent color, 6px border radius
- Hover states: slight color shift or border highlight

**Keep current vibe:** Technical, data-forward, clean

---

## Technical Notes

### File Organization
```
/home/dataops/cms-data/
├── dashboard/
│   └── index.html           (existing, update)
├── frontend/                (NEW directory)
│   ├── index.html           (homepage)
│   ├── projects/
│   ├── access/
│   ├── about/
│   ├── components/          (shared nav, footer)
│   ├── styles/
│   │   ├── global.css
│   │   └── pages.css
│   └── assets/
│       └── images/
└── api/                     (existing backend)
```

### nginx Configuration
Update `/opt/personal-website/nginx/conf.d/healthcaredataai.conf`:

```nginx
location / {
    root /home/dataops/cms-data/frontend;
    try_files $uri $uri/ /index.html;
}

location /dashboard/ {
    root /home/dataops/cms-data;
    try_files $uri $uri/ /dashboard/index.html;
}

location /api/ {
    proxy_pass http://172.18.0.1:8080/;
    # ... existing proxy config
}
```

### Form Handling Options
**Option 1**: Netlify Forms (if we migrate to Netlify)  
**Option 2**: Formspree.io (free tier, 50 submissions/month)  
**Option 3**: Custom endpoint (FastAPI route, store in SQLite)  

**Recommend**: Formspree.io for now (quick setup), migrate to custom if volume grows

---

## Success Metrics

**Traffic:**
- Homepage: 1,000 unique visitors/month (6 months)
- Dashboard: 500 active users/month
- Projects: 200 visitors/page/month

**Engagement:**
- Avg session duration: 3+ minutes
- Pages per session: 2.5+
- Bounce rate: <60%

**Conversions:**
- Data access requests: 20/month
- Email contacts: 5/month
- Sales conversations: 2/month

---

## Next Actions

1. ✅ Document site architecture (this file)
2. Create homepage HTML
3. Create projects overview page
4. Create project detail pages (2)
5. Create data access page
6. Create about page
7. Update dashboard with navigation
8. Deploy to production
9. Test all flows
10. Set up analytics

---

**Owner**: Blake Thomson  
**Collaborator**: Chief  
**Status**: Design phase  
**Target launch**: 2026-03-10 (1 week)

---

*Site architecture created 2026-03-03 by Chief*
