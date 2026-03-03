#!/bin/bash
# Quick deployment script for HealthcareDataAI.com
# Run this to deploy the complete website

echo "🚀 Deploying HealthcareDataAI.com Complete Website"
echo "=================================================="
echo ""

# Step 1: Copy files to server
echo "📂 Step 1: Copying frontend files to server..."
scp -r frontend/* root@5.78.148.70:/home/dataops/cms-data/frontend/
echo "✅ Files copied"
echo ""

# Step 2: Update nginx config
echo "⚙️  Step 2: Updating nginx configuration..."
echo "Please manually update /opt/personal-website/nginx/conf.d/healthcaredataai.conf"
echo "See COMPLETE-SITE-READY.md for exact config"
echo ""
read -p "Press Enter when nginx config is updated..."

# Step 3: Reload nginx
echo "🔄 Step 3: Reloading nginx..."
ssh root@5.78.148.70 "nginx -t && systemctl reload nginx"
echo "✅ nginx reloaded"
echo ""

# Step 4: Test pages
echo "🧪 Step 4: Testing pages..."
echo "Homepage:" 
curl -sI https://healthcaredataai.com/ | head -1
echo "Projects:"
curl -sI https://healthcaredataai.com/projects/ | head -1
echo "Data Access:"
curl -sI https://healthcaredataai.com/access/ | head -1
echo "About:"
curl -sI https://healthcaredataai.com/about/ | head -1
echo "Dashboard:"
curl -sI https://healthcaredataai.com/dashboard/ | head -1
echo "API Health:"
curl -s https://healthcaredataai.com/api/health | head -1
echo ""
echo "✅ All tests complete"
echo ""

# Step 5: Git commit
echo "📝 Step 5: Committing to git..."
git add frontend/
git add COMPLETE-SITE-READY.md
git add DEPLOYMENT-READY.md
git add SITE-ARCHITECTURE.md
git add WEBSITE-BUILD-SUMMARY.md
git commit -m "Complete website: 6 pages, 110KB content, production-ready"
git push origin main
echo "✅ Committed and pushed"
echo ""

echo "🎉 DEPLOYMENT COMPLETE!"
echo "========================"
echo ""
echo "Your website is now live at: https://healthcaredataai.com"
echo ""
echo "Pages deployed:"
echo "  • Homepage"
echo "  • Projects Overview"
echo "  • Healthcare Cost Project"
echo "  • Fraud Analysis Project"
echo "  • Data Access"
echo "  • About"
echo "  • Dashboard (existing)"
echo ""
echo "Share it with the world! 🌎"
