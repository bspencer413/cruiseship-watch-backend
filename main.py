# ============================================================================
# CW BACKEND PATCH — v0.1.9 → v0.1.10
# ============================================================================
# Two changes in main.py:
#
#   1. Bump VERSION constant near top of file:
#         OLD:  VERSION = "0.1.9"
#         NEW:  VERSION = "0.1.10"
#
#   2. Add the new endpoint below RIGHT AFTER the existing admin_signup_stats
#      function (search for "async def admin_signup_stats" and add the new
#      function block immediately after its closing `return` block).
#
#      Same admin token (ADMIN_STATS_TOKEN env var), same response shape —
#      just queries tw_users instead of users.
#
# Commit, Render auto-redeploys, scoreboard Travel Watch card lights up.
# ============================================================================


@app.get("/tw/admin/signup-stats")
async def tw_admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    """Read-only Travel Watch signup metrics for the 3Brains scoreboard.
    Same backend service as Cruise Ship Watch; same ADMIN_STATS_TOKEN env var.
    Queries tw_users (the Travel Watch user table) instead of CW's users."""
    expected = os.environ.get("ADMIN_STATS_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tw_users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM tw_users WHERE created_at >= NOW() - INTERVAL '24 hours'")
        signups_24h = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM tw_users WHERE created_at >= NOW() - INTERVAL '7 days'")
        signups_7d = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM tw_users WHERE created_at >= NOW() - INTERVAL '30 days'")
        signups_30d = c.fetchone()[0]
        c.execute("SELECT MAX(created_at) FROM tw_users")
        latest_row = c.fetchone()
        latest = latest_row[0].isoformat() if latest_row and latest_row[0] else None
        return {
            "total_users": total_users,
            "signups_24h": signups_24h,
            "signups_7d": signups_7d,
            "signups_30d": signups_30d,
            "latest_signup_at": latest
        }
