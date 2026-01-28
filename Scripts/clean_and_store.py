import mysql.connector
from mysql.connector import Error
import sys

# -------------------------------
# DB CONFIG (adjust if needed)
# -------------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "springstudent",
    "password": "springstudent",
    "database": "fpladmin",
    "port": 3306,
}

# -------------------------------
# Helper
# -------------------------------
def log(step, msg):
    print(f"[{step}] {msg}")

# -------------------------------
# MAIN
# -------------------------------
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # -------------------------------------------------
    # [1/10] Create clean tables
    # -------------------------------------------------
    log("1/10", "Creating clean tables...")

    cur.execute("DROP TABLE IF EXISTS clean_team_season_stats")
    cur.execute("DROP TABLE IF EXISTS clean_team_xg_season")
    cur.execute("DROP TABLE IF EXISTS clean_team_season_metrics")

    cur.execute("""
    CREATE TABLE clean_team_season_stats (
        season VARCHAR(10),
        team_id INT,
        team_name VARCHAR(255),
        played INT,
        wins INT,
        draws INT,
        losses INT,
        gf INT,
        ga INT,
        gd INT,
        pts INT,
        PRIMARY KEY (season, team_id)
    )
    """)

    cur.execute("""
    CREATE TABLE clean_team_xg_season (
        season INT,
        team_name VARCHAR(255),
        xg_for FLOAT,
        xg_against FLOAT,
        PRIMARY KEY (season, team_name)
    )
    """)

    cur.execute("""
    CREATE TABLE clean_team_season_metrics (
        season VARCHAR(10),
        team_id INT,
        team_name VARCHAR(255),
        played INT,
        wins INT,
        draws INT,
        losses INT,
        gf INT,
        ga INT,
        gd INT,
        pts INT,
        xg_for FLOAT,
        xg_against FLOAT,
        PRIMARY KEY (season, team_id)
    )
    """)

    conn.commit()
    log("2/10", "Tables ready")

    # -------------------------------------------------
    # [3/10] Aggregate FPL team season stats
    # -------------------------------------------------
    log("3/10", "Aggregating FPL team season stats...")

    cur.execute("""
    SELECT
        season,
        team_id,
        COUNT(*) AS played,
        SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) AS losses,
        SUM(goals_for) AS gf,
        SUM(goals_against) AS ga,
        SUM(points) AS pts
    FROM (
        SELECT
            season,
            team_h AS team_id,
            team_h_score AS goals_for,
            team_a_score AS goals_against,
            CASE
                WHEN team_h_score > team_a_score THEN 3
                WHEN team_h_score = team_a_score THEN 1
                ELSE 0
            END AS points
        FROM fpl_fixtures
        WHERE finished = 1

        UNION ALL

        SELECT
            season,
            team_a AS team_id,
            team_a_score AS goals_for,
            team_h_score AS goals_against,
            CASE
                WHEN team_a_score > team_h_score THEN 3
                WHEN team_a_score = team_h_score THEN 1
                ELSE 0
            END AS points
        FROM fpl_fixtures
        WHERE finished = 1
    ) t
    GROUP BY season, team_id
    """)

    fpl_rows = cur.fetchall()
    log("4/10", f"Computed {len(fpl_rows)} FPL team-season rows")

    # -------------------------------------------------
    # [5/10] Attach team names (FPL only)
    # -------------------------------------------------
    log("5/10", "Attaching team names from fpl_season_teams...")

    cur.execute("""
    SELECT season, team_id, team_name
    FROM fpl_season_teams
    """)
    team_map = {
        (r["season"], r["team_id"]): r["team_name"]
        for r in cur.fetchall()
    }

    clean_fpl = []
    for r in fpl_rows:
        key = (r["season"], r["team_id"])
        team_name = team_map.get(key)

        if not team_name:
            continue  # skip unsafe rows

        clean_fpl.append({
            "season": r["season"],
            "team_id": r["team_id"],
            "team_name": team_name,
            "played": r["played"],
            "wins": r["wins"],
            "draws": r["draws"],
            "losses": r["losses"],
            "gf": r["gf"],
            "ga": r["ga"],
            "gd": r["gf"] - r["ga"],
            "pts": r["pts"],
        })

    log("6/10", f"Clean FPL rows: {len(clean_fpl)}")

    # Insert clean FPL stats
    cur.executemany("""
    INSERT INTO clean_team_season_stats
    (season, team_id, team_name, played, wins, draws, losses, gf, ga, gd, pts)
    VALUES (%(season)s, %(team_id)s, %(team_name)s,
            %(played)s, %(wins)s, %(draws)s, %(losses)s,
            %(gf)s, %(ga)s, %(gd)s, %(pts)s)
    """, clean_fpl)

    conn.commit()

    # -------------------------------------------------
    # [7/10] Aggregate Understat xG (team-level only)
    # -------------------------------------------------
    log("7/10", "Aggregating Understat team xG...")

    cur.execute("""
    SELECT
        season,
        team_name,
        ROUND(SUM(xg_for), 2) AS xg_for,
        ROUND(SUM(xg_against), 2) AS xg_against
    FROM (
        SELECT
            season,
            team_h AS team_name,
            h_xg AS xg_for,
            a_xg AS xg_against
        FROM understat_team_metrics

        UNION ALL

        SELECT
            season,
            team_a AS team_name,
            a_xg AS xg_for,
            h_xg AS xg_against
        FROM understat_team_metrics
    ) u
    GROUP BY season, team_name
    """)

    xg_rows = cur.fetchall()
    log("8/10", f"Computed {len(xg_rows)} Understat rows")

    cur.executemany("""
    INSERT INTO clean_team_xg_season
    (season, team_name, xg_for, xg_against)
    VALUES (%(season)s, %(team_name)s, %(xg_for)s, %(xg_against)s)
    """, xg_rows)

    conn.commit()

    # -------------------------------------------------
    # [9/10] Merge FPL + Understat in Python
    # -------------------------------------------------
    log("9/10", "Merging FPL stats with Understat xG...")

    cur.execute("SELECT * FROM clean_team_season_stats")
    fpl = cur.fetchall()

    cur.execute("SELECT * FROM clean_team_xg_season")
    xg = cur.fetchall()

    xg_lookup = {
        (str(r["season"]), r["team_name"].lower().strip()):
        (r["xg_for"], r["xg_against"])
        for r in xg
    }

    merged = []
    for r in fpl:
        season_start = r["season"].split("-")[0]
        key = (season_start, r["team_name"].lower().strip())
        xg_for, xg_against = xg_lookup.get(key, (None, None))

        merged.append({
            **r,
            "xg_for": xg_for,
            "xg_against": xg_against,
        })

    # -------------------------------------------------
    # [10/10] Store final clean metrics
    # -------------------------------------------------
    cur.executemany("""
    INSERT INTO clean_team_season_metrics
    (season, team_id, team_name, played, wins, draws, losses, gf, ga, gd, pts, xg_for, xg_against)
    VALUES (%(season)s, %(team_id)s, %(team_name)s,
            %(played)s, %(wins)s, %(draws)s, %(losses)s,
            %(gf)s, %(ga)s, %(gd)s, %(pts)s,
            %(xg_for)s, %(xg_against)s)
    """, merged)

    conn.commit()
    log("10/10", "Cleaning data and storing into tables ✅")

except Error as e:
    print("ERROR: cleaning and storing failed")
    print(e)
    sys.exit(1)

finally:
    if "cur" in locals():
        cur.close()
    if "conn" in locals() and conn.is_connected():
        conn.close()
