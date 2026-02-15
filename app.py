from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client, Client
from sqlalchemy import create_engine

app = Flask(__name__, static_folder="static")

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DATABASE_URL = os.environ.get('DATABASE_URL')

CORS(app, resources={
    r"/*": {
        "origins": [ "https://schipperstatlines.onrender.com", "https://website-a7a.pages.dev", "http://127.0.0.1:5501", "http://localhost:5501", "http://127.0.0.1:5500", "http://localhost:5500", "https://noahschipper.net"],
    }
})

def get_db_engine():
    """Create SQLAlchemy engine for database connections"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    # Create engine with connection pooling
    engine = create_engine(
        database_url,
        pool_pre_ping=True,   # Verify connections before use
        pool_recycle=300,      # Recycle connections every 5 minutes
        echo=False             # Set to True for SQL debugging
    )
    
    return engine

db_engine = get_db_engine()

def get_supabase_client():
    """Get Supabase client for easier operations"""
    return supabase


# ─── UNIFIED TEAM DATA ──────────────────────────────────────────────────────
# Single source for every team code.
# Keys are Lahman DB team codes. Each value contains:
#   name       – display name
#   espn       – ESPN logo abbreviation (lowercase)
#   mlb_id     – MLB Stats API numeric team ID (for mlbstatic URLs)
#   aliases    – lowercase search terms that map to this code
#   alt_codes  – other DB codes that should resolve to this entry
TEAMS = {
    # Angels
    "LAA": {"name": "Los Angeles Angels",    "espn": "laa", "mlb_id": "108", "aliases": ["angels", "los angeles angels"],                     "alt_codes": ["ALT", "CAL", "ANA"]},
    "CAL": {"name": "California Angels",     "espn": "laa", "mlb_id": "108", "aliases": ["california angels", "anaheim angels"],               "alt_codes": ["ANA"]},
    "ANA": {"name": "Anaheim Angels",        "espn": "laa", "mlb_id": "108", "aliases": [],                                                    "alt_codes": []},
    # Diamondbacks
    "ARI": {"name": "Arizona Diamondbacks",  "espn": "ari", "mlb_id": "109", "aliases": ["diamondbacks", "arizona diamondbacks", "d-backs", "dbacks"], "alt_codes": []},
    # Braves
    "ATL": {"name": "Atlanta Braves",        "espn": "atl", "mlb_id": "144", "aliases": ["braves", "atlanta braves", "atlanta"],               "alt_codes": ["BSN", "ML1", "MLA"]},
    "BSN": {"name": "Boston Braves",         "espn": "atl", "mlb_id": "144", "aliases": ["boston braves"],                                     "alt_codes": []},
    "ML1": {"name": "Milwaukee Braves",      "espn": "atl", "mlb_id": "144", "aliases": ["milwaukee braves"],                                 "alt_codes": ["MLA"]},
    # Orioles
    "BAL": {"name": "Baltimore Orioles",     "espn": "bal", "mlb_id": "110", "aliases": ["orioles", "baltimore orioles", "baltimore", "o's"],  "alt_codes": ["SLA"]},
    "SLA": {"name": "St. Louis Browns",      "espn": "bal", "mlb_id": "110", "aliases": ["st. louis browns", "st louis browns", "browns"],     "alt_codes": []},
    # Red Sox
    "BOS": {"name": "Boston Red Sox",        "espn": "bos", "mlb_id": "111", "aliases": ["red sox", "boston red sox", "boston", "redsox"],      "alt_codes": ["BS1"]},
    # Cubs
    "CHN": {"name": "Chicago Cubs",          "espn": "chc", "mlb_id": "112", "aliases": ["cubs", "chicago cubs", "cubbies"],                  "alt_codes": ["CHC"]},
    # White Sox
    "CHA": {"name": "Chicago White Sox",     "espn": "cws", "mlb_id": "145", "aliases": ["white sox", "chicago white sox", "whitesox"],        "alt_codes": ["CHW", "CWS"]},
    # Reds
    "CIN": {"name": "Cincinnati Reds",       "espn": "cin", "mlb_id": "113", "aliases": ["reds", "cincinnati reds", "cincinnati"],             "alt_codes": ["CN2", "CN3"]},
    # Guardians / Indians
    "CLE": {"name": "Cleveland Guardians",   "espn": "cle", "mlb_id": "114", "aliases": ["guardians", "cleveland guardians", "cleveland", "indians", "cleveland indians"], "alt_codes": []},
    # Rockies
    "COL": {"name": "Colorado Rockies",      "espn": "col", "mlb_id": "115", "aliases": ["rockies", "colorado rockies", "colorado"],           "alt_codes": []},
    # Tigers
    "DET": {"name": "Detroit Tigers",        "espn": "det", "mlb_id": "116", "aliases": ["tigers", "detroit tigers", "detroit"],               "alt_codes": []},
    # Astros
    "HOU": {"name": "Houston Astros",        "espn": "hou", "mlb_id": "117", "aliases": ["astros", "houston astros", "houston"],               "alt_codes": []},
    # Royals
    "KCA": {"name": "Kansas City Royals",    "espn": "kc",  "mlb_id": "118", "aliases": ["royals", "kansas city royals", "kansas city"],       "alt_codes": ["KCR"]},
    # Dodgers
    "LAN": {"name": "Los Angeles Dodgers",   "espn": "lad", "mlb_id": "119", "aliases": ["dodgers", "los angeles dodgers"],                   "alt_codes": ["LAD", "BR1", "BR2", "BR4", "BRO"]},
    "BR2": {"name": "Brooklyn Dodgers",      "espn": "lad", "mlb_id": "119", "aliases": ["brooklyn dodgers"],                                 "alt_codes": ["BR1", "BR4", "BRO"]},
    # Marlins
    "MIA": {"name": "Miami Marlins",         "espn": "mia", "mlb_id": "146", "aliases": ["marlins", "miami marlins", "miami"],                "alt_codes": ["FLA"]},
    "FLA": {"name": "Florida Marlins",       "espn": "mia", "mlb_id": "146", "aliases": ["florida marlins"],                                  "alt_codes": []},
    # Brewers
    "MIL": {"name": "Milwaukee Brewers",     "espn": "mil", "mlb_id": "158", "aliases": ["brewers", "milwaukee brewers", "milwaukee"],         "alt_codes": ["ML4"]},
    # Twins
    "MIN": {"name": "Minnesota Twins",       "espn": "min", "mlb_id": "142", "aliases": ["twins", "minnesota twins", "minnesota"],             "alt_codes": ["WS1"]},
    # Mets
    "NYN": {"name": "New York Mets",         "espn": "nym", "mlb_id": "121", "aliases": ["mets", "new york mets", "ny mets"],                 "alt_codes": ["NYM"]},
    # Yankees
    "NYA": {"name": "New York Yankees",      "espn": "nyy", "mlb_id": "147", "aliases": ["yankees", "new york yankees", "ny yankees", "yanks"], "alt_codes": ["NYY"]},
    # Athletics
    "OAK": {"name": "Oakland Athletics",     "espn": "oak", "mlb_id": "133", "aliases": ["athletics", "oakland athletics", "oakland", "a's", "as"], "alt_codes": ["PHA", "KC1"]},
    "PHA": {"name": "Philadelphia Athletics", "espn": "oak", "mlb_id": "133", "aliases": ["philadelphia athletics"],                           "alt_codes": []},
    # Phillies
    "PHI": {"name": "Philadelphia Phillies", "espn": "phi", "mlb_id": "143", "aliases": ["phillies", "philadelphia phillies", "philadelphia", "phils"], "alt_codes": ["PHN", "PH3"]},
    # Pirates
    "PIT": {"name": "Pittsburgh Pirates",    "espn": "pit", "mlb_id": "134", "aliases": ["pirates", "pittsburgh pirates", "pittsburgh", "bucs"], "alt_codes": ["PT1"]},
    # Padres
    "SDN": {"name": "San Diego Padres",      "espn": "sd",  "mlb_id": "135", "aliases": ["padres", "san diego padres", "san diego"],          "alt_codes": ["SDP", "SD"]},
    # Mariners
    "SEA": {"name": "Seattle Mariners",      "espn": "sea", "mlb_id": "136", "aliases": ["mariners", "seattle mariners", "seattle", "m's"],    "alt_codes": []},
    # Giants
    "SFN": {"name": "San Francisco Giants",  "espn": "sf",  "mlb_id": "137", "aliases": ["giants", "san francisco giants", "sf giants", "san francisco"], "alt_codes": ["SFG", "SF"]},
    "NY1": {"name": "New York Giants",       "espn": "sf",  "mlb_id": "137", "aliases": ["new york giants"],                                  "alt_codes": []},
    # Cardinals
    "SLN": {"name": "St. Louis Cardinals",   "espn": "stl", "mlb_id": "138", "aliases": ["cardinals", "st. louis cardinals", "st louis cardinals", "cards", "st. louis", "st louis"], "alt_codes": ["SL4", "STL"]},
    # Rays
    "TBA": {"name": "Tampa Bay Rays",        "espn": "tb",  "mlb_id": "139", "aliases": ["rays", "tampa bay rays", "tampa bay"],              "alt_codes": ["TB"]},
    "TBD": {"name": "Tampa Bay Devil Rays",  "espn": "tb",  "mlb_id": "139", "aliases": ["devil rays", "tampa bay devil rays"],               "alt_codes": []},
    # Rangers
    "TEX": {"name": "Texas Rangers",         "espn": "tex", "mlb_id": "140", "aliases": ["rangers", "texas rangers", "texas"],                "alt_codes": ["WS2"]},
    # Blue Jays
    "TOR": {"name": "Toronto Blue Jays",     "espn": "tor", "mlb_id": "141", "aliases": ["blue jays", "toronto blue jays", "toronto", "jays", "bluejays"], "alt_codes": []},
    # Nationals / Expos
    "WAS": {"name": "Washington Nationals",  "espn": "wsh", "mlb_id": "120", "aliases": ["nationals", "washington nationals", "washington", "nats"], "alt_codes": ["WSN"]},
    "MON": {"name": "Montreal Expos",        "espn": "wsh", "mlb_id": "120", "aliases": ["expos", "montreal expos"],                          "alt_codes": []},
    # Historical - Senators
    "WS1": {"name": "Washington Senators",   "espn": "min", "mlb_id": "142", "aliases": ["washington senators", "senators"],                  "alt_codes": []},
}

# ── Derived lookup tables (built once at import time) ────────────────────────

# Map alternate codes → primary code  (e.g. "CHC" → "CHN")
_CODE_TO_PRIMARY = {}
for _code, _info in TEAMS.items():
    _CODE_TO_PRIMARY[_code.lower()] = _code  # self-maps
    for _alt in _info.get("alt_codes", []):
        _CODE_TO_PRIMARY[_alt.lower()] = _code

# Map lowercase alias → primary code  (e.g. "cubs" → "CHN")
_ALIAS_TO_CODE = {}
for _code, _info in TEAMS.items():
    for _alias in _info.get("aliases", []):
        _ALIAS_TO_CODE[_alias] = _code


KNOWN_TWO_WAY_PLAYERS = {
    # Modern era two-way players
    "ohtansh01": "Shohei Ohtani",
    # Historical two-way players (primarily known for both)
    "ruthba01": "Babe Ruth",
    # Players who had significant time as both (adjust as needed)
    # Format: 'playerid': 'Display Name'
}


def is_predefined_two_way_player(playerid):
    """Check if player is in predefined list of two-way players"""
    return playerid in KNOWN_TWO_WAY_PLAYERS


def detect_two_way_player_simple(playerid, conn):
    """Simplified two-way detection using only predefined list"""
    if is_predefined_two_way_player(playerid):
        return "two-way"

    # For all other players, use the original detection logic
    return detect_player_type(playerid, conn)


def get_photo_url_for_player(playerid, conn):
    """No photo URL - frontend will handle images"""
    return None

def get_world_series_championships(playerid, conn=None):
    """Get World Series championships for a player"""
    from sqlalchemy import text
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        ws_query = text("""
        SELECT DISTINCT b.yearid, b.teamid, s.name as team_name
        FROM lahman_batting b
        JOIN lahman_seriespost sp ON b.yearid = sp.yearid AND b.teamid = sp.teamidwinner
        LEFT JOIN lahman_teams s ON b.teamid = s.teamid AND b.yearid = s.yearid
        WHERE b.playerid = :playerid AND sp.round = 'WS'
        
        UNION
        
        SELECT DISTINCT p.yearid, p.teamid, s.name as team_name
        FROM lahman_pitching p
        JOIN lahman_seriespost sp ON p.yearid = sp.yearid AND p.teamid = sp.teamidwinner
        LEFT JOIN lahman_teams s ON p.teamid = s.teamid AND p.yearid = s.yearid
        WHERE p.playerid = :playerid AND sp.round = 'WS'
        
        ORDER BY 1 DESC
        """)

        ws_results = conn.execute(ws_query, {"playerid": playerid}).fetchall()

        championships = []
        for row in ws_results:
            year, team_id, team_name = row
            championships.append(
                {"year": year, "team": team_id, "team_name": team_name or team_id}
            )

        return championships

    except Exception as e:
        # Fallback: check awards table for WS entries
        try:
            fallback_query = text("""
                SELECT yearid, notes
                FROM lahman_awardsplayers 
                WHERE playerid = :playerid AND awardid = 'WS'
                ORDER BY yearid DESC
            """)

            fallback_results = conn.execute(fallback_query, {"playerid": playerid}).fetchall()
                
            championships = []
            for year, notes in fallback_results:
                championships.append(
                    {
                        "year": year,
                        "team": "Unknown",
                        "team_name": notes or "World Series Champion",
                    })
                
            return championships

        except Exception as e2:
            return []
        finally:
            if owns_conn:
                conn.close()


def get_career_war(playerid):
    """Get career WAR from JEFFBAGWELL database"""
    from sqlalchemy import text
    
    try:
        query = text("""
            SELECT SUM(WAR162) as career_war 
            FROM jeffbagwell_war 
            WHERE key_bbref = :playerid
        """)

        with db_engine.connect() as conn:
            result = conn.execute(query, {"playerid": playerid}).fetchone()

        if result and result[0] is not None:
            return float(result[0])
        return 0.0

    except Exception as e:
        return 0.0


def get_season_war_history(playerid):
    """Get season-by-season WAR from JEFFBAGWELL database"""
    from sqlalchemy import text
    
    try:
        query = text("""
            SELECT year_ID, WAR162 as war
            FROM jeffbagwell_war 
            WHERE key_bbref = :playerid
            ORDER BY year_ID DESC
        """)
        
        df = pd.read_sql_query(query, db_engine, params={"playerid": playerid})
    
        if 'year_ID' in df.columns:
            df = df.rename(columns={'year_ID': 'yearid'})
        
        return df

    except Exception as e:
        print(f"get_season_war_history error: {e}")
        return pd.DataFrame()

def detect_player_type(playerid, conn=None):
    """Detect if player is primarily a pitcher or hitter based on their stats"""
    from sqlalchemy import text
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        pitching_query = text("""
        SELECT COUNT(*) as pitch_seasons, SUM(g) as total_games_pitched, SUM(gs) as total_starts
        FROM lahman_pitching WHERE playerid = :playerid
        """)

        batting_query = text("""
        SELECT COUNT(*) as bat_seasons, SUM(g) as total_games_batted, SUM(ab) as total_at_bats
        FROM lahman_batting WHERE playerid = :playerid
        """)

        pitch_result = conn.execute(pitching_query, {"playerid": playerid}).fetchone()
        bat_result = conn.execute(batting_query, {"playerid": playerid}).fetchone()

        pitch_seasons = pitch_result[0] if pitch_result else 0
        total_games_pitched = pitch_result[1] if pitch_result and pitch_result[1] else 0
        total_starts = pitch_result[2] if pitch_result and pitch_result[2] else 0

        bat_seasons = bat_result[0] if bat_result else 0
        total_games_batted = bat_result[1] if bat_result and bat_result[1] else 0
        total_at_bats = bat_result[2] if bat_result and bat_result[2] else 0

        if pitch_seasons >= 3 or total_games_pitched >= 50 or total_starts >= 10:
            return "pitcher"
        elif bat_seasons >= 3 or total_at_bats >= 300:
            return "hitter"
        else:
            return "pitcher" if pitch_seasons > 0 else "hitter"
    finally:
        if owns_conn:
            conn.close()


def get_player_awards(playerid, conn=None):
    """Get all awards for a player from the lahman database"""
    from sqlalchemy import text
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        # Query for all awards
        awards_query = text("""
        SELECT yearid, awardid, lgid, tie, notes
        FROM lahman_awardsplayers 
        WHERE playerid = :playerid
        ORDER BY yearid DESC, awardid
        """)

        awards_data = conn.execute(awards_query, {"playerid": playerid}).fetchall()
            
        awards = []
        for row in awards_data:
            year, award_id, league, tie, notes = row

            # Format award name for display
            award_display = format_award_name(award_id)

            award_info = {
                "year": year,
                "award": award_display,
                "award_id": award_id,
                "league": league,
                "tie": bool(tie) if tie else False,
                "notes": notes,
            }
            awards.append(award_info)

        # Group and summarize awards
        award_summary = summarize_awards(awards)

        # Get MLB All-Star Game appearances:
        allstar_games = get_allstar_appearances(playerid, conn)

        # Get world series championships
        ws_championships = get_world_series_championships(playerid, conn)

        return {
            "awards": awards,
            "summary": award_summary,
            "mlbAllStar": allstar_games,
            "world_series_championships": ws_championships,
            "ws_count": len(ws_championships),
        }

    except Exception as e:
        return {
            "awards": [],
            "summary": {},
            "mlbAllStar": [],
            "world_series_championships": [],
            "ws_count": 0,
        }
    finally:
        if owns_conn:
            conn.close()


def format_award_name(award_id):
    """Convert award IDs to readable names"""
    award_names = {
        "MVP": "Most Valuable Player",
        "CYA": "Cy Young Award",
        "CY": "Cy Young Award",
        "ROY": "Rookie of the Year",
        "GG": "Gold Glove",
        "SS": "Silver Slugger",
        "AS": "TSN All-Star Team",
        "WSMVP": "World Series MVP",
        "WS": "World Series Champion",
        "ALCS MVP": "ALCS MVP",
        "NLCS MVP": "NLCS MVP",
        "ASG MVP": "All-Star Game MVP",
        "ASGMVP": "All-Star Game MVP",
        "COMEB": "Comeback Player of the Year",
        "Hutch": "Hutch Award",
        "Lou Gehrig": "Lou Gehrig Memorial Award",
        "Babe Ruth": "Babe Ruth Award",
        "Roberto Clemente": "Roberto Clemente Award",
        "Branch Rickey": "Branch Rickey Award",
        "Hank Aaron": "Hank Aaron Award",
        "DHL Hometown Hero": "DHL Hometown Hero",
        "Edgar Martinez": "Edgar Martinez Outstanding DH Award",
        "Hutch Award": "Hutch Award",
        "Man of the Year": "Man of the Year",
        "Players Choice": "Players Choice Award",
        "Reliever": "Reliever of the Year",
        "TSN Fireman": "The Sporting News Fireman Award",
        "TSN MVP": "The Sporting News MVP",
        "TSN Pitcher": "The Sporting News Pitcher of the Year",
        "TSN Player": "The Sporting News Player of the Year",
        "TSN Rookie": "The Sporting News Rookie of the Year",
    }

    return award_names.get(award_id, award_id)


def summarize_awards(awards):
    """Create summary statistics for awards"""
    summary = {}

    # Count by award type
    for award in awards:
        award_id = award["award_id"]
        if award_id not in summary:
            summary[award_id] = {
                "count": 0,
                "years": [],
                "display_name": award["award"],
            }
        summary[award_id]["count"] += 1
        summary[award_id]["years"].append(award["year"])

    # Sort years for each award
    for award_id in summary:
        summary[award_id]["years"].sort(reverse=True)

    return summary


def get_allstar_appearances(playerid, conn=None):
    """Get MLB All-Star Game appearances from AllstarFull table"""
    from sqlalchemy import text
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        query = text("""
            SELECT COUNT(*) as allstar_games
            FROM lahman_allstarfull 
            WHERE playerid = :playerid
        """)
        result = conn.execute(query, {"playerid": playerid}).fetchone()
        
        return result[0] if result else 0
    except Exception as e:
        return 0
    finally:
        if owns_conn:
            conn.close()

@app.route("/")
def serve_index():
    return send_from_directory("static", "index.html")

# Route for two-way player handling
@app.route("/player-two-way")
def get_player_with_two_way():
    """Enhanced player endpoint that handles two-way players"""
    from sqlalchemy import text
    
    name = request.args.get("name", "")
    mode = request.args.get("mode", "career").lower()
    player_type = request.args.get("player_type", "").lower()

    if " " not in name:
        return jsonify({"error": "Enter full name"}), 400

    playerid, suggestions = improved_player_lookup_with_disambiguation(name)

    if playerid is None and suggestions:
        return (
            jsonify({
                "error": "Multiple players found",
                "suggestions": suggestions,
                "message": f"Found {len(suggestions)} players named '{name.split(' Jr.')[0].split(' Sr.')[0]}'. Please specify which player:",
            }),
            422,
        )

    if playerid is None:
        return jsonify({"error": "Player not found"}), 404

    detected_type = detect_two_way_player_simple(playerid, None)

    # Get player's actual name for display
    name_query = text("SELECT namefirst, namelast FROM lahman_people WHERE playerid = :playerid")
    
    with db_engine.connect() as conn:
        name_result = conn.execute(name_query, {"playerid": playerid}).fetchone()
    
    first, last = name_result if name_result else ("Unknown", "Unknown")

    # Handle two-way players
    if detected_type == "two-way" and not player_type:
        # Return options for user to choose
        return (
            jsonify(
                {
                    "error": "Two-way player detected",
                    "player_type": "two-way",
                    "options": [
                        {
                            "type": "pitcher",
                            "label": f"{first} {last} (Pitching Stats)",
                        },
                        {"type": "hitter", "label": f"{first} {last} (Hitting Stats)"},
                    ],
                    "message": f"{first} {last} is a known two-way player. Please select which stats to display:",
                }
            ),
            423,
        )  # Using 423 for two-way player selection

    # Use specified player_type or detected type
    final_type = player_type if player_type in ["pitcher", "hitter"] else detected_type
    if final_type == "two-way":
        final_type = "hitter"  # Default fallback

    # Get photo URL
    photo_url = get_photo_url_for_player(playerid, None)

    # Process stats based on final type
    if final_type == "pitcher":
        return handle_pitcher_stats(playerid, None, mode, photo_url, first, last)
    else:
        return handle_hitter_stats(playerid, mode, photo_url, first, last)


@app.route('/search-players')
def search_players_enhanced():
    """Enhanced search that handles father/son players and provides disambiguation"""
    from sqlalchemy import text
    
    query = request.args.get("q", "").strip()

    if len(query) < 2:
        return jsonify([])

    try:
        query_clean = query.lower().strip()
        search_term = f"%{query_clean}%"
        exact_match = f"{query_clean}%"

        search_query = text("""
        SELECT DISTINCT 
            p.namefirst,
            p.namelast,
            p.playerid,
            p.debut,
            p.finalgame,
            p.birthyear,
            CASE 
                WHEN LOWER(p.namefirst || ' ' || p.namelast) LIKE :exact_match THEN 1
                WHEN LOWER(p.namelast) LIKE :exact_match THEN 2
                WHEN LOWER(p.namefirst) LIKE :exact_match THEN 3
                ELSE 4
            END as priority,
            (SELECT pos FROM lahman_fielding f 
             WHERE f.playerid = p.playerid
             GROUP BY pos 
             ORDER BY SUM(g) DESC 
             LIMIT 1) as primary_pos
        FROM lahman_people p
        WHERE (
            LOWER(p.namefirst || ' ' || p.namelast) LIKE :search_term
            OR LOWER(p.namelast) LIKE :search_term
            OR LOWER(p.namefirst) LIKE :search_term
        )
        AND p.birthyear IS NOT NULL
        AND (
            EXISTS (SELECT 1 FROM lahman_batting b WHERE b.playerid = p.playerid)
            OR EXISTS (SELECT 1 FROM lahman_pitching pt WHERE pt.playerid = p.playerid)
        )
        ORDER BY priority, p.debut DESC NULLS LAST, p.namelast, p.namefirst
        LIMIT 15
        """)

        with db_engine.connect() as conn:
            results = conn.execute(search_query, {
                "exact_match": exact_match,
                "search_term": search_term
            }).fetchall()


        # Group players by name to detect duplicates
        name_groups = {}
        for row in results:
            first_name = row[0]
            last_name = row[1]
            full_name = f"{first_name} {last_name}"
            playerid = row[2]
            debut = row[3]
            final_game = row[4]
            birth_year = row[5]
            priority = row[6]
            position = row[7]

            if full_name not in name_groups:
                name_groups[full_name] = []

            name_groups[full_name].append({
                "full_name": full_name,
                "playerid": playerid,
                "debut": debut,
                "final_game": final_game,
                "birth_year": birth_year,
                "position": position,
            })

        # Process results and add disambiguation
        players = []
        for name, player_list in name_groups.items():
            if len(player_list) == 1:
                # Single player with this name
                player = player_list[0]
                debut_year = player["debut"][:4] if player["debut"] else "Unknown"

                if player["position"]:
                    display_name = f"{name} ({player['position']}, {debut_year})"
                else:
                    display_name = f"{name} ({debut_year})"

                players.append({
                    "name": name,
                    "display": display_name,
                    "playerid": player["playerid"],
                    "debut_year": debut_year,
                    "position": player["position"] or "Unknown",
                    "disambiguation": None,
                })
            else:
                # Multiple players with same name - add disambiguation
                # Sort by debut year (older first)
                player_list.sort(key=lambda x: x["debut"] or "9999")

                for i, player in enumerate(player_list):
                    debut_year = player["debut"][:4] if player["debut"] else "Unknown"
                    birth_year = player["birth_year"] or "Unknown"

                    # Determine suffix (Sr./Jr. or I/II based on debut order)
                    if len(player_list) == 2:
                        suffix = "Sr." if i == 0 else "Jr."
                    else:
                        suffix = ["Sr.", "Jr.", "III"][i] if i < 3 else f"({i+1})"

                    # Create display name with disambiguation
                    base_display = f"{name} {suffix}"
                    if player["position"]:
                        display_name = f"{base_display} ({player['position']}, {debut_year})"
                    else:
                        display_name = f"{base_display} ({debut_year})"

                    players.append({
                        "name": name,
                        "display": display_name,
                        "playerid": player["playerid"],
                        "debut_year": debut_year,
                        "birth_year": str(birth_year),
                        "position": player["position"] or "Unknown",
                        "disambiguation": suffix,
                        "original_name": name,
                    })

        return jsonify(players)

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Search error: {error_trace}")
        # Return error details for debugging
        return jsonify({
            "error": str(e),
            "traceback": error_trace
        }), 500

def improved_player_lookup_with_disambiguation(name):
    """
    Improved player lookup that handles common father/son cases
    and provides suggestions when multiple players exist
    """
    from sqlalchemy import text
    
    # Handle common suffixes
    suffixes = {
        "jr": "Jr.",
        "jr.": "Jr.",
        "junior": "Jr.",
        "sr": "Sr.",
        "sr.": "Sr.",
        "senior": "Sr.",
        "ii": "II",
        "iii": "III",
        "2nd": "II",
        "3rd": "III",
    }

    name_lower = name.lower().strip()
    suffix = None
    clean_name = name

    # Check if name contains a suffix
    for suffix_variant, standard_suffix in suffixes.items():
        if name_lower.endswith(" " + suffix_variant):
            suffix = standard_suffix
            clean_name = name[: -(len(suffix_variant) + 1)].strip()
            break

    if " " not in clean_name:
        return None, []

    first, last = clean_name.split(" ", 1)

    # Find all players with this name using SQLAlchemy
    all_players_query = text("""
    SELECT playerid, namefirst, namelast, debut, finalgame, birthyear
    FROM lahman_people
    WHERE LOWER(namefirst) = :first AND LOWER(namelast) = :last
    ORDER BY debut
    """)

    with db_engine.connect() as conn:
        all_matches = conn.execute(all_players_query, {
            "first": first.lower(), 
            "last": last.lower()
        }).fetchall()

    if not all_matches:
        return None, []

    if len(all_matches) == 1:
        return all_matches[0][0], []

    # Multiple players found - create suggestions
    suggestions = []
    target_player = None

    for i, (playerid, fname, lname, debut, final_game, birth_year) in enumerate(all_matches):
        full_name = f"{fname} {lname}"
        debut_year = debut[:4] if debut else "Unknown"

        # Create suggestion with disambiguation
        if len(all_matches) == 2:
            player_suffix = "Sr." if i == 0 else "Jr."
        else:
            player_suffix = ["Sr.", "Jr.", "III"][i] if i < 3 else f"({i+1})"

        suggestion = {
            "name": f"{full_name} {player_suffix}",
            "playerid": playerid,
            "debut_year": debut_year,
            "birth_year": birth_year or "Unknown",
        }
        suggestions.append(suggestion)

        # If user specified a suffix, try to match it
        if suffix and suffix == player_suffix:
            target_player = playerid

    return target_player, suggestions


@app.route("/player-disambiguate")
def get_player_with_disambiguation():
    """Enhanced player endpoint that handles disambiguation"""
    from sqlalchemy import text
    
    name = request.args.get("name", "")
    mode = request.args.get("mode", "career").lower()
    player_type = request.args.get("player_type", "").lower()

    if " " not in name:
        return jsonify({"error": "Enter full name"}), 400

    # Try improved lookup
    playerid, suggestions = improved_player_lookup_with_disambiguation(name)

    if playerid is None and suggestions:
        # Multiple players found, return suggestions
        return (
            jsonify(
                {
                    "error": "Multiple players found",
                    "suggestions": suggestions,
                    "message": f"Found {len(suggestions)} players named '{name.split(' Jr.')[0].split(' Sr.')[0]}'. Please specify which player:",
                }
            ),
            422,
        )

    if playerid is None:
        return jsonify({"error": "Player not found"}), 404

    # Continue with existing logic using the found playerid
    detected_type = detect_two_way_player_simple(playerid, None)

    # Get player's actual name for display using SQLAlchemy
    name_query = text("SELECT namefirst, namelast FROM lahman_people WHERE playerid = :playerid")
    
    with db_engine.connect() as conn:
        name_result = conn.execute(name_query, {"playerid": playerid}).fetchone()
    
    first, last = name_result if name_result else ("Unknown", "Unknown")

    # Handle two-way players
    if detected_type == "two-way" and not player_type:
        return (
            jsonify(
                {
                    "error": "Two-way player detected",
                    "player_type": "two-way",
                    "options": [
                        {
                            "type": "pitcher",
                            "label": f"{first} {last} (Pitching Stats)",
                        },
                        {"type": "hitter", "label": f"{first} {last} (Hitting Stats)"},
                    ],
                    "message": f"{first} {last} is a known two-way player. Please select which stats to display:",
                }
            ),
            423,
        )

    # Continue with existing logic using specified or detected type
    final_type = player_type if player_type in ["pitcher", "hitter"] else detected_type
    if final_type == "two-way":
        final_type = "hitter"  # Default fallback
    
    # Get photo URL
    photo_url = get_photo_url_for_player(playerid, None)

    if final_type == "pitcher":
        return handle_pitcher_stats(playerid, None, mode, photo_url, first, last)
    else:
        return handle_hitter_stats(playerid, mode, photo_url, first, last)

@app.route("/popular-players")
def popular_players():
    fallback_players = [
        "Mike Trout",
        "Aaron Judge",
        "Mookie Betts",
        "Ronald Acuña",
        "Juan Soto",
        "Vladimir Guerrero Jr.",
        "Fernando Tatis Jr.",
        "Gerrit Cole",
        "Jacob deGrom",
        "Tarik Skubal",
        "Spencer Strider",
        "Freddie Freeman",
        "Manny Machado",
        "Jose Altuve",
        "Kyle Tucker",
    ]
    return jsonify(fallback_players)

def handle_pitcher_stats(playerid, conn, mode, photo_url, first, last):
    from sqlalchemy import text
    
    stats_query = text("""
    SELECT yearid, teamid, w, l, g, gs, cg, sho, sv, ipouts, h, er, hr, bb, so, era
    FROM lahman_pitching WHERE playerid = :playerid
    ORDER BY yearid DESC
    """)
    
    df_lahman = pd.read_sql_query(stats_query, db_engine, params={"playerid": playerid})
    awards_data = get_player_awards(playerid, None)
    
    if mode == "career":
        if df_lahman.empty:
            return jsonify({"error": "No pitching stats found"}), 404

        totals = df_lahman.agg({
            "w": "sum", "l": "sum", "g": "sum", "gs": "sum", "cg": "sum", 
            "sho": "sum", "sv": "sum", "ipouts": "sum", "h": "sum", 
            "er": "sum", "hr": "sum", "bb": "sum", "so": "sum",
        }).to_dict()

        innings_pitched = totals["ipouts"] / 3.0 if totals["ipouts"] > 0 else 0
        era = (totals["er"] * 9) / innings_pitched if innings_pitched > 0 else 0
        whip = (totals["h"] + totals["bb"]) / innings_pitched if innings_pitched > 0 else 0
        career_war = get_career_war(playerid)

        result = {
            "war": round(career_war, 1),
            "wins": int(totals["w"]),
            "losses": int(totals["l"]),
            "games": int(totals["g"]),
            "games_started": int(totals["gs"]),
            "complete_games": int(totals["cg"]),
            "shutouts": int(totals["sho"]),
            "saves": int(totals["sv"]),
            "innings_pitched": round(innings_pitched, 1),
            "hits_allowed": int(totals["h"]),
            "earned_runs": int(totals["er"]),
            "home_runs_allowed": int(totals["hr"]),
            "walks": int(totals["bb"]),
            "strikeouts": int(totals["so"]),
            "era": round(era, 2),
            "whip": round(whip, 2),
        }

        return jsonify({
            "mode": "career",
            "player_type": "pitcher", 
            "totals": result,
            "photo_url": photo_url,
            "awards": awards_data,
        })

    elif mode == "season":
        if df_lahman.empty:
            return jsonify({"error": "No pitching stats found"}), 404

        df_war_history = get_season_war_history(playerid)

        df = df_lahman.copy()
        df["innings_pitched"] = df["ipouts"] / 3.0
        df["era_calc"] = df.apply(
            lambda row: (row["er"] * 9) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0,
            axis=1,
        )
        df["whip"] = df.apply(
            lambda row: (row["h"] + row["bb"]) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0,
            axis=1,
        )
        df["era_final"] = df.apply(
            lambda row: row["era"] if row["era"] > 0 else row["era_calc"], axis=1
        )

        if not df_war_history.empty:
            df = df.merge(df_war_history, on="yearid", how="left")
            df["war"] = df["war"].fillna(0)
        else:
            df["war"] = 0

        df_result = df[[
            "yearid", "teamid", "w", "l", "g", "gs", "cg", "sho", "sv",
            "innings_pitched", "h", "er", "hr", "bb", "so", "era_final", "whip", "war",
        ]].rename(columns={
            "yearid": "year", "w": "wins", "l": "losses", "g": "games",
            "gs": "games_started", "cg": "complete_games", "sho": "shutouts",
            "sv": "saves", "h": "hits_allowed", "er": "earned_runs",
            "hr": "home_runs_allowed", "bb": "walks", "so": "strikeouts",
            "era_final": "era",
        })

        return jsonify({
            "mode": "season",
            "player_type": "pitcher",
            "stats": df_result.to_dict(orient="records"),
            "photo_url": photo_url,
            "awards": awards_data,
        })

    # Return error for live and combined modes
    elif mode in ["live", "combined"]:
        return jsonify({"error": f"{mode.title()} stats temporarily disabled"}), 503

    else:
        return jsonify({"error": "Invalid mode"}), 400

# Lightweight cache for league averages by year (~4KB for all of baseball history)
_league_avg_cache = {}

def get_league_averages(conn=None, year=None):
    """Get league average OBP and SLG for a given year (cached)"""
    from sqlalchemy import text
    
    # Convert numpy types to plain Python int
    year = int(year)
    
    # Return cached value if available
    if year in _league_avg_cache:
        return _league_avg_cache[year]
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        query = text("""
        SELECT 
            SUM(h + bb + hbp) * 1.0 / NULLIF(SUM(ab + bb + hbp + sf), 0) as lg_obp,
            (SUM(h - "2b" - "3b" - hr) + 2 * SUM("2b") + 3 * SUM("3b") + 4 * SUM(hr)) * 1.0 / NULLIF(SUM(ab), 0) as lg_slg
        FROM lahman_batting
        WHERE yearid = :year
        """)
        
        result = conn.execute(query, {"year": year}).fetchone()
        
        if result and result[0] and result[1]:
            avg = {"obp": float(result[0]), "slg": float(result[1])}
        else:
            avg = {"obp": 0.320, "slg": 0.400}  # Fallback averages
        
        _league_avg_cache[year] = avg
        return avg
    finally:
        if owns_conn:
            conn.close()


def _batch_load_league_averages(years, conn=None):
    """Fetch league averages for multiple years in a single query and cache them."""
    from sqlalchemy import text
    
    # Filter to only years not already cached
    missing = [int(y) for y in years if int(y) not in _league_avg_cache]
    if not missing:
        return
    
    owns_conn = conn is None
    if owns_conn:
        conn = db_engine.connect()
    
    try:
        placeholders = ",".join([f":y{i}" for i in range(len(missing))])
        query = text(f"""
        SELECT yearid,
            SUM(h + bb + hbp) * 1.0 / NULLIF(SUM(ab + bb + hbp + sf), 0) as lg_obp,
            (SUM(h - "2b" - "3b" - hr) + 2 * SUM("2b") + 3 * SUM("3b") + 4 * SUM(hr)) * 1.0 / NULLIF(SUM(ab), 0) as lg_slg
        FROM lahman_batting
        WHERE yearid IN ({placeholders})
        GROUP BY yearid
        """)
        params = {f"y{i}": y for i, y in enumerate(missing)}
        rows = conn.execute(query, params).fetchall()
        
        for row in rows:
            yr, obp_val, slg_val = row
            if obp_val and slg_val:
                _league_avg_cache[int(yr)] = {"obp": float(obp_val), "slg": float(slg_val)}
            else:
                _league_avg_cache[int(yr)] = {"obp": 0.320, "slg": 0.400}
        
        # Fill any years that had no data at all
        for y in missing:
            if y not in _league_avg_cache:
                _league_avg_cache[y] = {"obp": 0.320, "slg": 0.400}
    finally:
        if owns_conn:
            conn.close()


def calculate_ops_plus(obp, slg, year):
    """Calculate OPS+ for a player given their OBP, SLG, and year"""
    lg_avg = get_league_averages(None, year)
    
    if lg_avg["obp"] == 0 or lg_avg["slg"] == 0:
        return 100
    
    # OPS+ = 100 * (OBP/lgOBP + SLG/lgSLG - 1)
    ops_plus = 100 * ((obp / lg_avg["obp"]) + (slg / lg_avg["slg"]) - 1)
    return round(ops_plus)


def calculate_career_ops_plus(playerid):
    """Calculate career OPS+ weighted by plate appearances"""
    from sqlalchemy import text
    
    # Get all seasons with OBP/SLG
    query = text("""
    SELECT yearid, ab, h, bb, hbp, sf, "2b", "3b", hr
    FROM lahman_batting 
    WHERE playerid = :playerid
    ORDER BY yearid
    """)
    
    df = pd.read_sql_query(query, db_engine, params={"playerid": playerid})
    
    if df.empty:
        return 100
    
    # Batch-load all league averages this player needs in one query
    _batch_load_league_averages(df['yearid'].unique())
    
    total_weighted_ops_plus = 0
    total_pa = 0
    
    for _, row in df.iterrows():
        ab = row['ab'] or 0
        h = row['h'] or 0
        bb = row['bb'] or 0
        hbp = row['hbp'] or 0
        sf = row['sf'] or 0
        doubles = row['2b'] or 0
        triples = row['3b'] or 0
        hr = row['hr'] or 0
        
        pa = ab + bb + hbp + sf
        
        if pa == 0:
            continue
        
        # Calculate season OBP and SLG
        obp_denom = ab + bb + hbp + sf
        obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else 0
        
        singles = h - doubles - triples - hr
        total_bases = singles + 2 * doubles + 3 * triples + 4 * hr
        slg = total_bases / ab if ab > 0 else 0
        
        # Get OPS+ for this season
        season_ops_plus = calculate_ops_plus(obp, slg, row['yearid'])
        
        # Weight by plate appearances
        total_weighted_ops_plus += season_ops_plus * pa
        total_pa += pa
    
    if total_pa == 0:
        return 100
    
    return round(total_weighted_ops_plus / total_pa)

def handle_hitter_stats(playerid, mode, photo_url, first, last):
    from sqlalchemy import text
    
    stats_query = text("""
    SELECT yearid, teamid, g, ab, h, hr, rbi, sb, bb, hbp, sf, sh, "2b", "3b"
    FROM lahman_batting WHERE playerid = :playerid
    ORDER BY yearid DESC
    """)
    
    df_lahman = pd.read_sql_query(stats_query, db_engine, params={"playerid": playerid})
    awards_data = get_player_awards(playerid, None)
    
    if mode == "career":
        if df_lahman.empty:
            return jsonify({"error": "No batting stats found"}), 404

        totals = df_lahman.agg({
            "g": "sum", "ab": "sum", "h": "sum", "hr": "sum", "rbi": "sum",
            "sb": "sum", "bb": "sum", "hbp": "sum", "sf": "sum", "sh": "sum",
            "2b": "sum", "3b": "sum",
        }).to_dict()

        singles = totals["h"] - totals["2b"] - totals["3b"] - totals["hr"]
        total_bases = singles + 2 * totals["2b"] + 3 * totals["3b"] + 4 * totals["hr"]
        ba = totals["h"] / totals["ab"] if totals["ab"] > 0 else 0
        obp_denominator = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"]
        obp = (totals["h"] + totals["bb"] + totals["hbp"]) / obp_denominator if obp_denominator > 0 else 0
        slg = total_bases / totals["ab"] if totals["ab"] > 0 else 0
        ops = obp + slg
        plate_appearances = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"] + totals["sh"]
        career_war = get_career_war(playerid)
        
        # Calculate career OPS+
        career_ops_plus = calculate_career_ops_plus(playerid)

        result = {
            "war": round(career_war, 1),
            "games": int(totals["g"]),
            "plate_appearances": int(plate_appearances),
            "hits": int(totals["h"]),
            "home_runs": int(totals["hr"]),
            "rbi": int(totals["rbi"]),
            "stolen_bases": int(totals["sb"]),
            "batting_average": round(ba, 3),
            "on_base_percentage": round(obp, 3),
            "slugging_percentage": round(slg, 3),
            "ops": round(ops, 3),
            "ops_plus": career_ops_plus,
        }

        return jsonify({
            "mode": "career",
            "player_type": "hitter",
            "totals": result,
            "photo_url": photo_url,
            "awards": awards_data,
        })

    elif mode == "season":
        if df_lahman.empty:
            return jsonify({"error": "No batting stats found"}), 404

        df_war_history = get_season_war_history(playerid)

        df = df_lahman.copy()
        df["singles"] = df["h"] - df["2b"] - df["3b"] - df["hr"]
        df["total_bases"] = df["singles"] + 2 * df["2b"] + 3 * df["3b"] + 4 * df["hr"]
        df["ba"] = df.apply(lambda row: row["h"] / row["ab"] if row["ab"] > 0 else 0, axis=1)
        df["obp"] = df.apply(lambda row: (
            (row["h"] + row["bb"] + row["hbp"]) / 
            (row["ab"] + row["bb"] + row["hbp"] + row["sf"])
            if (row["ab"] + row["bb"] + row["hbp"] + row["sf"]) > 0 else 0
        ), axis=1)
        df["slg"] = df.apply(lambda row: row["total_bases"] / row["ab"] if row["ab"] > 0 else 0, axis=1)
        df["ops"] = df["obp"] + df["slg"]
        df["pa"] = df["ab"] + df["bb"] + df["hbp"] + df["sf"] + df["sh"]
        
        # Calculate OPS+ for each season
        df["ops_plus"] = df.apply(
            lambda row: calculate_ops_plus(row["obp"], row["slg"], row["yearid"]),
            axis=1
        )

        if not df_war_history.empty:
            df = df.merge(df_war_history, on="yearid", how="left")
            df["war"] = df["war"].fillna(0)
        else:
            df["war"] = 0

        df_result = df[[
            "yearid", "teamid", "g", "pa", "ab", "h", "hr", "rbi", "sb", "bb",
            "hbp", "sf", "2b", "3b", "ba", "obp", "slg", "ops", "ops_plus", "war",
        ]].rename(columns={
            "yearid": "year", "g": "games", "ab": "at_bats", "h": "hits",
            "hr": "home_runs", "rbi": "rbi", "sb": "stolen_bases", "bb": "walks",
            "hbp": "hit_by_pitch", "sf": "sacrifice_flies", "2b": "doubles", "3b": "triples",
        })

        return jsonify({
            "mode": "season",
            "player_type": "hitter",
            "stats": df_result.to_dict(orient="records"),
            "photo_url": photo_url,
            "awards": awards_data,
        })

    else:
        return jsonify({"error": "Invalid mode. Use 'career' or 'season'"}), 400


@app.route("/team")
def get_team_stats():
    """Unified endpoint that returns both batting and pitching stats"""
    try:
        team = request.args.get("team", "").strip()
        mode = request.args.get("mode", "season").lower()

        if not team:
            return jsonify({"error": "Enter team"}), 400

        team_id, year = parse_team_input(team)

        # Get combined stats
        return handle_combined_team_stats(team_id, year, mode)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


def handle_combined_team_stats(team_id, year, mode):
    """Get both batting and pitching stats in one query - updated for SQLAlchemy"""
    try:
        # Use SQLAlchemy engine directly instead of get_db_connection()
        from sqlalchemy import text
        
        # Track the actual year being used
        actual_year = None

        if mode == "season":
            actual_year = year or 2024
            query = text("""
            SELECT yearid, teamid, 
                   -- Basic team stats
                   g, w, l, r, ra,
                   -- We'll calculate playoff stats separately
                   0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
            FROM lahman_teams 
            WHERE teamid = :team_id AND yearid = :year
            """)
            df = pd.read_sql_query(query, db_engine, params={"team_id": team_id, "year": actual_year})

        elif mode in ["franchise", "career", "overall"]:
            # Check for franchise moves
            franchise_ids = get_franchise_team_ids(team_id)

            if len(franchise_ids) > 1:
                # Multiple team IDs for this franchise
                placeholders = ",".join([f":team_id_{i}" for i in range(len(franchise_ids))])
                query_str = f"""
                SELECT 'FRANCHISE' as teamid, 
                       COUNT(*) as seasons,
                       -- Basic aggregates
                       SUM(g) as g, SUM(w) as w, SUM(l) as l, 
                       SUM(r) as r, SUM(ra) as ra,
                       -- We'll calculate playoff stats separately
                       0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
                FROM lahman_teams 
                WHERE teamid IN ({placeholders})
                """
                query = text(query_str)
                params = {f"team_id_{i}": team_id for i, team_id in enumerate(franchise_ids)}
                df = pd.read_sql_query(query, db_engine, params=params)
            else:
                # Single team ID
                query = text("""
                SELECT teamid, 
                       COUNT(*) as seasons,
                       -- Basic aggregates
                       SUM(g) as g, SUM(w) as w, SUM(l) as l, 
                       SUM(r) as r, SUM(ra) as ra,
                       -- We'll calculate playoff stats separately
                       0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
                FROM lahman_teams 
                WHERE teamid = :team_id
                GROUP BY teamid
                """)
                df = pd.read_sql_query(query, db_engine, params={"team_id": team_id})

        else:
            # Default to season
            actual_year = year or 2024
            query = text("""
            SELECT yearid, teamid, 
                   g, w, l, r, ra,
                   0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
            FROM lahman_teams 
            WHERE teamid = :team_id AND yearid = :year
            """)
            df = pd.read_sql_query(query, db_engine, params={"team_id": team_id, "year": actual_year})

        if not df.empty:
            # Add playoff statistics - pass actual_year for season mode
            df = add_playoff_stats(
                df, team_id, actual_year if mode == "season" else year, mode
            )

        if df.empty:
            if mode in ["franchise", "career", "overall"]:
                return (
                    jsonify({"error": f"Team '{team_id}' not found in database"}),
                    404,
                )
            else:
                return (
                    jsonify(
                        {"error": f"Team '{team_id}' not found for year {actual_year}"}
                    ),
                    404,
                )

        # Calculate derived stats
        df = calculate_simple_team_stats(df)

        # Pass the correct year value based on mode
        year_to_pass = actual_year if mode == "season" else None
        return format_combined_team_response(df, mode, team_id, year_to_pass)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Database error: {str(e)}"}), 500


def get_franchise_team_ids(team_id):
    """
    Map current team IDs to all historical team IDs for franchise totals
    This handles team moves and ID changes
    """
    franchise_mapping = {
        # Modern team ID -> All historical IDs for that franchise
        # Milwaukee Brewers - Current franchise (1970+)
        "MIL": ["MIL", "ML4"],  # NL Brewers (1998+) + AL Brewers (1970-1997)
        # Atlanta Braves - includes Boston Braves and Milwaukee Braves
        "ATL": ["ATL", "BSN", "ML1"],  # Atlanta + Boston + Milwaukee Braves (1953-1965)
        # Los Angeles Dodgers - includes all Brooklyn Dodgers
        "LAN": ["LAN", "BRO", "BR3"],
        # San Francisco Giants - includes New York Giants
        "SFN": ["SFN", "NY1"],
        # Baltimore Orioles - includes St. Louis Browns
        "BAL": ["BAL", "SLA", "MLA"],
        # Chicago White Sox
        "CHA": ["CHA"],
        # Cleveland Guardians/Indians
        "CLE": ["CLE"],
        # Cincinnati Reds
        "CIN": ["CIN", "CN2"],
        # Philadelphia Phillies
        "PHI": ["PHI"],
        # Oakland Athletics
        "OAK": [
            "OAK",
            "KC1",
            "PHA",
        ],
        # St. Louis Cardinals
        "SLN": ["SLN", "SL4"],
        # New York Yankees
        "NYA": ["NYA"],
        # New York Mets
        "NYN": ["NYN"],
        # Kansas City Royals
        "KCR": ["KCR"],
        # Minnesota Twins - includes original Washington Senators (1901-1960)
        "MIN": ["MIN", "WS1"],
        # Texas Rangers - includes expansion Washington Senators (1961-1971)
        "TEX": ["TEX", "WS2"],
        # Washington Nationals - includes Montreal Expos
        "WAS": ["WAS", "MON"],
        # Los Angeles Angels - various eras
        "LAA": ["LAA", "ANA", "CAL"],
        # Tampa Bay Rays
        "TBA": ["TBA", "TBD"],
        # Miami Marlins
        "MIA": ["MIA", "FLO", "FLA"],
        # Seattle Mariners
        "SEA": ["SEA"],
        # Pittsburgh Pirates
        "PIT": ["PIT", "PT1"],
        # Single-location franchises (no historical moves)
        "ARI": ["ARI"],
        "BOS": ["BOS"],
        "COL": ["COL"],
        "DET": ["DET"],
        "HOU": ["HOU"],
        "SDN": ["SDN"],
        "TOR": ["TOR"],
        "CHC": ["CHN"],
    }

    return franchise_mapping.get(team_id, [team_id])


def add_playoff_stats(df, team_id, year, mode):
    """Add playoff appearance and World Series statistics using lahman_seriespost"""
    try:
        from sqlalchemy import text
        
        if mode == "season":
            # For single season, check if team made playoffs that year
            actual_year = year or 2024

            # Check if they appeared in any playoff series that year
            playoff_query = text("""
            SELECT COUNT(*) as series_count
            FROM lahman_seriespost 
            WHERE (teamidwinner = :team_id OR teamidloser = :team_id) 
            AND yearid = :year
            """)

            with db_engine.connect() as conn:
                result = conn.execute(playoff_query, {"team_id": team_id, "year": actual_year})
                playoff_apps = 1 if result.fetchone()[0] > 0 else 0

                # Check World Series appearances
                ws_query = text("""
                SELECT COUNT(*) as ws_series
                FROM lahman_seriespost 
                WHERE (teamidwinner = :team_id OR teamidloser = :team_id) 
                AND yearid = :year 
                AND round = 'WS'
                """)

                result = conn.execute(ws_query, {"team_id": team_id, "year": actual_year})
                ws_apps = 1 if result.fetchone()[0] > 0 else 0

                # Check World Series wins
                ws_win_query = text("""
                SELECT COUNT(*) as ws_wins
                FROM lahman_seriespost 
                WHERE teamidwinner = :team_id
                AND yearid = :year 
                AND round = 'WS'
                """)

                result = conn.execute(ws_win_query, {"team_id": team_id, "year": actual_year})
                ws_championships = result.fetchone()[0]

        else:
            # For franchise/career mode, count all playoff appearances
            with db_engine.connect() as conn:
                playoff_query = text("""
                SELECT COUNT(DISTINCT yearid) as playoff_years
                FROM lahman_seriespost 
                WHERE (teamidwinner = :team_id OR teamidloser = :team_id)
                """)

                result = conn.execute(playoff_query, {"team_id": team_id})
                playoff_apps = result.fetchone()[0]

                # Count World Series appearances
                ws_query = text("""
                SELECT COUNT(DISTINCT yearid) as ws_years
                FROM lahman_seriespost 
                WHERE (teamidwinner = :team_id OR teamidloser = :team_id) 
                AND round = 'WS'
                """)

                result = conn.execute(ws_query, {"team_id": team_id})
                ws_apps = result.fetchone()[0]

                # Count World Series championships
                ws_win_query = text("""
                SELECT COUNT(*) as total_ws_wins
                FROM lahman_seriespost 
                WHERE teamidwinner = :team_id
                AND round = 'WS'
                """)

                result = conn.execute(ws_win_query, {"team_id": team_id})
                ws_championships = result.fetchone()[0]

        df.loc[0, "playoff_apps"] = playoff_apps
        df.loc[0, "ws_apps"] = ws_apps
        df.loc[0, "ws_championships"] = ws_championships

        return df

    except Exception as e:
        import traceback
        traceback.print_exc()
        # Return original df with zeros for playoff stats
        df.loc[0, "playoff_apps"] = 0
        df.loc[0, "ws_apps"] = 0
        df.loc[0, "ws_championships"] = 0
        return df


def calculate_simple_team_stats(df):
    """Calculate basic team stats for StatHead format"""
    try:
        df.columns = df.columns.str.lower()

        # Essential columns for StatHead format
        essential_cols = ["g", "w", "l", "r", "ra"]

        for col in essential_cols:
            if col not in df.columns:
                df[col] = 0

        # Fill NaN and convert to numeric
        for col in essential_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate derived stats
        df["gp"] = df["g"]  # Games played same as games
        df["rpg"] = df.apply(
            lambda row: row["r"] / row["g"] if row["g"] > 0 else 0, axis=1
        )  # Runs per game
        df["rapg"] = df.apply(
            lambda row: row["ra"] / row["g"] if row["g"] > 0 else 0, axis=1
        )  # Runs allowed per game

        return df

    except Exception as e:
        return df


def calculate_combined_team_stats(df):
    """Calculate both batting and pitching derived stats - without RBI"""
    try:
        df.columns = df.columns.str.lower()

        # Updated to include hbp and sf but exclude rbi
        batting_cols = ["ab", "h", "bb", "hbp", "sf", "2b", "3b", "hr", "r", "sb"]
        pitching_cols = ["ipouts", "er", "ha", "bba", "so_pitching", "w", "l"]

        for col in batting_cols + pitching_cols:
            if col not in df.columns:
                df[col] = 0

        # Fill NaN and convert to numeric
        for col in batting_cols + pitching_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Batting calculations
        df["ba"] = df.apply(
            lambda row: row["h"] / row["ab"] if row["ab"] > 0 else 0, axis=1
        )

        # Pitching calculations
        df["ip"] = df["ipouts"] / 3.0
        df["era_calc"] = df.apply(
            lambda row: (
                (row["er"] * 9) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0
            ),
            axis=1,
        )
        df["whip"] = df.apply(
            lambda row: (
                (row["ha"] + row["bba"]) / (row["ipouts"] / 3.0)
                if row["ipouts"] > 0
                else 0
            ),
            axis=1,
        )

        # Rename columns for consistency
        df = df.rename(
            columns={
                "ha": "h_allowed",
                "bba": "bb_allowed",
                "so_pitching": "so",  # Use pitching strikeouts as main SO stat
                "hra": "hr_allowed",
            }
        )

        return df

    except Exception as e:
        return df


def format_combined_team_response(df, mode, team_id, year):
    """Format combined team stats response"""
    try:
        # Pass the mode to get_team_name for proper formatting
        team_name = get_team_name(team_id, year, mode)

        stats = df.to_dict(orient="records")[0] if not df.empty else {}

        # Convert numpy types and apply formatting
        for key, value in stats.items():
            if hasattr(value, "item"):
                stats[key] = value.item()
            elif pd.isna(value):
                stats[key] = None

        stats = format_and_round_stats(stats)
        team_logo = get_team_logo_with_fallback(team_id, year)

        return jsonify(
            {
                "mode": mode,
                "team_id": team_id,
                "team_name": team_name,
                "year": year,
                "team_logo": team_logo,
                "stats": stats,
            }
        )

    except Exception as e:
        return jsonify({"error": f"Response formatting error: {str(e)}"}), 500

def parse_team_input(team):
    """Parse team input like '2024 Dodgers', 'Dodgers 2024', 'Yankees', etc."""
    try:
        parts = team.strip().split()

        if len(parts) == 1:
            team_code = get_team_code_from_search(parts[0])
            return team_code, None

        elif len(parts) == 2:
            if parts[0].isdigit():
                team_code = get_team_code_from_search(parts[1])
                return team_code, int(parts[0])
            elif parts[1].isdigit():
                team_code = get_team_code_from_search(parts[0])
                return team_code, int(parts[1])
            else:
                full_name = " ".join(parts)
                team_code = get_team_code_from_search(full_name)
                return team_code, None

        else:
            if parts[-1].isdigit() and len(parts[-1]) == 4:
                team_name = " ".join(parts[:-1])
                team_code = get_team_code_from_search(team_name)
                return team_code, int(parts[-1])
            elif parts[0].isdigit() and len(parts[0]) == 4:
                team_name = " ".join(parts[1:])
                team_code = get_team_code_from_search(team_name)
                return team_code, int(parts[0])
            else:
                full_name = " ".join(parts)
                team_code = get_team_code_from_search(full_name)
                return team_code, None

    except Exception as e:
        team_code = get_team_code_from_search(team)
        return team_code, None


def get_team_code_from_search(search_term):
    """Convert team search terms to database team codes"""
    search_term = search_term.lower().strip()

    # Direct code lookup  (e.g. 'chc' -> 'CHN')
    if search_term in _CODE_TO_PRIMARY:
        return _CODE_TO_PRIMARY[search_term]

    # Alias lookup  (e.g. 'cubs' -> 'CHN')
    if search_term in _ALIAS_TO_CODE:
        return _ALIAS_TO_CODE[search_term]

    # Partial-match fallback
    for alias, code in _ALIAS_TO_CODE.items():
        if search_term in alias or alias in search_term:
            return code

    return search_term.upper()


def get_team_name(team_id, year=None, mode=None):
    """Get full team name for display"""
    code = _CODE_TO_PRIMARY.get(team_id.lower(), team_id.upper())
    info = TEAMS.get(code)
    base_name = info["name"] if info else team_id

    if mode == "season" and year is not None:
        return f"{year} {base_name}"
    elif mode in ["franchise", "career", "overall"] or year is None:
        return f"{base_name} (All-Time)"
    else:
        return f"{year} {base_name}"


def get_team_logo_url(team_id, year=None):
    """Get team logo URL using working MLB logo sources"""
    code = _CODE_TO_PRIMARY.get(team_id.lower(), team_id.upper())
    info = TEAMS.get(code, {})
    abbrev = info.get("espn", team_id.lower())
    team_number = info.get("mlb_id", "0")

    logo_urls = [
        f"https://www.mlbstatic.com/team-logos/url/{team_number}.svg",
        f"https://img.mlbstatic.com/mlb-photos/image/upload/v1/team/{abbrev}/logo/current",
        f"https://a.espncdn.com/i/teamlogos/mlb/500/{abbrev}.png",
        f"https://content.sportslogos.net/logos/54/{team_number}/{abbrev}-logo-primary-dark.png",
        f"https://loodibee.com/wp-content/uploads/mlb-{abbrev}-logo-transparent.png",
        f"https://teamcolorcodes.com/wp-content/uploads/{abbrev}-logo.png",
    ]
    return logo_urls[0]


def get_team_logo_with_fallback(team_id, year=None):
    """Get team logo with fallback options"""
    code = _CODE_TO_PRIMARY.get(team_id.lower(), team_id.upper())
    info = TEAMS.get(code, {})
    abbrev = info.get("espn", team_id.lower())

    primary_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{abbrev}.png"
    fallback_urls = [
        f"https://loodibee.com/wp-content/uploads/mlb-{abbrev}-logo-transparent.png",
        f"https://content.sportslogos.net/logos/54/team/{abbrev}-logo-primary-dark.png",
    ]
    return {"primary": primary_url, "fallbacks": fallback_urls}


def format_and_round_stats(stats_dict):
    """Format stats with proper decimal places - updated for StatHead format"""

    # Stats that should show one decimal place
    per_game_stats = ["rpg", "rapg"]

    formatted_stats = {}

    for key, value in stats_dict.items():
        if value is None or pd.isna(value):
            formatted_stats[key] = None
            continue

        try:
            num_value = float(value)
        except (ValueError, TypeError):
            formatted_stats[key] = value
            continue

        if key in per_game_stats:
            # Per-game stats get 1 decimal place
            formatted_stats[key] = f"{num_value:.1f}"
        else:
            # Everything else is whole numbers
            if isinstance(num_value, float) and num_value.is_integer():
                formatted_stats[key] = int(num_value)
            elif isinstance(num_value, float):
                formatted_stats[key] = int(round(num_value))
            else:
                formatted_stats[key] = value

    return formatted_stats


def get_regular_season_h2h(engine, team_a, team_b, year_filter=None):
    """
    Get regular season head-to-head record from retrosheet_teamstats
    Updated to use SQLAlchemy properly
    """
    print(f"=== Starting get_regular_season_h2h for {team_a} vs {team_b} ===")
    
    try:
        from sqlalchemy import text, inspect
        
        team_a_ids = get_franchise_team_ids(team_a)
        team_b_ids = get_franchise_team_ids(team_b)
        
        with engine.connect() as conn:
            print("Database connection established successfully")
            
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            if 'retrosheet_teamstats' not in tables:
                print("retrosheet_teamstats table does not exist")
                return {"error": "retrosheet_teamstats table not found"}
            
            print("retrosheet_teamstats table exists")
            
            # Check columns
            columns = inspector.get_columns('retrosheet_teamstats')
            column_names = [col['name'] for col in columns]
            print(f"retrosheet_teamstats columns: {column_names}")
            
            # Check total row count
            count_query = text("SELECT COUNT(*) FROM retrosheet_teamstats")
            result = conn.execute(count_query)
            total_rows = result.fetchone()[0]
            print(f"Total rows in retrosheet_teamstats: {total_rows}")
            
            if total_rows == 0:
                print("Table is empty!")
                return {"team_a_wins": 0, "team_b_wins": 0, "ties": 0, "total_games": 0, "error": "Table is empty"}

        # Get franchise team IDs
        team_a_ids = get_franchise_team_ids(team_a)
        team_b_ids = get_franchise_team_ids(team_b)
        
        print(f"Team A ({team_a}) IDs: {team_a_ids}")
        print(f"Team B ({team_b}) IDs: {team_b_ids}")

        # Build dynamic query with named parameters
        team_a_placeholders = ",".join([f":team_a_{i}" for i in range(len(team_a_ids))])
        team_b_placeholders = ",".join([f":team_b_{i}" for i in range(len(team_b_ids))])

        base_query_str = f"""
        SELECT team, opp, date, win
        FROM retrosheet_teamstats 
        WHERE (
            (team IN ({team_a_placeholders}) AND opp IN ({team_b_placeholders})) OR 
            (team IN ({team_b_placeholders}) AND opp IN ({team_a_placeholders}))
        )
        """

        # Build parameter dictionary
        params = {}
        for i, team_id in enumerate(team_a_ids):
            params[f"team_a_{i}"] = team_id
        for i, team_id in enumerate(team_b_ids):
            params[f"team_b_{i}"] = team_id

        if year_filter:
            base_query_str += " AND CAST(date / 10000 AS INTEGER) = :year_filter"
            params["year_filter"] = int(year_filter)

        base_query_str += " ORDER BY date, team"
        
        print(f"Final query: {base_query_str}")
        print(f"Query params: {params}")

        # Execute query using SQLAlchemy text and pandas
        query = text(base_query_str)
        games_df = pd.read_sql_query(query, engine, params=params)
        print(f"Query executed successfully, returned {len(games_df)} rows")
        
        if not games_df.empty:
            print(f"Sample results:\n{games_df.head()}")

        if games_df.empty:
            print("No games found between these teams")
            return {"team_a_wins": 0, "team_b_wins": 0, "ties": 0, "total_games": 0}

        # Count wins for each franchise
        team_a_wins = 0
        team_b_wins = 0

        for _, row in games_df.iterrows():
            game_winner = row['team']
            game_win_flag = row['win']

            if game_win_flag == 1:  # This team won
                if game_winner in team_a_ids:
                    team_a_wins += 1
                elif game_winner in team_b_ids:
                    team_b_wins += 1

        total_games = len(games_df) // 2

        result = {
            "team_a_wins": team_a_wins,
            "team_b_wins": team_b_wins,
            "ties": 0,
            "total_games": total_games,
        }
        
        print(f"Final result: {result}")
        return result

    except Exception as e:
        print(f"get_regular_season_h2h error: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            "team_a_wins": 0,
            "team_b_wins": 0,
            "ties": 0,
            "total_games": 0,
            "error": str(e),
        }


def get_head_to_head_record(team_a, team_b, year_filter=None):
    """
    Get head-to-head record between two teams using SQLAlchemy
    """
    try:
        from sqlalchemy import text
        
        # Get regular season head-to-head 
        regular_season_record = get_regular_season_h2h(db_engine, team_a, team_b, year_filter)

        # Get playoff data using SQLAlchemy
        playoff_query = text("""
        SELECT yearid, round, teamidwinner, teamidloser, wins, losses
        FROM lahman_seriespost 
        WHERE (
            (teamidwinner = :team_a AND teamidloser = :team_b) OR 
            (teamidwinner = :team_b AND teamidloser = :team_a)
        )
        """)

        params = {"team_a": team_a, "team_b": team_b}

        if year_filter:
            playoff_query = text("""
            SELECT yearid, round, teamidwinner, teamidloser, wins, losses
            FROM lahman_seriespost 
            WHERE (
                (teamidwinner = :team_a AND teamidloser = :team_b) OR 
                (teamidwinner = :team_b AND teamidloser = :team_a)
            ) AND yearid = :year_filter
            """)
            params["year_filter"] = year_filter

        playoff_df = pd.read_sql_query(playoff_query, db_engine, params=params)

        # Process playoff data
        team_a_series_wins = len(playoff_df[playoff_df["teamidwinner"] == team_a])
        team_b_series_wins = len(playoff_df[playoff_df["teamidwinner"] == team_b])

        team_a_game_wins = 0
        team_b_game_wins = 0

        if "wins" in playoff_df.columns and "losses" in playoff_df.columns:
            for _, row in playoff_df.iterrows():
                wins_val = int(row["wins"]) if pd.notna(row["wins"]) else 0
                losses_val = int(row["losses"]) if pd.notna(row["losses"]) else 0

                if row["teamidwinner"] == team_a:
                    team_a_game_wins += wins_val
                    team_b_game_wins += losses_val
                else:
                    team_b_game_wins += wins_val
                    team_a_game_wins += losses_val

        series_details = []
        for _, row in playoff_df.iterrows():
            series_details.append(
                {
                    "year": int(row["yearid"]),
                    "round": row["round"],
                    "winner": row["teamidwinner"],
                    "loser": row["teamidloser"],
                    "series_wins": int(row["wins"]) if pd.notna(row["wins"]) else None,
                    "series_losses": (
                        int(row["losses"]) if pd.notna(row["losses"]) else None
                    ),
                }
            )

        playoff_record = {
            "series_wins": {"team_a": team_a_series_wins, "team_b": team_b_series_wins},
            "game_wins": {"team_a": team_a_game_wins, "team_b": team_b_game_wins},
            "series_details": series_details,
        }

        return {
            "regular_season": regular_season_record,
            "playoffs": playoff_record,
            "note": "Regular season from Retrosheet teamstats, playoff records from Lahman database.",
        }

    except Exception as e:
        return {
            "regular_season": {"team_a_wins": 0, "team_b_wins": 0, "ties": 0},
            "playoffs": {"series_wins": {"team_a": 0, "team_b": 0}},
            "error": str(e),
        }


@app.route('/team/h2h')
def team_h2h():
    team_a = request.args.get('team_a')
    team_b = request.args.get('team_b')
    year = request.args.get('year')
    
    if not team_a or not team_b:
        return jsonify({"error": "team_a and team_b parameters required"}), 400
    
    try:
        # Parse team inputs to get team codes
        team_a_id, _ = parse_team_input(team_a)
        team_b_id, _ = parse_team_input(team_b)
        
        # Use the parsed team IDs, not the original strings
        h2h_data = get_head_to_head_record(team_a_id, team_b_id, year)  # CHANGED THIS LINE
        
        return jsonify(h2h_data)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))

    app.run(host='0.0.0.0', port=port, debug=False)
