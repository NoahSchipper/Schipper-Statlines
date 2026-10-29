from flask import Blueprint, jsonify, request
import pandas as pd
from sqlalchemy import text

from database import db_engine


teams_bp = Blueprint("teams", __name__)


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


_CODE_TO_PRIMARY = {}


_ALIAS_TO_CODE = {}

for _code, _info in TEAMS.items():
    _CODE_TO_PRIMARY[_code.lower()] = _code
    for _alt in _info.get("alt_codes", []):
        _CODE_TO_PRIMARY[_alt.lower()] = _code

for _code, _info in TEAMS.items():
    for _alias in _info.get("aliases", []):
        _ALIAS_TO_CODE[_alias] = _code


@teams_bp.route("/team")
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
            actual_year = year or 2025
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
            actual_year = year or 2025
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
            actual_year = year or 2025

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


@teams_bp.route('/team/h2h')
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
