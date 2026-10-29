from flask import Blueprint, jsonify, request
import pandas as pd
from sqlalchemy import text

from database import db_engine


players_bp = Blueprint("players", __name__)


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


@players_bp.route("/player-two-way")
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


@players_bp.route('/search-players')
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


@players_bp.route("/player-disambiguate")
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


@players_bp.route("/popular-players")
def popular_players():
    fallback_players = [
        "Mike Trout",
        "Aaron Judge",
        "Mookie Betts",
        "Ronald AcuÃ±a",
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
