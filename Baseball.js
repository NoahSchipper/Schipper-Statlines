// maps hitter stats
const hitterLabelMap = {
  war: "WAR",
  games: "G",
  plate_appearances: "PA",
  hits: "H",
  home_runs: "HR",
  rbi: "RBI",
  stolen_bases: "SB",
  batting_average: "BA",
  on_base_percentage: "OBP",
  slugging_percentage: "SLG",
  ops: "OPS",
  ops_plus: "OPS+",
};

// maps pitcher stats
const pitcherLabelMap = {
  war: "WAR",
  wins: "W",
  losses: "L",
  games: "G",
  games_started: "GS",
  complete_games: "CG",
  shutouts: "SHO",
  saves: "SV",
  innings_pitched: "IP",
  hits_allowed: "H",
  earned_runs: "ER",
  home_runs_allowed: "HR",
  walks: "BB",
  strikeouts: "SO",
  era: "ERA",
  whip: "WHIP",
};

// maps hitter stats for season mode
const hitterSeasonLabelMap = {
  war: "WAR",
  games: "G",
  pa: "PA",
  hits: "H",
  home_runs: "HR",
  rbi: "RBI",
  stolen_bases: "SB",
  ba: "BA",
  obp: "OBP",
  slg: "SLG",
  ops: "OPS",
  ops_plus: "OPS+",
};

// maps pitcher stats for season
const pitcherSeasonLabelMap = {
  war: "WAR",
  wins: "W",
  losses: "L",
  games: "G",
  games_started: "GS",
  complete_games: "CG",
  shutouts: "SHO",
  saves: "SV",
  innings_pitched: "IP",
  hits_allowed: "H",
  earned_runs: "ER",
  home_runs_allowed: "HR",
  walks: "BB",
  strikeouts: "SO",
  era: "ERA",
  whip: "WHIP",
};

const backendBaseUrl = window.location.hostname === "127.0.0.1" || window.location.hostname === "localhost"
  ? "http://127.0.0.1:5000"
  : "https://schipperstatlines.onrender.com";

// Common father/son player mappings for quick reference
const COMMON_FATHER_SON_PLAYERS = {
  "ken griffey": ["Ken Griffey Sr.", "Ken Griffey Jr."],
  "fernando tatis": ["Fernando Tatis Sr.", "Fernando Tatis Jr."],
  "cal ripken": ["Cal Ripken Sr.", "Cal Ripken Jr."],
  "bobby bonds": ["Bobby Bonds", "Barry Bonds"],
  "cecil fielder": ["Cecil Fielder", "Prince Fielder"],
  "tim raines": ["Tim Raines Sr.", "Tim Raines Jr."],
  "sandy alomar": ["Sandy Alomar Sr.", "Sandy Alomar Jr."],
  "pete rose": ["Pete Rose Sr.", "Pete Rose Jr."],
};

function extractStats(res) {
  if (res.error) return null;

  if (["career", "combined", "live"].includes(res.mode)) {
    return res.totals || res.stats;
  } else if (res.mode === "season") {
    return res.stats;
  }
  return null;
}

// formats player awards
function formatAwardsForStructuredDisplay(awards) {
  // Handle null/undefined awards
  if (!awards) {
    return {
      championships: 0,
      tsnAllStar: 0, // Separate TSN All-Star count
      mlbAllStar: 0, // MLB All-Star Game appearances (if available)
      goldGlove: 0,
      silverSlugger: 0,
      mvp: 0,
      cyYoung: 0,
      royAward: 0,
      worldSeriesMvp: 0,
      relieverAward: 0,
      otherMajor: [],
    };
  }

  // Handle case where awards.summary doesn't exist but awards.awards might
  let summary = awards.summary;

  if (!summary && awards.awards && Array.isArray(awards.awards)) {
    // Create summary from awards array if summary is missing
    summary = {};
    awards.awards.forEach((award) => {
      const awardId = award.award_id || award.award;
      if (!summary[awardId]) {
        summary[awardId] = {
          count: 0,
          display_name: award.award,
          years: [],
        };
      }
      summary[awardId].count += 1;
      summary[awardId].years.push(award.year);
    });
  }

  if (!summary) {
    return {
      championships: 0,
      tsnAllStar: 0,
      mlbAllStar: 0,
      goldGlove: 0,
      silverSlugger: 0,
      mvp: 0,
      cyYoung: 0,
      royAward: 0,
      worldSeriesMvp: 0,
      relieverAward: 0,
      otherMajor: [],
    };
  }

  // Enhanced mapping to properly distinguish TSN All-Stars from MLB All-Star Games
  const result = {
    // FIXED: Check for World Series championships in multiple places
    championships:
      (awards.world_series_championships &&
      Array.isArray(awards.world_series_championships)
        ? awards.world_series_championships.length
        : 0) ||
      awards.ws_count ||
      summary["WS"]?.count ||
      summary["World Series Champion"]?.count ||
      0,

    // TSN All-Star team selections (from awards table)
    tsnAllStar:
      summary["AS"]?.count ||
      summary["TSN All-Star"]?.count ||
      summary["The Sporting News All-Star"]?.count ||
      0,

    // MLB All-Star Game appearances
    mlbAllStar:
      (awards.mlbAllStar && Array.isArray(awards.mlbAllStar)
        ? awards.mlbAllStar.length
        : 0) ||
      awards.mlbAllStar ||
      0,

    goldGlove: summary["GG"]?.count || summary["Gold Glove"]?.count || 0,
    silverSlugger:
      summary["SS"]?.count || summary["Silver Slugger"]?.count || 0,
    mvp: summary["MVP"]?.count || summary["Most Valuable Player"]?.count || 0,
    cyYoung:
      summary["CYA"]?.count ||
      summary["CY"]?.count ||
      summary["Cy Young Award"]?.count ||
      0,
    royAward:
      summary["ROY"]?.count || summary["Rookie of the Year"]?.count || 0,
    worldSeriesMvp:
      summary["WSMVP"]?.count || summary["World Series MVP"]?.count || 0,
    relieverAward:
      summary["Reliever"]?.count || summary["Reliever of the Year"]?.count || 0,
    otherMajor: getOtherMajorAwards(summary),
  };

  return result;
}

// gets other major awards
function getOtherMajorAwards(summary) {
  const majorAwardTypes = [
    "ALCS MVP",
    "NLCS MVP",
    "ASG MVP",
    "COMEB",
    "Hutch",
    "Roberto Clemente",
    "Hank Aaron",
    "Edgar Martinez",
    "Lou Gehrig",
    "Branch Rickey",
    "All-MLB - First Team",
    "All-MLB - Second Team",
    "Player of the Month",
    "Player of the Week",
  ];

  const other = [];

  // Check each award type in the summary
  Object.keys(summary).forEach((awardKey) => {
    const awardData = summary[awardKey];

    // Skip awards we've already categorized in the main function
    const mainAwards = [
      "WS",
      "World Series Champion",
      "AS",
      "The Sporting News All-Star",
      "MLB All-Star",
      "All-Star Game",
      "GG",
      "Gold Glove",
      "SS",
      "Silver Slugger",
      "MVP",
      "Most Valuable Player",
      "CYA",
      "CY",
      "Cy Young Award",
      "ROY",
      "Rookie of the Year",
      "WSMVP",
      "World Series MVP",
      "Reliever",
      "Reliever of the Year",
    ];

    if (!mainAwards.includes(awardKey)) {
      // Include this as an "other major award"
      other.push({
        name: awardData.display_name || awardKey,
        count: awardData.count,
      });
    }
  });

  return other;
}

// calls various functions to update the player comparison table
function updateComparisonTable(resA, resB, nameA, nameB) {
  const tbody = document.getElementById("comparisonBody");
  const thA = document.getElementById("playerAName");
  const thB = document.getElementById("playerBName");
  const photoA = document.getElementById("photoA");
  const photoB = document.getElementById("photoB");

  // Apply capitalization to the names before displaying
  const capitalizedNameA = capitalizePlayerName(nameA) || "Player A";
  const capitalizedNameB = capitalizePlayerName(nameB) || "Player B";

  thA.querySelector(".player-name").textContent = capitalizedNameA;
  thB.querySelector(".player-name").textContent = capitalizedNameB;

  // Set images based on player type instead of photo_url
  const playerAImage =
    resA?.player_type === "pitcher"
      ? "./assets/pitcherShadow.png"
      : "./assets/batterShadow.png";

  const playerBImage =
    resB?.player_type === "pitcher"
      ? "./assets/pitcherShadow.png"
      : "./assets/batterShadow.png";

  if (photoA) {
    photoA.src = playerAImage;
    photoA.style.display = "block";
    photoA.onerror = () => {
      console.error("Failed to load Player A image:", playerAImage);
      photoA.src =
        resA?.player_type === "pitcher"
          ? "https://via.placeholder.com/150/0066cc/ffffff?text=Pitcher"
          : "https://via.placeholder.com/150/cc0000/ffffff?text=Batter";
    };
  } else {
    console.error("photoA element not found!");
  }

  if (photoB) {
    photoB.src = playerBImage;
    photoB.style.display = "block";
    photoB.onerror = () => {
      console.error("Failed to load Player B image:", playerBImage);
      photoB.src =
        resB?.player_type === "pitcher"
          ? "https://via.placeholder.com/150/0066cc/ffffff?text=Pitcher"
          : "https://via.placeholder.com/150/cc0000/ffffff?text=Batter";
    };
  } else {
    console.error("photoB element not found!");
  }

  tbody.innerHTML = "";

  if (!resA || !resB) {
    tbody.innerHTML = `<tr><td colspan='3'>Error loading player data.</td></tr>`;
    return;
  }
  if (resA.error || resB.error) {
    tbody.innerHTML = `<tr><td colspan='3'>${
      resA.error || resB.error
    }</td></tr>`;
    return;
  }

  if (resA.player_type !== resB.player_type) {
    tbody.innerHTML = `<tr><td colspan='3'>Cannot compare pitcher and hitter statistics.</td></tr>`;
    return;
  }

  const mode = resA.mode;
  const playerType = resA.player_type || "hitter";

  if (["career", "combined", "live"].includes(mode)) {
    const statsA = extractStats(resA);
    const statsB = extractStats(resB);

    const currentLabelMap =
      playerType === "pitcher" ? pitcherLabelMap : hitterLabelMap;

    for (const key of Object.keys(currentLabelMap)) {
      const statName = currentLabelMap[key];
      let valA = statsA[key] ?? 0;
      let valB = statsB[key] ?? 0;

      if (playerType === "pitcher") {
        const decimalStats = ["war", "era", "whip", "innings_pitched"];
        if (decimalStats.includes(key)) {
          if (key === "war") {
            valA = valA ? Number(valA).toFixed(1) : "0.0";
            valB = valB ? Number(valB).toFixed(1) : "0.0";
          } else if (key === "innings_pitched") {
            valA = valA ? Number(valA).toFixed(1) : "0.0";
            valB = valB ? Number(valB).toFixed(1) : "0.0";
          } else {
            valA = valA ? Number(valA).toFixed(2) : "0.00";
            valB = valB ? Number(valB).toFixed(2) : "0.00";
          }
        }
      } else {
        const decimalStats = [
          "war",
          "batting_average",
          "on_base_percentage",
          "slugging_percentage",
          "ops",
        ];
        if (decimalStats.includes(key)) {
          if (key === "war") {
            valA = valA ? Number(valA).toFixed(1) : "0.0";
            valB = valB ? Number(valB).toFixed(1) : "0.0";
          } else {
            valA = valA ? Number(valA).toFixed(3).replace(/^0/, "") : ".000";
            valB = valB ? Number(valB).toFixed(3).replace(/^0/, "") : ".000";
          }
        } else if (key === "ops_plus") {
          valA = valA ? Math.round(valA) : 0;
          valB = valB ? Math.round(valB) : 0;
        }
      }

      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${valA}</td>
        <td style="background-color: #f1f3f4;"><strong>${statName}</strong></td>
        <td>${valB}</td>
      `;
      tbody.appendChild(row);
    }
  }

  // AWARDS SECTION - Fixed to properly check for awards
  const hasAwardsA = resA.awards && (resA.awards.summary || resA.awards.awards);
  const hasAwardsB = resB.awards && (resB.awards.summary || resB.awards.awards);

  if (mode === "live" || mode === "season") {
  } else if (hasAwardsA || hasAwardsB) {
    if (hasAwardsA || hasAwardsB) {
      const awardsA = formatAwardsForStructuredDisplay(resA.awards);
      const awardsB = formatAwardsForStructuredDisplay(resB.awards);

      // Define award rows to display (only show if at least one player has the award)
      const awardRows = [
        {
          key: "championships",
          label: "Championships",
          valueA: awardsA.championships,
          valueB: awardsB.championships,
        },
        { key: "mvp", label: "MVP", valueA: awardsA.mvp, valueB: awardsB.mvp },
        {
          key: "cyYoung",
          label: "Cy Young",
          valueA: awardsA.cyYoung,
          valueB: awardsB.cyYoung,
        },
        {
          key: "royAward",
          label: "Rookie of Year",
          valueA: awardsA.royAward,
          valueB: awardsB.royAward,
        },
        {
          key: "worldSeriesMvp",
          label: "World Series MVP",
          valueA: awardsA.worldSeriesMvp,
          valueB: awardsB.worldSeriesMvp,
        },
        {
          key: "mlbAllStar",
          label: "All-Star Games",
          valueA: awardsA.mlbAllStar,
          valueB: awardsB.mlbAllStar,
        },
        {
          key: "goldGlove",
          label: "Gold Glove",
          valueA: awardsA.goldGlove,
          valueB: awardsB.goldGlove,
        },
        {
          key: "silverSlugger",
          label: "Silver Slugger",
          valueA: awardsA.silverSlugger,
          valueB: awardsB.silverSlugger,
        },
        {
          key: "relieverAward",
          label: "Reliever of Year",
          valueA: awardsA.relieverAward,
          valueB: awardsB.relieverAward,
        },
      ];

      // Track if any awards were added
      let awardsAdded = 0;

      // Add Awards & Honors header before the first award (only if we have awards to show)
      let hasAnyAwards = awardRows.some(
        (row) => row.valueA > 0 || row.valueB > 0,
      );
      if (hasAnyAwards && awardsAdded === 0) {
        const headerRow = document.createElement("tr");
        headerRow.innerHTML = `<th colspan="3" class="stat-header">Awards & Honors (Through 2024 Season)</th>`;
        tbody.appendChild(headerRow);
      }

      // Only show awards where at least one player has a non-zero value
      awardRows.forEach((awardRow, index) => {
        if (awardRow.valueA > 0 || awardRow.valueB > 0) {
          const row = document.createElement("tr");

          // Display values - show 0 for players without awards, actual count for those with awards
          const displayA = awardRow.valueA > 0 ? awardRow.valueA : "0";
          const displayB = awardRow.valueB > 0 ? awardRow.valueB : "0";

          row.innerHTML = `
          <td style="text-align: center; padding: 8px;">${displayA}</td>
          <td style="text-align: center; padding: 8px; font-weight: bold; background-color: #f1f3f4;">${awardRow.label}</td>
          <td style="text-align: center; padding: 8px;">${displayB}</td>
        `;
          tbody.appendChild(row);
          awardsAdded++;
        }
      });

      // Add other major awards if any
      const allOtherAwards = [
        ...(awardsA.otherMajor || awardsA.awards || []),
        ...(awardsB.otherMajor || awardsB.awards || []),
      ];
      const uniqueOtherAwards = [...new Set(allOtherAwards.map((a) => a.name))];

      uniqueOtherAwards.forEach((awardName) => {
        const countA =
          (awardsA.otherMajor || awardsA.awards || []).find(
            (a) => a.name === awardName,
          )?.count || 0;
        const countB =
          (awardsB.otherMajor || awardsB.awards || []).find(
            (a) => a.name === awardName,
          )?.count || 0;

        if (countA > 0 || countB > 0) {
          const row = document.createElement("tr");

          // Show actual count or 0, not empty
          const displayA = countA > 0 ? countA : "0";
          const displayB = countB > 0 ? countB : "0";

          row.innerHTML = `
          <td style="text-align: center; padding: 8px;">${displayA}</td>
          <td style="text-align: center; padding: 8px; font-weight: bold; background-color: #f1f3f4;">${awardName}</td>
          <td style="text-align: center; padding: 8px;">${displayB}</td>
        `;
          tbody.appendChild(row);
          awardsAdded++;
        }
      });

      // If no awards were added, show a debug message
      if (awardsAdded === 0) {
        const row = document.createElement("tr");
        row.innerHTML = `<td colspan="3" style="text-align: center; padding: 12px; color: #666; font-style: italic;">No major awards found for either player</td>`;
        tbody.appendChild(row);
      } else {
      }
    } else {
    }
  }
  // SEASON MODE HANDLING
  if (mode === "season") {
    const statsA = extractStats(resA);
    const statsB = extractStats(resB);

    const years = new Set();
    statsA.forEach((s) => years.add(s.year));
    statsB.forEach((s) => years.add(s.year));

    let yearsArray = Array.from(years);
    const sortOrder = document.getElementById("viewMode").value;
    if (sortOrder === "oldest") {
      yearsArray.sort((a, b) => a - b);
    } else {
      yearsArray.sort((a, b) => b - a);
    }

    const seasonLabelMap =
      playerType === "pitcher" ? pitcherSeasonLabelMap : hitterSeasonLabelMap;

    yearsArray.forEach((year) => {
      const playerAStat = statsA.find((s) => s.year === year) || {};
      const playerBStat = statsB.find((s) => s.year === year) || {};

      const yearRow = document.createElement("tr");
      yearRow.innerHTML = `<td colspan="3" style="text-align: center; font-weight: bold; background-color: #f0f0f0; padding: 8px;">${year}</td>`;
      tbody.appendChild(yearRow);

      for (const key of Object.keys(seasonLabelMap)) {
        const statName = seasonLabelMap[key];

        let valA = playerAStat[key] ?? 0;
        let valB = playerBStat[key] ?? 0;

        if (playerType === "pitcher") {
          const decimalStats = ["war", "era", "whip", "innings_pitched"];
          if (decimalStats.includes(key)) {
            if (key === "war") {
              valA = valA ? Number(valA).toFixed(1) : "0.0";
              valB = valB ? Number(valB).toFixed(1) : "0.0";
            } else if (key === "innings_pitched") {
              valA = valA ? Number(valA).toFixed(1) : "0.0";
              valB = valB ? Number(valB).toFixed(1) : "0.0";
            } else {
              valA = valA ? Number(valA).toFixed(2) : "0.00";
              valB = valB ? Number(valB).toFixed(2) : "0.00";
            }
          }
        } else {
          const decimalStats = ["war", "ba", "obp", "slg", "ops"];
          if (decimalStats.includes(key)) {
            if (key === "war") {
              valA = valA ? Number(valA).toFixed(1) : "0.0";
              valB = valB ? Number(valB).toFixed(1) : "0.0";
            } else {
              valA = valA ? Number(valA).toFixed(3).replace(/^0/, "") : ".000";
              valB = valB ? Number(valB).toFixed(3).replace(/^0/, "") : ".000";
            }
          }
        }

        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${valA}</td>
          <td style="background-color: #f1f3f4;"><strong>${statName}</strong></td>
          <td>${valB}</td>
        `;
        tbody.appendChild(row);
      }
    });
  }
}

// ENHANCED FETCH WITH DISAMBIGUATION AND TWO-WAY HANDLING
async function fetchStats(name, mode, playerType = null) {
  try {
    let backendMode = mode;
    if (mode === "newest" || mode === "oldest") {
      backendMode = "season";
    }

    // Build URL with player_type parameter if specified
    let url = `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
      name,
    )}&mode=${backendMode}`;
    if (playerType) {
      url += `&player_type=${playerType}`;
    }

    // Use the two-way endpoint instead of disambiguate
    const response = await fetch(url);

    if (response.status === 422) {
      // Multiple players found - handle disambiguation
      const data = await response.json();
      return await handleDisambiguation(name, data.suggestions, backendMode);
    }

    if (response.status === 423) {
      // Two-way player found - handle player type selection
      const data = await response.json();
      return await handleTwoWayPlayerSelection(name, data.options, backendMode);
    }

    if (response.ok) {
      return await response.json();
    }

    // If 404 or other error, try the disambiguate endpoint
    const fallbackUrl = `${backendBaseUrl}/player-disambiguate?name=${encodeURIComponent(
      name,
    )}&mode=${backendMode}`;
    const fallbackResponse = await fetch(fallbackUrl);

    if (fallbackResponse.status === 422) {
      const data = await fallbackResponse.json();
      return await handleDisambiguation(name, data.suggestions, backendMode);
    }

    if (fallbackResponse.ok) {
      return await fallbackResponse.json();
    }

    // Final fallback to original endpoint
    const originalResponse = await fetch(
      `${backendBaseUrl}/player?name=${encodeURIComponent(
        name,
      )}&mode=${backendMode}`,
    );
    return await originalResponse.json();
  } catch (e) {
    //console.error("Fetch error:", e);
    return { error: "Failed to fetch data" };
  }
}

// Define which stats are "higher is better" vs "lower is better"
const statConfigurations = {
  // Higher is better stats
  WAR: { higherIsBetter: true },
  G: { higherIsBetter: true },
  PA: { higherIsBetter: true },
  H: { higherIsBetter: true },
  HR: { higherIsBetter: true },
  RBI: { higherIsBetter: true },
  SB: { higherIsBetter: true },
  BA: { higherIsBetter: true },
  OBP: { higherIsBetter: true },
  SLG: { higherIsBetter: true },
  OPS: { higherIsBetter: true },
  "OPS+": { higherIsBetter: true },
  Championships: { higherIsBetter: true },
  "All-Star": { higherIsBetter: true },
  "Gold Glove": { higherIsBetter: true },
  "Silver Slugger": { higherIsBetter: true },

  // Team stats
  GP: { higherIsBetter: true }, // Games Played
  W: { higherIsBetter: true }, // Wins
  L: { higherIsBetter: false }, // Losses (lower is better)
  "R/Gm": { higherIsBetter: true }, // Runs per game
  "RA/Gm": { higherIsBetter: false }, // Runs allowed per game (lower is better)
  "Playoff Apps.": { higherIsBetter: true }, // Playoff appearances
  "WS Apps.": { higherIsBetter: true }, // World Series appearances
  "WS Championships": { higherIsBetter: true }, // Championships

  // Head-to-head records
  "Reg Season": { higherIsBetter: true }, // More wins is better
  "Playoff Games": { higherIsBetter: true },
  "Playoff Series": { higherIsBetter: true },
  Playoffs: { higherIsBetter: true },

  // Awards - these are the formatted versions that appear in your table
  "**All-MLB Team - First Team**": { higherIsBetter: true },
  "**All-MLB Team - Second Team**": { higherIsBetter: true },
  "**Player of the Month**": { higherIsBetter: true },
  "**Player of the Week**": { higherIsBetter: true },
  "**TSN All-Star**": { higherIsBetter: true },
  "**MVP**": { higherIsBetter: true },
  "**Cy Young**": { higherIsBetter: true },
  "**Rookie of the Year**": { higherIsBetter: true },
  "**World Series MVP**": { higherIsBetter: true },
  "**All-Star Game MVP**": { higherIsBetter: true },
  "**Silver Slugger**": { higherIsBetter: true },
  "**Gold Glove**": { higherIsBetter: true },
  "**Hank Aaron Award**": { higherIsBetter: true },
  "**Comeback Player**": { higherIsBetter: true },
  "**NLCS MVP**": { higherIsBetter: true },
  "**ALCS MVP**": { higherIsBetter: true },

  // Clean versions without asterisks (fallback)
  "All-MLB Team - First Team": { higherIsBetter: true },
  "All-MLB Team - Second Team": { higherIsBetter: true },
  "Player of the Month": { higherIsBetter: true },
  "Player of the Week": { higherIsBetter: true },
  "TSN All-Star": { higherIsBetter: true },
  MVP: { higherIsBetter: true },
  "Cy Young": { higherIsBetter: true },
  "Rookie of the Year": { higherIsBetter: true },
  "World Series MVP": { higherIsBetter: true },
  "All-Star Game MVP": { higherIsBetter: true },
  "Hank Aaron Award": { higherIsBetter: true },
  "Comeback Player": { higherIsBetter: true },

  // Pitching stats - lower is better for some
  ERA: { higherIsBetter: false },
  WHIP: { higherIsBetter: false },
  L: { higherIsBetter: false }, // Losses

  // Pitching stats - higher is better
  W: { higherIsBetter: true }, // Wins
  K: { higherIsBetter: true }, // Strikeouts
  SO: { higherIsBetter: true }, // Strikeouts
  SV: { higherIsBetter: true }, // Saves
  IP: { higherIsBetter: true }, // Innings Pitched
  CG: { higherIsBetter: true }, // Complete Games
  SHO: { higherIsBetter: true }, // Shutouts
};

// highlights better stats
function highlightBetterStats(tableId = "comparisonTable") {
  const table = document.getElementById(tableId);
  if (!table) return;

  const rows = table.querySelectorAll("tbody tr");

  rows.forEach((row) => {
    const cells = row.querySelectorAll("td");
    if (cells.length !== 3) return; // Skip if not a stat row (player A, stat label, player B)

    // Skip section headers
    if (row.classList.contains("section-header")) return;

    const playerACell = cells[0];
    const statLabelCell = cells[1];
    const playerBCell = cells[2];

    const statLabel = statLabelCell.textContent.trim();

    // Skip section headers by text content
    if (
      statLabel.includes("Head-to-Head") ||
      statLabel.includes("Overall Stats")
    )
      return;

    // Handle head-to-head records specially (format: "5-3" vs "3-5")
    if (isHeadToHeadStat(statLabel)) {
      highlightHeadToHeadRecord(playerACell, playerBCell, statLabel);
      return;
    }

    // Extract award name from formats like "2**All-MLB Team - Second Team**6"
    let cleanStatLabel = statLabel;
    const asteriskMatch = statLabel.match(/\*\*(.*?)\*\*/);
    if (asteriskMatch) {
      cleanStatLabel = asteriskMatch[1]; // Extract text between ** **
    }

    // Try to find matching stat configuration
    let statConfig =
      statConfigurations[cleanStatLabel] ||
      statConfigurations["**" + cleanStatLabel + "**"] ||
      statConfigurations[statLabel];

    // If still not found, try partial matching for complex award names
    if (!statConfig) {
      const labelLower = cleanStatLabel.toLowerCase();
      for (const [key, config] of Object.entries(statConfigurations)) {
        const keyLower = key.toLowerCase().replace(/\*\*/g, "");
        if (labelLower.includes(keyLower) || keyLower.includes(labelLower)) {
          statConfig = config;
          break;
        }
      }
    }

    if (!statConfig) {
      return; // Skip unknown stats
    }

    // Parse values - handle different number formats
    const playerAValue = parseStatValue(playerACell.textContent.trim());
    const playerBValue = parseStatValue(playerBCell.textContent.trim());

    if (playerAValue === null || playerBValue === null) return; // Skip if can't parse

    // Remove existing highlighting
    playerACell.classList.remove("better-stat", "tied-stat");
    playerBCell.classList.remove("better-stat", "tied-stat");

    // Apply highlighting based on comparison
    if (playerAValue === playerBValue) {
      // Tied stats (optional)
      playerACell.classList.add("tied-stat");
      playerBCell.classList.add("tied-stat");
    } else {
      const playerABetter = statConfig.higherIsBetter
        ? playerAValue > playerBValue
        : playerAValue < playerBValue;

      if (playerABetter) {
        playerACell.classList.add("better-stat");
      } else {
        playerBCell.classList.add("better-stat");
      }
    }
  });
}

// defines head to head stats
function isHeadToHeadStat(statLabel) {
  const h2hStats = [
    "Reg Season",
    "Playoff Games",
    "Playoff Series",
    "Playoffs",
  ];
  return h2hStats.includes(statLabel);
}

// highlights team with better head to head record
function highlightHeadToHeadRecord(cellA, cellB, statLabel) {
  const valueA = cellA.textContent.trim();
  const valueB = cellB.textContent.trim();

  // Parse records like "5-3" to extract wins
  const matchA = valueA.match(/^(\d+)-(\d+)$/);
  const matchB = valueB.match(/^(\d+)-(\d+)$/);

  if (!matchA || !matchB) return;

  const winsA = parseInt(matchA[1]);
  const winsB = parseInt(matchB[1]);

  // Remove existing highlighting
  cellA.classList.remove("better-stat", "tied-stat");
  cellB.classList.remove("better-stat", "tied-stat");

  if (winsA === winsB) {
    cellA.classList.add("tied-stat");
    cellB.classList.add("tied-stat");
  } else if (winsA > winsB) {
    cellA.classList.add("better-stat");
  } else {
    cellB.classList.add("better-stat");
  }
}

// formats recieved player stats
function parseStatValue(valueStr) {
  // Handle different stat formats
  if (!valueStr || valueStr === "-" || valueStr === "N/A") return null;

  // Remove commas and convert to number
  const cleanValue = valueStr.replace(/,/g, "");
  const numValue = parseFloat(cleanValue);

  return isNaN(numValue) ? null : numValue;
}

// Function to call highlighting after comparison table is populated
function applyStatHighlighting() {
  // Wait a bit for the table to be fully rendered
  setTimeout(highlightBetterStats, 100);
}

// handler for below function
async function handleTwoWayPlayerSelection(originalName, options, mode) {
  return new Promise((resolve) => {
    showTwoWaySelectionModal(options, originalName, resolve, mode);
  });
}

// modal for two way player hitter/pitcher stat display
function showTwoWaySelectionModal(options, originalName, callback, mode) {
  // Remove any existing modal
  const existingModal = document.getElementById("two-way-modal");
  if (existingModal) {
    existingModal.remove();
  }

  const modal = document.createElement("div");
  modal.id = "two-way-modal";
  modal.className = "modal";
  modal.style.cssText = `
    display: block;
    position: fixed;
    z-index: 1000;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0,0,0,0.5);
  `;

  const html = `
    <div class="modal-content" style="
      background-color: #fff;
      margin: 15% auto;
      padding: 24px;
      border-radius: 8px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
      width: 90%;
      max-width: 500px;
    ">
      <h3 style="margin-top: 0; margin-bottom: 16px; color: #333; font-size: 20px;">
        Two-Way Player Detected
      </h3>
      <p style="margin-bottom: 20px; color: #666; line-height: 1.5;">
        ${originalName} was both a significant pitcher and hitter. Please select which stats to display:
      </p>
      <div class="player-type-options" style="margin-bottom: 24px;">
        ${options
          .map(
            (option) => `
          <div class="player-type-option" data-type="${option.type}" style="
            padding: 16px;
            border: 2px solid #e9ecef;
            border-radius: 6px;
            margin-bottom: 12px;
            cursor: pointer;
            transition: all 0.2s ease;
            text-align: center;
          " onmouseover="this.style.borderColor='#007bff'; this.style.backgroundColor='#f8f9ff';" 
             onmouseout="this.style.borderColor='#e9ecef'; this.style.backgroundColor='white';">
            <div class="option-content">
              <strong style="display: block; font-size: 16px; color: #333; margin-bottom: 4px;">
                ${option.label}
              </strong>
              <div style="font-size: 13px; color: #666;">
                ${
                  option.type === "pitcher"
                    ? "Wins, Losses, ERA, Strikeouts, etc."
                    : "Batting Average, Home Runs, RBIs, etc."
                }
              </div>
            </div>
          </div>
        `,
          )
          .join("")}
      </div>
      <button class="modal-close" style="
        background-color: #6c757d;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 14px;
      " onmouseover="this.style.backgroundColor='#5a6268';" 
         onmouseout="this.style.backgroundColor='#6c757d';">
        Cancel
      </button>
    </div>
  `;

  modal.innerHTML = html;
  document.body.appendChild(modal);

  // Add event handlers
  modal.querySelectorAll(".player-type-option").forEach((option) => {
    option.addEventListener("click", async function () {
      const selectedType = this.dataset.type;
      modal.remove();

      // Fetch stats for selected player type using the two-way endpoint
      try {
        const response = await fetch(
          `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
            originalName,
          )}&mode=${mode}&player_type=${selectedType}`,
        );
        const result = await response.json();
        callback(result);
      } catch (error) {
        callback({ error: "Failed to fetch selected player type data" });
      }
    });
  });

  modal.querySelector(".modal-close").addEventListener("click", function () {
    modal.remove();
    callback({ error: "User cancelled two-way selection" });
  });

  // Close modal when clicking outside
  modal.addEventListener("click", function (e) {
    if (e.target === modal) {
      modal.remove();
      callback({ error: "User cancelled two-way selection" });
    }
  });
}

async function showTwoWayComparisonModal(nameA, nameB, optionsA, optionsB) {
  return new Promise((resolve) => {
    // Remove any existing modal
    const existingModal = document.getElementById("two-way-comparison-modal");
    if (existingModal) {
      existingModal.remove();
    }

    const modal = document.createElement("div");
    modal.id = "two-way-comparison-modal";
    modal.className = "modal";
    modal.style.cssText = `
      display: block;
      position: fixed;
      z-index: 1000;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0,0,0,0.5);
    `;

    const html = `
      <div class="modal-content" style="
        background-color: #fff;
        margin: 15% auto;
        padding: 32px;
        border-radius: 12px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
        width: 90%;
        max-width: 500px;
        text-align: center;
      ">
        <h3 style="margin-top: 0; margin-bottom: 20px; color: #333; font-size: 24px;">
          Two-Way Players Comparison
        </h3>
        <p style="margin-bottom: 32px; color: #666; line-height: 1.6; font-size: 16px;">
          Both <strong>${nameA}</strong> and <strong>${nameB}</strong> were significant pitchers and hitters.<br>
          Choose which stats to compare:
        </p>
        
        <!-- Main comparison buttons -->
        <div style="margin-bottom: 24px;">
          <button class="comparison-type-btn" data-types="pitcher,pitcher" style="
            background: linear-gradient(135deg, #28a745, #20c997);
            color: white;
            border: none;
            padding: 16px 24px;
            border-radius: 8px;
            margin: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            min-width: 180px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(40, 167, 69, 0.3);
          " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 6px 16px rgba(40, 167, 69, 0.4)';" 
             onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 4px 12px rgba(40, 167, 69, 0.3)';">
            Compare as Pitchers
            <div style="font-size: 12px; font-weight: normal; opacity: 0.9; margin-top: 4px;">
              ERA, Wins, Strikeouts, etc.
            </div>
          </button>
          
          <button class="comparison-type-btn" data-types="hitter,hitter" style="
            background: linear-gradient(135deg, #007bff, #6f42c1);
            color: white;
            border: none;
            padding: 16px 24px;
            border-radius: 8px;
            margin: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            min-width: 180px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(0, 123, 255, 0.3);
          " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 6px 16px rgba(0, 123, 255, 0.4)';" 
             onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 4px 12px rgba(0, 123, 255, 0.3)';">
            Compare as Hitters
            <div style="font-size: 12px; font-weight: normal; opacity: 0.9; margin-top: 4px;">
              Batting Avg, Home Runs, RBIs, etc.
            </div>
          </button>
        </div>

        <button class="modal-close" style="
          background-color: #6c757d;
          color: white;
          border: none;
          padding: 10px 20px;
          border-radius: 6px;
          cursor: pointer;
          font-size: 14px;
        " onmouseover="this.style.backgroundColor='#5a6268';" 
           onmouseout="this.style.backgroundColor='#6c757d';">
          Cancel
        </button>
      </div>
    `;

    modal.innerHTML = html;
    document.body.appendChild(modal);

    // NEW:
    modal.querySelectorAll(".comparison-type-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const types = btn.dataset.types.split(",");
        modal.remove();

        // Actually fetch the stats for both players with the selected types
        try {
          const mode = document.getElementById("viewMode").value;
          const [statsA, statsB] = await Promise.all([
            fetch(
              `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
                nameA,
              )}&mode=${mode}&player_type=${types[0]}`,
            ).then((r) => r.json()),
            fetch(
              `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
                nameB,
              )}&mode=${mode}&player_type=${types[1]}`,
            ).then((r) => r.json()),
          ]);

          resolve({
            statsA: statsA,
            statsB: statsB,
            playerAType: types[0],
            playerBType: types[1],
          });
        } catch (error) {
          console.error("Error fetching two-way stats:", error);
          resolve({ error: "Failed to fetch stats" });
        }
      });
    });

    // Cancel functionality
    modal.querySelector(".modal-close").addEventListener("click", () => {
      modal.remove();
      resolve({ error: "User cancelled two-way comparison" });
    });

    // Close modal when clicking outside
    modal.addEventListener("click", (e) => {
      if (e.target === modal) {
        modal.remove();
        resolve({ error: "User cancelled two-way comparison" });
      }
    });
  });
}

// calls below function to handle disambiguation in father/son cases
async function handleDisambiguation(originalName, suggestions, mode) {
  return new Promise((resolve) => {
    showDisambiguationModal(suggestions, originalName, resolve, mode);
  });
}

let currentDisambiguationPlayer = null;

// modal to disambiguate father/son cases
function showDisambiguationModal(suggestions, originalName, callback, mode) {
  // Remove any existing modal
  const existingModal = document.getElementById("disambiguation-modal");
  if (existingModal) {
    existingModal.remove();
  }

  const modal = document.createElement("div");
  modal.id = "disambiguation-modal";
  modal.className = "modal";
  modal.style.cssText = `
    display: block;
    position: fixed;
    z-index: 1000;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0,0,0,0.5);
  `;

  const cleanName = originalName.split(" Jr.")[0].split(" Sr.")[0];

  const html = `
    <div class="modal-content" style="
      background-color: #fff;
      margin: 15% auto;
      padding: 24px;
      border-radius: 8px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
      width: 90%;
      max-width: 500px;
    ">
      <h3 style="margin-top: 0; margin-bottom: 16px; color: #333; font-size: 20px;">
        Multiple Players Found
      </h3>
      <p style="margin-bottom: 20px; color: #666; line-height: 1.5;">
        Found multiple players named "${cleanName}". Please select which player:
      </p>
      <div class="player-options" style="margin-bottom: 24px;">
        ${suggestions
          .map(
            (player, index) => `
          <div class="player-option" data-name="${
            player.name
          }" data-playerid="${player.playerid}" style="
            padding: 16px;
            border: 2px solid #e9ecef;
            border-radius: 6px;
            margin-bottom: 12px;
            cursor: pointer;
            transition: all 0.2s ease;
          " onmouseover="this.style.borderColor='#007bff'; this.style.backgroundColor='#f8f9ff';" 
             onmouseout="this.style.borderColor='#e9ecef'; this.style.backgroundColor='white';">
            <div class="player-details">
              <strong style="display: block; font-size: 16px; color: #333; margin-bottom: 4px;">
                ${player.name}
              </strong>
              <div class="player-meta" style="font-size: 13px; color: #666;">
                Debut: ${player.debut_year} | Born: ${player.birth_year}
                ${player.playerid ? ` | ID: ${player.playerid}` : ""}
              </div>
            </div>
          </div>
        `,
          )
          .join("")}
      </div>
      <button class="modal-close" style="
        background-color: #6c757d;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 14px;
      " onmouseover="this.style.backgroundColor='#5a6268';" 
         onmouseout="this.style.backgroundColor='#6c757d';">
        Cancel
      </button>
    </div>
  `;

  modal.innerHTML = html;
  document.body.appendChild(modal);

  // Add event handlers
  modal.querySelectorAll(".player-option").forEach((option) => {
    option.addEventListener("click", async function () {
      const selectedName = this.dataset.name;
      const selectedPlayerId = this.dataset.playerid;
      modal.remove();

      // Hide any open dropdowns to prevent interference
      hideAllDropdowns();

      // Fetch stats for selected player using the exact name from suggestions
      try {
        const response = await fetch(
          `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
            selectedName,
          )}&mode=${mode}`,
        );
        if (response.ok) {
          const result = await response.json();
          // Add the selected name to the result so we can use it in the display
          result.selected_name = selectedName;
          result.original_search_name = originalName;
          callback(result);
        } else {
          const errorData = await response
            .json()
            .catch(() => ({ error: `HTTP ${response.status}` }));
          console.error("Error fetching selected player:", errorData);
          callback(errorData);
        }
      } catch (error) {
        console.error("Error in disambiguation selection:", error);
        callback({ error: "Failed to fetch selected player data" });
      }
    });
  });

  modal.querySelector(".modal-close").addEventListener("click", function () {
    modal.remove();
    callback({ error: "User cancelled disambiguation" });
  });

  // Close modal when clicking outside
  modal.addEventListener("click", function (e) {
    if (e.target === modal) {
      modal.remove();
      callback({ error: "User cancelled disambiguation" });
    }
  });
}

// Function to properly capitalize player names
function capitalizePlayerName(name) {
  if (!name) return name;

  // Handle special cases and particles that should remain lowercase
  const lowercaseWords = [
    "de",
    "del",
    "la",
    "le",
    "van",
    "von",
    "da",
    "dos",
    "el",
  ];
  const romanNumerals = [
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "vii",
    "viii",
    "ix",
    "x",
  ];

  return name
    .toLowerCase()
    .split(" ")
    .map((word, index) => {
      // Always capitalize first word
      if (index === 0) {
        return capitalizeWord(word);
      }

      // Handle Jr., Sr., III, etc.
      if (word.includes(".") || romanNumerals.includes(word.toLowerCase())) {
        return word.toUpperCase();
      }

      // Keep certain particles lowercase (unless they're the first word)
      if (lowercaseWords.includes(word.toLowerCase())) {
        return word.toLowerCase();
      }

      // Handle hyphenated names (like Mary-Jane)
      if (word.includes("-")) {
        return word
          .split("-")
          .map((part) => capitalizeWord(part))
          .join("-");
      }

      // Handle apostrophes (like O'Connor, D'Angelo)
      if (word.includes("'")) {
        return word
          .split("'")
          .map((part) => capitalizeWord(part))
          .join("'");
      }

      // Regular capitalization
      return capitalizeWord(word);
    })
    .join(" ");
}

// Helper function to capitalize a single word
function capitalizeWord(word) {
  if (!word) return word;
  return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
}

// Alternative simpler function if you just want basic title case
function simpleCapitalize(name) {
  if (!name) return name;

  return name
    .toLowerCase()
    .split(" ")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

// compares players
async function comparePlayers() {
  const nameA = document.getElementById("playerA").value.trim();
  const nameB = document.getElementById("playerB").value.trim();
  const mode = document.getElementById("viewMode").value;

  // Hide any open dropdowns
  hideAllDropdowns();

  // Show loading indicator
  const tbody = document.getElementById("comparisonBody");
  tbody.innerHTML = `<tr><td colspan='3' style='text-align: center; padding: 20px;'>Loading player data. This may take a moment...</td></tr>`;

  // First, try to fetch both players without player_type to detect if they're two-way
  const [initialResA, initialResB] = await Promise.all([
    fetchStatsInitial(nameA, mode),
    fetchStatsInitial(nameB, mode),
  ]);

  // Handle disambiguation for both players if needed
  let resA = initialResA;
  let resB = initialResB;

  if (resA && resA.error && resA.error.includes("Multiple players found")) {
    resA = await handleDisambiguation(nameA, resA.suggestions, mode);
  }

  if (resB && resB.error && resB.error.includes("Multiple players found")) {
    resB = await handleDisambiguation(nameB, resB.suggestions, mode);
  }

  // Check if both players are two-way players
  const playerAIsTwoWay = resA && resA.player_type === "two-way";
  const playerBIsTwoWay = resB && resB.player_type === "two-way";

  // Handle different two-way scenarios
  if (playerAIsTwoWay && playerBIsTwoWay) {
    // Both are two-way players - show coordinated selection modal
    const result = await showTwoWayComparisonModal(
      nameA,
      nameB,
      resA.options,
      resB.options,
    );
    if (result.error) {
      tbody.innerHTML = `<tr><td colspan='3' style='text-align: center; padding: 20px;'>${result.error}</td></tr>`;
      return;
    }

    // The modal now returns the fetched stats directly
    resA = result.statsA;
    resB = result.statsB;
  } else if (playerAIsTwoWay && !playerBIsTwoWay) {
    // Only Player A is two-way
    const selectedType = await handleTwoWayPlayerSelection(
      nameA,
      resA.options,
      mode,
    );
    if (selectedType.error) {
      tbody.innerHTML = `<tr><td colspan='3' style='text-align: center; padding: 20px;'>${selectedType.error}</td></tr>`;
      return;
    }
    resA = selectedType;
  } else if (!playerAIsTwoWay && playerBIsTwoWay) {
    // Only Player B is two-way
    const selectedType = await handleTwoWayPlayerSelection(
      nameB,
      resB.options,
      mode,
    );
    if (selectedType.error) {
      tbody.innerHTML = `<tr><td colspan='3' style='text-align: center; padding: 20px;'>${selectedType.error}</td></tr>`;
      return;
    }
    resB = selectedType;
  }

  // Use the selected names if available, otherwise use the input names
  const displayNameA = resA?.selected_name || nameA;
  const displayNameB = resB?.selected_name || nameB;

  updateComparisonTable(resA, resB, displayNameA, displayNameB);
  applyStatHighlighting();
}

// fetches player stats from backend
async function fetchStatsInitial(name, mode) {
  try {
    let backendMode = mode;
    if (mode === "newest" || mode === "oldest") {
      backendMode = "season";
    }

    const url = `${backendBaseUrl}/player-two-way?name=${encodeURIComponent(
      name,
    )}&mode=${backendMode}`;
    const response = await fetch(url);

    if (response.status === 422) {
      // Multiple players found - return for handling
      const data = await response.json();
      return { error: "Multiple players found", suggestions: data.suggestions };
    }

    if (response.status === 423) {
      // Two-way player found
      const data = await response.json();
      return data; // Return the two-way data for handling
    }

    if (response.ok) {
      return await response.json();
    }

    // Fallback to original endpoint
    const fallbackResponse = await fetch(
      `${backendBaseUrl}/player?name=${encodeURIComponent(
        name,
      )}&mode=${backendMode}`,
    );
    return await fallbackResponse.json();
  } catch (e) {
    console.error("Fetch error:", e);
    return { error: "Failed to fetch data" };
  }
}

//document.getElementById("viewMode").addEventListener("change", comparePlayers);

// CUSTOM DROPDOWN FUNCTIONALITY
let searchTimeout;
let popularPlayersCache = null;
let currentDropdown = null;
const SEARCH_DELAY = 0;

// Show dropdown with players
// FIXED: Better dropdown display
function showDropdown(inputId, players) {
  let dropdownId;

  // Determine dropdown ID based on input ID
  if (inputId === "playerA") dropdownId = "dropdownA";
  else if (inputId === "playerB") dropdownId = "dropdownB";
  else if (inputId === "teamA") dropdownId = "dropdownTeamA";
  else if (inputId === "teamB") dropdownId = "dropdownTeamB";

  if (!dropdownId) {
    console.error("ERROR: No dropdownId determined for inputId:", inputId);
    return;
  }

  const dropdown = document.getElementById(dropdownId);

  if (!dropdown) {
    console.error("ERROR: Dropdown element not found:", dropdownId);
    return;
  }

  // Hide other dropdowns first
  hideAllDropdowns();

  dropdown.innerHTML = "";

  if (!players || players.length === 0) {
    const message = inputId.startsWith("team")
      ? "No teams found"
      : "No players found";
    dropdown.innerHTML = `<div class="dropdown-item" style="color: #999; cursor: default; padding: 12px;">${message}</div>`;
  } else {
    players.forEach((player, index) => {
      const item = document.createElement("div");
      item.className = "dropdown-item";

      if (typeof player === "string") {
        item.textContent = player;
        item.dataset.value = player;
      } else {
        const name = player.name || player.display;
        const display = player.display || player.name;

        // Create HTML with name and optional extra info
        const nameSpan = document.createElement("span");
        nameSpan.textContent = name;
        item.appendChild(nameSpan);

        if (display !== name && display.includes("(")) {
          const infoSpan = document.createElement("span");
          infoSpan.className = "player-years";
          infoSpan.textContent = " " + display.substring(display.indexOf("("));
          infoSpan.style.color = "#666";
          infoSpan.style.fontSize = "0.9em";
          item.appendChild(infoSpan);
        }

        item.dataset.value = name;
      }

      item.addEventListener("click", () => {
        document.getElementById(inputId).value = item.dataset.value;
        hideDropdown(dropdownId);

        // Auto-compare if both fields are filled
        const isTeamInput = inputId.startsWith("team");
        if (isTeamInput) {
          const otherInput = inputId === "teamA" ? "teamB" : "teamA";
          if (document.getElementById(otherInput).value.trim()) {
            compareTeams();
          }
        } else {
          const otherInput = inputId === "playerA" ? "playerB" : "playerA";
          if (document.getElementById(otherInput).value.trim()) {
            comparePlayers();
          }
        }
      });

      dropdown.appendChild(item);
    });
  }

  dropdown.classList.add("show");
  currentDropdown = dropdownId;
}

// Hide specific dropdown
function hideDropdown(dropdownId) {
  const dropdown = document.getElementById(dropdownId);
  dropdown.classList.remove("show");
  if (currentDropdown === dropdownId) {
    currentDropdown = null;
  }
}

// Hide all dropdowns
function hideAllDropdowns() {
  ["dropdownA", "dropdownB", "dropdownTeamA", "dropdownTeamB"].forEach((id) => {
    const dropdown = document.getElementById(id);
    if (dropdown) {
      dropdown.classList.remove("show");
    }
  });
  currentDropdown = null;
}

// Load popular players
async function loadPopularPlayers(inputId) {
  if (popularPlayersCache) {
    showDropdown(inputId, popularPlayersCache);
    return;
  }

  try {
    const response = await fetch(`${backendBaseUrl}/popular-players`);
    if (response.ok) {
      const players = await response.json();
      popularPlayersCache = players;
      showDropdown(inputId, players);
    } else {
      const fallback = [
        "Mike Trout",
        "Aaron Judge",
        "Mookie Betts",
        "Ronald Acuña",
        "Juan Soto",
        "Gerrit Cole",
        "Jacob deGrom",
        "Clayton Kershaw",
        "Vladimir Guerrero Jr.",
        "Fernando Tatis Jr.",
        "Shane Bieber",
        "Freddie Freeman",
        "Manny Machado",
        "Jose Altuve",
        "Kyle Tucker",
      ];
      showDropdown(inputId, fallback);
    }
  } catch (error) {
    console.error("Error loading popular players:", error);
    const fallback = [
      "Mike Trout",
      "Aaron Judge",
      "Mookie Betts",
      "Ronald Acuña",
      "Juan Soto",
      "Gerrit Cole",
      "Jacob deGrom",
      "Clayton Kershaw",
    ];
    showDropdown(inputId, fallback);
  }
}

// Enhanced search for players
async function searchPlayersEnhanced(query, inputId) {
  // Don't search if query is too short
  if (!query || query.trim().length === 0) {
    loadPopularPlayers(inputId);
    return;
  }

  const trimmedQuery = query.trim();
  lastSearchQuery = trimmedQuery; // Store current search query

  try {
    // Properly encode the query - this handles spaces and special characters
    const encodedQuery = encodeURIComponent(trimmedQuery);
    const url = `${backendBaseUrl}/search-players?q=${encodedQuery}`;

    const response = await fetch(url);

    // Check if this search is still relevant (user might have typed something else)
    const currentInputValue = document.getElementById(inputId).value.trim();
    if (currentInputValue !== trimmedQuery) {
      return; // User has typed something else, ignore these results
    }

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Response not OK. Status:", response.status);
      console.error("Error text:", errorText);
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const players = await response.json();

    // Double-check search is still relevant before showing results
    const finalInputValue = document.getElementById(inputId).value.trim();
    if (finalInputValue !== trimmedQuery) {
      return;
    }

    // Check if we got an error response
    if (players.error) {
      console.error("Backend returned error:", players.error);
      loadPopularPlayers(inputId);
      return;
    }

    // Format players for display
    const formattedPlayers = players.map((player, index) => {
      // Handle both string format and object format
      if (typeof player === "string") {
        return {
          name: player,
          display: player,
        };
      } else {
        return {
          name: player.original_name || player.name,
          display: player.display || player.name,
        };
      }
    });

    // Show the results
    if (formattedPlayers.length > 0) {
      showDropdown(inputId, formattedPlayers);
    } else {
      loadPopularPlayers(inputId);
    }
  } catch (error) {
    console.error("=== Enhanced search error ===");
    console.error("Error type:", error.constructor.name);
    console.error("Error message:", error.message);
    console.error("Error stack:", error.stack);

    // Fallback to popular players on error
    loadPopularPlayers(inputId);
  }
}

// FIXED: Input handler with better debouncing and immediate feedback
function handlePlayerInput(e) {
  const query = e.target.value; // Don't trim here - let user see what they're typing
  const inputId = e.target.id;

  // Clear any existing timeout
  clearTimeout(searchTimeout);

  // If query is empty or just whitespace, show popular players immediately
  if (!query || query.trim().length === 0) {
    loadPopularPlayers(inputId);
    return;
  }

  // If query is too short (less than 2 non-whitespace characters), show popular immediately
  if (query.trim().length < 2) {
    loadPopularPlayers(inputId);
    return;
  }

  // For queries 2+ characters, search after delay
  searchTimeout = setTimeout(() => {
    searchPlayersEnhanced(query.trim(), inputId);
  }, 200); // 200ms delay to reduce API calls while typing
}

// Click event handler
function handlePlayerClick(e) {
  const inputId = e.target.id;
  const query = e.target.value.trim();

  if (query.length < 2) {
    loadPopularPlayers(inputId);
  } else {
    searchPlayersEnhanced(query, inputId);
  }
}

// Focus event handler
function handlePlayerFocus(e) {
  const inputId = e.target.id;
  const query = e.target.value.trim();

  if (query.length < 2) {
    loadPopularPlayers(inputId);
  } else {
    searchPlayersEnhanced(query, inputId);
  }
}

// Handle keyboard navigation
function handleEnterKey(e) {
  const inputId = e.target.id;
  const query = e.target.value.trim();

  if (e.key === "Enter") {
    e.preventDefault();

    // Search if there's a query, otherwise just compare
    if (query.length >= 2) {
      searchPlayersEnhanced(query, inputId);
    } else {
      const dropdownId = inputId === "playerA" ? "dropdownA" : "dropdownB";
      hideDropdown(dropdownId);
      comparePlayers();
    }
  } else if (e.key === "Escape") {
    const dropdownId = inputId === "playerA" ? "dropdownA" : "dropdownB";
    hideDropdown(dropdownId);
  }
}

// Setup event listeners
function setupPlayerAutofill() {
  const playerAInput = document.getElementById("playerA");
  const playerBInput = document.getElementById("playerB");

  if (playerAInput) {
    playerAInput.addEventListener("input", handlePlayerInput);
    playerAInput.addEventListener("click", handlePlayerClick);
    playerAInput.addEventListener("focus", handlePlayerFocus);
    playerAInput.addEventListener("keydown", handleEnterKey);
  }

  if (playerBInput) {
    playerBInput.addEventListener("input", handlePlayerInput);
    playerBInput.addEventListener("click", handlePlayerClick);
    playerBInput.addEventListener("focus", handlePlayerFocus);
    playerBInput.addEventListener("keydown", handleEnterKey);
  }

  // Hide dropdown when clicking outside
  document.addEventListener("click", (e) => {
    if (currentDropdown && !e.target.closest(".input-container")) {
      hideAllDropdowns();
    }
  });
}

// Simplified team stats - matching StatHead format exactly
const teamStatsLabelMap = {
  // Team Record
  gp: "GP",
  w: "W",
  l: "L",
  rpg: "R/Gm",
  rapg: "RA/Gm",

  // Playoff stats (you'll need to add these to your backend)
  playoff_apps: "Playoff Apps.",
  ws_apps: "WS Apps.",
  ws_championships: "WS Championships",
};

// fetches team stats from backend
async function fetchTeamStats(team, mode) {
  try {
    const response = await fetch(
      `${backendBaseUrl}/team?team=${encodeURIComponent(team)}&mode=${mode}`,
    );
    return await response.json();
  } catch (e) {
    console.error("Team fetch error:", e);
    return { error: "Failed to fetch team data" };
  }
}

// fetches head to head record from backend
async function fetchHeadToHeadRecord(teamA, teamB, mode) {
  try {
    let url = `${backendBaseUrl}/team/h2h?team_a=${encodeURIComponent(
      teamA,
    )}&team_b=${encodeURIComponent(teamB)}`;

    // Handle year logic based on mode
    if (mode === "season") {
      // Extract years from both team names
      const yearMatchA = teamA.match(/\b(19|20)\d{2}\b/);
      const yearMatchB = teamB.match(/\b(19|20)\d{2}\b/);

      if (yearMatchA && yearMatchB) {
        const yearA = yearMatchA[0];
        const yearB = yearMatchB[0];

        // Only proceed if both teams have the same year
        if (yearA === yearB) {
          url += `&year=${yearA}`;
        } else {
          // Years don't match - return empty H2H data
          return {
            head_to_head: {
              regular_season: { team_a_wins: 0, team_b_wins: 0 },
              playoffs: { game_wins: { team_a: 0, team_b: 0 } },
            },
          };
        }
      } else if (yearMatchA || yearMatchB) {
        // One team has a year, the other doesn't - return empty H2H data
        return {
          head_to_head: {
            regular_season: { team_a_wins: 0, team_b_wins: 0 },
            playoffs: { game_wins: { team_a: 0, team_b: 0 } },
          },
        };
      } else {
        // Neither team has a year - default to 2024 (matching your single team behavior)
        url += `&year=2024`;
      }
    }
    // For career/combined mode, don't add year parameter to get all-time H2H

    const response = await fetch(url);
    const data = await response.json();

    return {
      head_to_head: data,
    };
  } catch (e) {
    console.error("H2H fetch error:", e);
    return { error: "Failed to fetch H2H data" };
  }
}

// Team comparison function
async function compareTeams() {
  const teamA = document.getElementById("teamA").value.trim();
  const teamB = document.getElementById("teamB").value.trim();
  const mode = document.getElementById("teamViewMode").value;

  if (!teamA || !teamB) {
    alert("Please enter both teams (e.g., '2024 Cubs' or 'Cubs')");
    return;
  }

  const tbody = document.getElementById("teamComparisonBody");
  tbody.innerHTML = `<tr><td colspan='3' style='text-align: center; padding: 20px;'>Loading team data. This may take a moment.</td></tr>`;

  const [resA, resB] = await Promise.all([
    fetchTeamStats(teamA, mode),
    fetchTeamStats(teamB, mode),
  ]);

  updateTeamComparisonTable(resA, resB, teamA, teamB, mode);

  // Load team logos
  const logoA = document.getElementById("teamLogoA");
  const logoB = document.getElementById("teamLogoB");

  if (resA?.team_logo && logoA) {
    loadTeamLogo(resA.team_logo, logoA, resA.team_name);
  }

  if (resB?.team_logo && logoB) {
    loadTeamLogo(resB.team_logo, logoB, resB.team_name);
  }
}

// uses verious other functions to update table
function updateTeamComparisonTable(resA, resB, teamA, teamB, mode) {
  const tbody = document.getElementById("teamComparisonBody");
  const thA = document.getElementById("teamAName");
  const thB = document.getElementById("teamBName");

  // Update headers with team names
  thA.innerHTML = `
        <div class="team-header">
            <img id="teamLogoA" class="team-logo" style="display: none;" alt="Team A Logo">
            <span class="team-name">${resA?.team_name || teamA}</span>
        </div>
    `;

  thB.innerHTML = `
        <div class="team-header">
            <img id="teamLogoB" class="team-logo" style="display: none;" alt="Team B Logo">
            <span class="team-name">${resB?.team_name || teamB}</span>
        </div>
    `;

  tbody.innerHTML = "";

  if (!resA || !resB) {
    tbody.innerHTML = `<tr><td colspan='3'>Error loading team data.</td></tr>`;
    return;
  }

  if (resA.error || resB.error) {
    tbody.innerHTML = `<tr><td colspan='3'>${
      resA.error || resB.error
    }</td></tr>`;
    return;
  }

  const statsA = resA.stats;
  const statsB = resB.stats;

  // Fetch H2H data first, then build the entire table in the correct order
  fetchHeadToHeadRecord(teamA, teamB, mode)
    .then((h2hData) => {
      // Clear the table and rebuild everything in order
      tbody.innerHTML = "";

      // 1. Add Head-to-Head section FIRST
      addSectionToTable(tbody, "Head-to-Head");

      if (h2hData && h2hData.head_to_head) {
        const h2h = h2hData.head_to_head;

        // Regular season record
        const regRecord = h2h.regular_season;
        const teamAReg = regRecord.team_a_wins || 0;
        const teamBReg = regRecord.team_b_wins || 0;
        addStatRow(
          tbody,
          `${teamAReg}-${teamBReg}`,
          "Reg Season",
          `${teamBReg}-${teamAReg}`,
        );

        // Playoff record - use the correct data structure
        const playoffRecord = h2h.playoffs;
        let teamAPlayoff = 0;
        let teamBPlayoff = 0;

        // Check if we have game_wins data (individual games) or series_wins data
        if (playoffRecord.game_wins) {
          teamAPlayoff = playoffRecord.game_wins.team_a || 0;
          teamBPlayoff = playoffRecord.game_wins.team_b || 0;
          addStatRow(
            tbody,
            `${teamAPlayoff}-${teamBPlayoff}`,
            "Playoff Games",
            `${teamBPlayoff}-${teamAPlayoff}`,
          );
        } else if (playoffRecord.series_wins) {
          // Fallback to series wins if game wins not available
          teamAPlayoff = playoffRecord.series_wins.team_a || 0;
          teamBPlayoff = playoffRecord.series_wins.team_b || 0;
          addStatRow(
            tbody,
            `${teamAPlayoff}-${teamBPlayoff}`,
            "Playoff Series",
            `${teamBPlayoff}-${teamAPlayoff}`,
          );
        } else {
          // Legacy format fallback
          teamAPlayoff = playoffRecord.team_a_wins || 0;
          teamBPlayoff = playoffRecord.team_b_wins || 0;
          addStatRow(
            tbody,
            `${teamAPlayoff}-${teamBPlayoff}`,
            "Playoffs",
            `${teamBPlayoff}-${teamAPlayoff}`,
          );
        }
      } else {
        // Add fallback H2H data if no data available
        addStatRow(tbody, "0-0", "Reg Season", "0-0");
      }

      // 2. Then add Overall Stats section
      addSectionToTable(tbody, "Overall Stats");

      // Stats in exact StatHead order from the screenshot
      const statsOrder = [
        "gp",
        "w",
        "l",
        "rpg",
        "rapg",
        "playoff_apps",
        "ws_apps",
        "ws_championships",
      ];

      // Display stats in order
      statsOrder.forEach((key) => {
        if (teamStatsLabelMap[key]) {
          const statName = teamStatsLabelMap[key];
          let valA = statsA[key] !== undefined ? statsA[key] : 0;
          let valB = statsB[key] !== undefined ? statsB[key] : 0;

          // Format values
          valA = formatStatValue(key, valA);
          valB = formatStatValue(key, valB);

          addStatRow(tbody, valA, statName, valB);
        }
      });

      setTimeout(() => highlightBetterStats("teamComparisonTable"), 100);
    })
    .catch((error) => {
      console.error("H2H fetch error:", error);
      // Clear and rebuild with fallback data
      tbody.innerHTML = "";

      // Add Head-to-Head section even if fetch failed
      addSectionToTable(tbody, "Head-to-Head");
      addStatRow(tbody, "0-0", "Reg Season", "0-0");

      // Add Overall Stats section
      addSectionToTable(tbody, "Overall Stats");

      const statsOrder = [
        "gp",
        "w",
        "l",
        "rpg",
        "rapg",
        "playoff_apps",
        "ws_apps",
        "ws_championships",
      ];

      statsOrder.forEach((key) => {
        if (teamStatsLabelMap[key]) {
          const statName = teamStatsLabelMap[key];
          let valA = statsA[key] !== undefined ? statsA[key] : 0;
          let valB = statsB[key] !== undefined ? statsB[key] : 0;

          valA = formatStatValue(key, valA);
          valB = formatStatValue(key, valB);

          addStatRow(tbody, valA, statName, valB);
        }
      });

      setTimeout(() => highlightBetterStats("teamComparisonTable"), 100);
    });
}

// adds section header to table
function addSectionToTable(tbody, sectionName) {
  const row = document.createElement("tr");
  row.classList.add("section-header");
  row.innerHTML = `<td colspan='3' style='background-color: #e0e0e0; color: black; text-align: center; font-weight: bold; padding: 8px; font-size: 1.1rem;'>${sectionName}</td>`;
  tbody.appendChild(row);
}

// injects a stat row
function addStatRow(tbody, valA, statName, valB) {
  const row = document.createElement("tr");
  row.innerHTML = `
        <td style="text-align: center;">${valA}</td>
        <td style="background-color: #f1f3f4; text-align: center;"><strong>${statName}</strong></td>
        <td style="text-align: center;">${valB}</td>
    `;
  tbody.appendChild(row);
}

// injects stat rows after a section header
function addStatRowAfterSection(tbody, sectionName, valA, statName, valB) {
  // Find the section header
  const rows = tbody.querySelectorAll("tr");
  let insertIndex = -1;

  for (let i = 0; i < rows.length; i++) {
    if (rows[i].textContent.includes(sectionName)) {
      insertIndex = i + 1;
      for (let j = i + 1; j < rows.length; j++) {
        if (rows[j].classList.contains("section-header")) {
          break;
        }
        insertIndex = j + 1;
      }
      break;
    }
  }

  const row = document.createElement("tr");
  row.innerHTML = `
        <td style="text-align: center;">${valA}</td>
        <td style="background-color: #f1f3f4; text-align: center;"><strong>${statName}</strong></td>
        <td style="text-align: center;">${valB}</td>
    `;

  if (insertIndex >= 0 && insertIndex < tbody.children.length) {
    tbody.insertBefore(row, tbody.children[insertIndex]);
  } else {
    tbody.appendChild(row);
  }
}

// formats stat values recieved
function formatStatValue(statKey, value) {
  if (value === null || value === undefined || value === "") {
    return "0";
  }

  const numValue = parseFloat(value);
  if (isNaN(numValue)) return value;

  // Runs per game stats (show decimal)
  if (statKey === "rpg" || statKey === "rapg") {
    return numValue.toFixed(1);
  }

  // Everything else - whole numbers
  return Math.round(numValue).toString();
}

// Team logo functions
function loadTeamLogo(logoData, imgElement, teamName = "") {
  if (!logoData || !imgElement) {
    setDefaultTeamLogo(imgElement, teamName);
    return;
  }

  const urlsToTry = [];
  if (logoData.primary) urlsToTry.push(logoData.primary);
  if (logoData.fallbacks && Array.isArray(logoData.fallbacks)) {
    urlsToTry.push(...logoData.fallbacks);
  }

  tryNextLogoUrl(urlsToTry, 0, imgElement, teamName);
}

// tries next url to find team logos
function tryNextLogoUrl(urls, index, imgElement, teamName) {
  if (index >= urls.length) {
    setDefaultTeamLogo(imgElement, teamName);
    return;
  }

  const img = new Image();
  const timeout = setTimeout(() => {
    img.onload = null;
    img.onerror = null;
    tryNextLogoUrl(urls, index + 1, imgElement, teamName);
  }, 5000);

  img.onload = function () {
    clearTimeout(timeout);
    imgElement.src = urls[index];
    imgElement.style.display = "block";
  };

  img.onerror = function () {
    tryNextLogoUrl(urls, index + 1, imgElement, teamName);
  };

  img.src = urls[index];
}

// sets default team logo
function setDefaultTeamLogo(imgElement, teamName = "") {
  const initials = teamName
    .split(" ")
    .map((word) => word[0])
    .join("")
    .toUpperCase()
    .slice(0, 3);

  const svg = `
        <svg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg">
            <circle cx="30" cy="30" r="28" fill="#003366" stroke="#ffffff" stroke-width="2"/>
            <text x="30" y="38" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-size="14" font-weight="bold">
                ${initials || "?"}
            </text>
        </svg>
    `;

  imgElement.src = "data:image/svg+xml;base64," + btoa(svg);
  imgElement.style.display = "block";
}

// Mode switching (if you still need tabs)
function switchMode(mode) {
  document.querySelectorAll(".mode-tab").forEach((tab) => {
    tab.classList.remove("active");
  });
  document.querySelectorAll(".comparison-section").forEach((section) => {
    section.classList.remove("active");
  });
  document.getElementById(mode + "-section").classList.add("active");
}

let teamSearchTimeout;
let popularTeamsCache = null;

const CURRENT_TEAMS = [
  "Angels", "Astros", "Athletics", "Blue Jays", "Braves", "Brewers",
  "Cardinals", "Cubs", "Diamondbacks", "Dodgers", "Giants", "Guardians",
  "Mariners", "Marlins", "Mets", "Nationals", "Orioles", "Padres",
  "Phillies", "Pirates", "Rangers", "Rays", "Red Sox", "Reds",
  "Rockies", "Royals", "Tigers", "Twins", "White Sox", "Yankees",
];

const POPULAR_YEAR_TEAMS = [
  "2016 Cubs", "2020 Dodgers", "1998 Yankees", "2004 Red Sox",
  "2019 Nationals", "2017 Astros", "2018 Red Sox", "2001 Mariners",
  "1995 Braves", "1975 Reds", "2024 Dodgers", "2023 Rangers",
];

const HISTORIC_YEAR_TEAMS = [
  ...POPULAR_YEAR_TEAMS,
  "1986 Mets", "1984 Tigers", "2002 Angels", "2008 Phillies",
  "2010 Giants", "2011 Cardinals", "2012 Giants", "2013 Red Sox",
  "2014 Giants", "2015 Royals", "2021 Braves", "2022 Astros",
  "1927 Yankees", "1955 Dodgers", "1969 Mets", "1976 Reds",
  "1988 Dodgers", "1989 Athletics", "1990 Reds", "1991 Twins",
  "1992 Blue Jays", "1993 Blue Jays", "1996 Yankees", "1997 Marlins",
  "1999 Yankees", "2000 Yankees", "2003 Marlins", "2005 White Sox",
  "2006 Cardinals", "2007 Red Sox", "2009 Yankees",
];

const DEFUNCT_TEAMS = ["Expos", "Senators", "Browns", "Pilots", "Colt .45s"];

const ALL_TEAMS = [...CURRENT_TEAMS, ...HISTORIC_YEAR_TEAMS, ...DEFUNCT_TEAMS];

function loadPopularTeams(inputId) {
  showDropdown(inputId, [...CURRENT_TEAMS, ...POPULAR_YEAR_TEAMS]);
}

// Search teams
function searchTeams(query, inputId) {
  const filtered = ALL_TEAMS.filter((team) =>
    team.toLowerCase().includes(query.toLowerCase()),
  );

  showDropdown(inputId, filtered);
}

// Team input event handlers
function handleTeamInput(e) {
  const query = e.target.value.trim();
  const inputId = e.target.id;

  clearTimeout(teamSearchTimeout);

  if (query.length < 2) {
    teamSearchTimeout = setTimeout(() => {
      loadPopularTeams(inputId);
    }, 100);
    return;
  }

  teamSearchTimeout = setTimeout(() => {
    searchTeams(query, inputId);
  }, SEARCH_DELAY);
}

// team click event handler
function handleTeamClick(e) {
  const inputId = e.target.id;
  const query = e.target.value.trim();

  if (query.length < 2) {
    loadPopularTeams(inputId);
  } else {
    searchTeams(query, inputId);
  }
}

// team focus event handler
function handleTeamFocus(e) {
  const inputId = e.target.id;
  const query = e.target.value.trim();

  if (query.length < 2) {
    loadPopularTeams(inputId);
  } else {
    searchTeams(query, inputId);
  }
}

// Handle Enter key for teams
function handleTeamEnterKey(e) {
  if (e.key === "Enter") {
    e.preventDefault();
    const dropdownId =
      e.target.id === "teamA" ? "dropdownTeamA" : "dropdownTeamB";
    hideDropdown(dropdownId);
    compareTeams();
  } else if (e.key === "Escape") {
    const dropdownId =
      e.target.id === "teamA" ? "dropdownTeamA" : "dropdownTeamB";
    hideDropdown(dropdownId);
  }
}

// Setup team autofill functionality
function setupTeamAutofill() {
  const teamAInput = document.getElementById("teamA");
  const teamBInput = document.getElementById("teamB");

  if (teamAInput) {
    teamAInput.addEventListener("input", handleTeamInput);
    teamAInput.addEventListener("click", handleTeamClick);
    teamAInput.addEventListener("focus", handleTeamFocus);
    teamAInput.addEventListener("keydown", handleTeamEnterKey);
  }

  if (teamBInput) {
    teamBInput.addEventListener("input", handleTeamInput);
    teamBInput.addEventListener("click", handleTeamClick);
    teamBInput.addEventListener("focus", handleTeamFocus);
    teamBInput.addEventListener("keydown", handleTeamEnterKey);
  }
}

// Initialize everything when DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  // Set default values
  document.getElementById("playerA").value = "Mike Trout";
  document.getElementById("playerB").value = "Aaron Judge";
  document.getElementById("teamA").value = "Cubs";
  document.getElementById("teamB").value = "Dodgers";

  // Initialize custom dropdown functionality
  setupPlayerAutofill();
  setupTeamAutofill();
  // Run initial comparison
  comparePlayers();
  compareTeams();
});
