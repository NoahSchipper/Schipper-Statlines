# [Schipper Statlines](https://noahschipper.net/baseball)

An interactive baseball statistics application with player comparisons, team analysis, historical data, and advanced metrics.

## Features

- Player career and season statistics, including awards and advanced metrics
- Team season and franchise statistics
- Historical team head-to-head records
- Player search and disambiguation

## Project Structure

| File | Purpose |
| --- | --- |
| `Baseball.html` | Frontend page |
| `Baseball.css` | Frontend styles |
| `Baseball.js` | Frontend behavior and API requests |
| `app.py` | Flask application setup, CORS, and app-level routes |
| `players.py` | Player API routes and statistics logic |
| `teams.py` | Team API routes, lookup data, and statistics logic |
| `database.py` | Loads local environment settings and creates the SQLAlchemy connection |
| `requirements.txt` | Python dependencies |
| `Procfile` | Render web process command |
| `render.yaml` | Render Python runtime configuration |

## API Routes

The frontend JavaScript calls the Flask API. It uses `http://127.0.0.1:5000` when the page is opened from localhost, and `https://schipperstatlines.onrender.com` otherwise.

| Route | Purpose |
| --- | --- |
| `GET /health` | Health check |
| `GET /player-two-way` | Player statistics with two-way-player selection |
| `GET /player-disambiguate` | Player statistics with name disambiguation |
| `GET /search-players` | Search for players |
| `GET /popular-players` | List popular players |
| `GET /team` | Team season or franchise statistics |
| `GET /team/h2h` | Team head-to-head record |

## Local Development

Python 3.11 is used for deployment. From the project directory, create and activate a virtual environment in PowerShell, then install the dependencies:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Set `DATABASE_URL` in your local environment before starting the Flask application.

Start the Flask application locally:

```powershell
python app.py
```

The local API listens on port `5000`. The frontend's local API URL expects the page to be served from `localhost` or `127.0.0.1`; CORS allows the configured development ports `5500` and `5501`.

## Deployment

Render runs the Flask web service with `gunicorn app:app` from the `Procfile`. The app uses `DATABASE_URL` to connect from Render to the PostgreSQL database hosted by Supabase. Set `DATABASE_URL` in the Render service's environment settings; keep its value out of the repository.

The SQLAlchemy connection explicitly uses the `psycopg2` driver provided by `psycopg2-binary`. Render installs it and the other dependencies from `requirements.txt`. The configured Python version is 3.11.9. When the Render service is connected to the repository, pushing to its configured branch triggers a deployment.

## Data Sources

This project uses publicly available baseball datasets, including:

- Lahman Baseball Database, with historical statistics dating back to 1871
- Retrosheet play-by-play data
- Additional datasets for advanced metrics and analytics

Dataset rights and licenses belong to their respective authors and organizations.

## License

This project is distributed under the MIT License. Third-party datasets are subject to their own licenses and terms.