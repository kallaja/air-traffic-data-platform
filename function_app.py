import azure.functions as func
import logging

from src.ingestion.ingest_opensky_states import main as ingest_opensky_main
from src.ingestion.ingest_openmeteo_forecast import main as ingest_openmeteo_main
from src.ingestion.ingest_restcountries_countries import main as ingest_restcountries_main

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 10 * * *",  # daily 10:00 UTC
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def ingest_opensky_timer(timer: func.TimerRequest) -> None:
    logging.info("OpenSky ingestion timer started. past_due=%s", timer.past_due)
    ingest_opensky_main()
    logging.info("OpenSky ingestion timer finished")


@app.timer_trigger(
    schedule="0 15 10 * * *",  # daily 10:15 UTC
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def ingest_openmeteo_timer(timer: func.TimerRequest) -> None:
    logging.info("Open-Meteo ingestion timer started. past_due=%s", timer.past_due)
    ingest_openmeteo_main()
    logging.info("Open-Meteo ingestion timer finished")


@app.timer_trigger(
    schedule="0 30 10 * * 1",  # Mondays 10:30 UTC (reference dim)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def ingest_restcountries_timer(timer: func.TimerRequest) -> None:
    logging.info("REST Countries ingestion timer started. past_due=%s", timer.past_due)
    ingest_restcountries_main()
    logging.info("REST Countries ingestion timer finished")
