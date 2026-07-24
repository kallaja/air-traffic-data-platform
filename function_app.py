import azure.functions as func
import logging
from src.ingestion.ingest_opensky_states import main as ingest_main

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 10 * * *",  # daily at 10:00 UTC
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)


def ingest_opensky_timer(timer: func.TimerRequest) -> None:
    logging.info("OpenSky ingestion timer started. past_due=%s", timer.past_due)
    ingest_main()
    logging.info("OpenSky ingestion timer finished")
