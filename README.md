# Teams-Bot
Checks the team queue in Zendesk and updates Teams channel if updates are detected.

This application requests the current status of the ticket queue of a predetermined team, and inputs/updates the records in sqlite database stored locally.

If any updates are detected in the Zendesk ticket queue, it sends notification to a Team's channel with the ticket information on when it was updated.

This script runs every 15 mins from 7am to 6pm from Monday to Friday, using Apache Airflow.
