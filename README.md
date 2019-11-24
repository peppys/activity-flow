# https://activity.peppysisay.com
Activity feed - Built with Airflow DAGs, GitHub/Strava APIs, and Google Cloud Storage.

## DAGs:
`github_commits_loader` - Scheduled to run daily. Pulls all of my recent GitHub commits via their [Events API](https://developer.github.com/v3/activity/events/) and dumps them to a Google Cloud Storage bucket.
This will then trigger the `public_activity_compiler` DAG.

`strava_activity_loader` - Scheduled to run daily. Pulls all of my recent Strava activity (mainly bike rides) and dumps them to a Google Cloud Storage. This will then trigger the `public_activity_compiler` DAG.

`public_activity_compiler` - Triggered by the previous two DAGs. Pulls the dumped GitHub commits & Strava activty from Google Cloud Storage, transforms the data to a normalized structure, and dumps the last four events to a public Google Cloud Storage `index.html` file.

## Hosting
Since my Google Cloud Storage bucket is configured to behave as a host for a static website, https://activity.peppysisay.com will serve the generated activity file.
