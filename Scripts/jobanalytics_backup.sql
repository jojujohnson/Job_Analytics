\c jobanalyticsdb
\conninfo
COPY (SELECT * FROM web_extractor_staging) TO '/home/ubuntu/JobAnalytics/Backup/web_extractor_staging.csv';
\q