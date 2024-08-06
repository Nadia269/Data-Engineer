# Data-Engineer

I downloaded the police_incident_reports dataset from the web and performed various data operations using PostgreSQL:

1. **Database Setup**: Created the `sfpolice` database and `police_incident_reports` table.
2. **Data Loading**: Imported the CSV data into the table.
3. **Basic Queries**: Retrieved initial dataset rows, identified the earliest and latest incident years, and counted distinct police districts.
4. **Aggregations**: Counted incidents per neighborhood and categorized them as 'high' or 'low' based on average incidents.
5. **Window Functions**: Applied `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()` to analyze incidents by category and district.
6. **Additional Analysis**: Created the `average_no_of_incidents()` function for calculating average incidents.

This project involves setting up a PostgreSQL database, importing data, and performing SQL operations to analyze police incident reports.

data link :https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-Historical-2003/tmnf-yvry/data_preview
