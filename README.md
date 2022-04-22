# MigrateMassiveMess
Sometimes tables get too big for PostgreSQL...MOVE THE TO THE CLOUD!
This is used to move files from a database (SQL Server accesed though a REST API) to Google Big Query

This isnt a free process. You gotta pay google to host it, but it IS cheaper than enabling the GoogleAPI

My Massive table had just over 1 billion rows(22 columns). 
JOINS were impossible. 200,000 new rows are added daily. 

20+ Min query in SQL Server or PostgreSQL. 10 Seconds in BigQuery
