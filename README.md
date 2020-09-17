# DAX_web_scraping

- The financial data for constituents under DAX is being collected from a german website https://www.boerse-frankfurt.de/?lang=en. 
- The data collected is being presented as charts using Chart.Js and flask.

- The constituents under DAX index are collected with constituent name and a WKN - (a unique identifier).
- The financial data for each constituent is collected, cleaned and stored in a sqlite database.
- The data is then visualized using ChartJs and shown on server using Flask.
