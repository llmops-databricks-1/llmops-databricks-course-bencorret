# Knowledge on the data source of this project
Different files were extracted from a World Bank's website. They all concern the most important survey carried by the World Bank: the **Global Findex Database**. The answers to the survey are recorded in a database, called the **microdata database**. And a report is produced to show the conclusions of the survey, called **Global Findex Report**. This is the 2025 edition. Link to the report's main web page: https://www.worldbank.org/en/publication/globalfindex/report

Here is a brief introduction to the goal of this report:
___
*Financial inclusion is a cornerstone of development, and since 2011, the Global Findex Database has been the definitive source of data on the ways in which adults around the world use financial services, from payments to savings and borrowing, and manage financial events such as a major expense or a loss of income. Results from the first survey were published in 2011 and have been followed by subsequent survey results from 2014, 2017, and 2021.
The 2025 edition, based on nationally representative surveys of about 148,000 adults in 141 economies and conducted over the calendar year 2024, contains updated indicators on access to and use of formal and informal financial services to save, borrow, make payments, and manage financial risk, and the first globally comparable data on mobile phone ownership, internet use, and digital safety. The data also identify gaps in access to and use of digital and financial services by women and poor adults.*
___

## Available data sources

### findex_microdata_2025.csv
This file is refered to as the microdata database. It is the raw data. It is in fact a list of the answers of all the respondants of the global findex survey. Each row represents the metadata and the answers from the respondant. And each header is either metadat about the respondant (country, sex ...) or answers to the questions. It contains 144090 records.

### documentation_microdata.pdf
This PDF document contains descriptions and context about all the columns present in the microdata database. Columns in the microdata database have very cryptic names like "con31e". These header names are questions or metadata about the survey respondants. All of these headers are referenced in this PDF document.

Example: the "con31e" field
This field is detailed 2 times in this PDF document.
- A first time in a table, with a label explanation + the question the respondant had to answer
- A second time in a dedicated section about that precise header. Which explains the question and the possible answers

This PDF document must be used to interpret the data present in the microdata database

### Global_findex_-_The_little_data_book.pdf
This file is a PDF document. It contains a list of tables with a few KPIs on financial inclusion, presented over income categories, region and finally all countries.

This PDF is based on the microdata database. It aggregates data from the microdata database to present KPIs at income category levels, or country level ... The information present in this document is a summary for survey readers who want to make quick comparisons between region, countries, country within a region etc.

### Global_findex_2025_-_Executive_report.pdf
This PDF document contains a detailed summary of the findings of the Global Findex Survey. The data present in the microdata database was used to produce conclusion on financial inclusion topics. Among these topics are:
- Adults owning a mobile phone
- Gender gaps in account ownership
- Savings per adult
- Digital merchant payments
This document is only 56 pages long, which makes it a lot easier to read for the great public.

### Global_findex_database_2025.pdf
This is the full report. It is 342 pages long. It contains all the conclusions from the survey. It is broken down in several sections, each covering global topics like Financial access, Financial use, Financial health ...

This document can be read by researchers or any individual wanting to deepen in a sepcific topic. The Executive report will not go deep enough on certain topics. But this PDF document will.

### global_findex_database_2025.csv
This csv file also contains data from the microdata database, but only 8564 records. And these records seem to be enriched with socio economical headers. This is probaby a refined and curated version of the microdata database.
