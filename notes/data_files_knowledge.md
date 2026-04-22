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

### Global_findex_database_2025.pdf
This is the full report. It is 342 pages long. It contains all the conclusions from the survey. It is broken down in several sections, each covering global topics like Financial access, Financial use, Financial health ...

This document can be read by researchers or any individual wanting to deepen in a sepcific topic. The Executive report will not go deep enough on certain topics. But this PDF document will.
