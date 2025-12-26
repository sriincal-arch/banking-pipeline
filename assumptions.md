# Project Assumptions

-- Balance: Imputed Null Values as 0.
-- HasLoan: Imputed Null Flag as False 
-- Data Export is a full snapshot while rest of ingestion is incremental
-- Assumed a 1:N relationship between customer and account
-- Assumed that the accounts file represents the customer account status as of year-end. For interest rate calculation purposes, assumed that each account was active throughout the full year.
-- Various other cleansing rules were applied based following the standard best practices



