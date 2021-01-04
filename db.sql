CREATE DATABASE covid19
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

\c covid19

CREATE TABLE IF NOT EXISTS world (
    NewConfirmed INT NOT NULL,
    TotalConfirmed INT NOT NULL,
    NewDeaths INT NOT NULL,
    TotalDeaths INT NOT NULL,
    NewRecovered INT NOT NULL,
    TotalRecovered INT NOT NULL,
    Datemaj TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS countries (
    Country VARCHAR(200) NOT NULL,
    CountryCode VARCHAR(6) NOT NULL,
    Slug VARCHAR(200) NOT NULL,
    NewConfirmed INT NOT NULL,
    TotalConfirmed INT NOT NULL,
    NewDeaths INT NOT NULL,
    TotalDeaths INT NOT NULL,
    NewRecovered INT NOT NULL,
    TotalRecovered INT NOT NULL,
    Datemaj TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
