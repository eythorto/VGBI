DROP TABLE Accidents;
CREATE TABLE Accidents (
    year_month VARCHAR(7) NOT NULL,
    accident_count INT NOT NULL,
    PRIMARY KEY (year_month)
);

DROP TABLE Tourists;
CREATE TABLE Tourists (
    year_month VARCHAR(7) NOT NULL,
    tourist_count INT NOT NULL,
    PRIMARY KEY (year_month)
);