CREATE TABLE MyPick(
    PublicID VARCHAR(200) NOT NULL,
    Station VARCHAR(10),
    TraceID VARCHAR(20),
    time_uts DATETIME(6),
    time_upd DATETIME(6),
    obj VARCHAR(200),
    PRIMARY KEY (PublicID)
    );

