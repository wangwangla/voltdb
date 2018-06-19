CREATE TABLE P1 (
  ID INTEGER NOT NULL,
  TINY TINYINT NOT NULL,
  SMALL SMALLINT,
  BIG BIGINT
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  TINY TINYINT NOT NULL,
  SMALL SMALLINT,
  BIG BIGINT,
  PRIMARY KEY (ID)
);

CREATE VIEW MATP1 (BIG, ID, NUM, IDCOUNT, TINYCOUNT, SMALLCOUNT, BIGCOUNT, TINYSUM, SMALLSUM) AS
  SELECT BIG, ID, COUNT(*), COUNT(ID), COUNT(TINY), COUNT(SMALL), COUNT(BIG), SUM(TINY), SUM(SMALL)
  FROM P1
  GROUP BY BIG, ID;

CREATE VIEW MATR1 (BIG, NUM, TINYSUM, SMALLSUM) AS
  SELECT BIG, COUNT(*), SUM(TINY), SUM(SMALL)
  FROM R1 WHERE ID > 5
  GROUP BY BIG;

--
CREATE TABLE P2 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P2 ON COLUMN ID;
--
CREATE VIEW V_P2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM P2
	GROUP BY wage, dept;


CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);

CREATE VIEW V_R2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2
	GROUP BY wage, dept;

CREATE VIEW V_R2_ABS (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT ABS(wage), dept, count(*), sum(age), sum(rent)  FROM R2
	GROUP BY ABS(wage), dept;

-- filters on view
CREATE VIEW V_R2_youth (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE age < 30
	GROUP BY wage, dept;

CREATE VIEW V_R2_senior (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE age > 55
	GROUP BY wage, dept;

CREATE VIEW V_R2_midage (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE age between 30 and 55
	GROUP BY wage, dept;

CREATE VIEW V_R2_financial (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE dept in (1,2,3,5,11,16,27,43,70,113)
	GROUP BY wage, dept;

CREATE VIEW V_R2_hr (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE dept in (2,4,6,10,16,26,42,68,110,178)
	GROUP BY wage, dept;

CREATE VIEW V_R2_youth_debt (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE age < 30 AND RENT > 18 AND WAGE < 60
	GROUP BY wage, dept;

CREATE VIEW V_R2_youth_rich (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
	SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2 WHERE age < 30 AND RENT > 18 AND WAGE > 120
	GROUP BY wage, dept;

--- This table is for testing three table joins, as mv partition table can only join with two more replicated tables.
CREATE TABLE R2V (
  V_G1 INTEGER NOT NULL,
  V_G2 SMALLINT,
  V_CNT SMALLINT,
  V_sum_age SMALLINT,
  V_sum_rent SMALLINT,
  PRIMARY KEY (V_G1)
);

