DROP TABLE DIRECTINSERT;
CREATE TABLE DIRECTINSERT(
  KEY1 VARCHAR(10),
  KEY2 DATE,
  KEY3 TIMESTAMP,
  KEY4 NUMBER,
  KEY5 NUMBER,
  KEY6 NUMBER(5,1),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY1)
) NOLOGGING;

DROP TABLE INSERTNOTEXIST;
CREATE TABLE INSERTNOTEXIST(
  KEY1 VARCHAR(10),
  KEY2 VARCHAR(10),
  KEY3 VARCHAR(10),
  KEY4 VARCHAR(10),
  KEY5 VARCHAR(10),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY1,KEY2,KEY3,KEY4,KEY5)
);


DROP TABLE INSERTNOTEXIST1;
CREATE TABLE INSERTNOTEXIST1(
  KEY1 VARCHAR(10),
  KEY2 VARCHAR(10),
  KEY3 VARCHAR(10),
  KEY4 VARCHAR(10),
  KEY5 VARCHAR(10),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY1,KEY2,KEY3,KEY4,KEY5)
);

DROP TABLE INSERTNOTEXIST2;
CREATE TABLE INSERTNOTEXIST2(
  KEY1 VARCHAR(10),
  KEY2 VARCHAR(10),
  KEY3 VARCHAR(10),
  KEY4 VARCHAR(10),
  KEY5 VARCHAR(10),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY5,KEY4,KEY3,KEY2,KEY1)
);

DROP TABLE INSERTNOTEXIST3;
CREATE TABLE INSERTNOTEXIST3(
  KEY1 VARCHAR(10),
  KEY2 DATE,
  KEY3 TIMESTAMP,
  KEY4 NUMBER,
  KEY5 NUMBER,
  KEY6 NUMBER(5,1),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY1,KEY2,KEY3,KEY4,KEY5,KEY6)
);

DROP TABLE INSERTNOTEXIST4;
CREATE TABLE INSERTNOTEXIST4(
  KEY1 VARCHAR(10),
  KEY2 DATE,
  KEY3 TIMESTAMP,
  KEY4 NUMBER,
  KEY5 NUMBER,
  KEY6 NUMBER(5,1),
  VALUE VARCHAR(10),
  PRIMARY KEY(KEY1)
);

DROP TABLE INSERTACC;
CREATE TABLE INSERTACC(
  KEY1 VARCHAR(10),
  KEY2 DATE,
  KEY3 TIMESTAMP,
  KEY4 NUMBER,
  KEY5 NUMBER,
  KEY6 NUMBER(5,1),
  VALUE VARCHAR(10)
);

DROP TABLE COL7;
CREATE TABLE COL7(
  KEY VARCHAR(10),
  V1 VARCHAR(10),
  V2 VARCHAR(10),
  V3 VARCHAR(10),
  V4 VARCHAR(10),
  V5 VARCHAR(10),
  V6 VARCHAR(10),
  PRIMARY KEY(KEY)
);

DROP TABLE DELETETEST;
CREATE TABLE DELETETEST(
  TEST VARCHAR(10),
  DDATE DATE,
  DTIMESTAMP TIMESTAMP,
  PRIMARY KEY(TEST)
);

DROP TABLE UPDATETEST;
CREATE TABLE UPDATETEST(
  KEY VARCHAR(10),
  V1 VARCHAR(10),
  V2 VARCHAR(10),
  PRIMARY KEY(KEY)
);

DROP TABLE UPDATETEST2;
CREATE TABLE UPDATETEST2(
  KEY VARCHAR(10),
  DDATE DATE,
  DTIMESTAMP TIMESTAMP,
  PRIMARY KEY(KEY,DDATE)
);

DROP TABLE UPSERTTEST1;
CREATE TABLE UPSERTTEST1(
  KEY VARCHAR(10),
  V1 VARCHAR(10),
  V2 VARCHAR(10),
  PRIMARY KEY(KEY,V1)
);

DROP TABLE UPSERTTEST2;
CREATE TABLE UPSERTTEST2(
  KEY VARCHAR(10),
  DDATE DATE,
  DTIMESTAMP TIMESTAMP,
  PRIMARY KEY(KEY)
);

DROP TABLE UPSERTTEST1;
CREATE TABLE UPSERTTEST1(
  KEY VARCHAR(10),
  V1 VARCHAR(10) DEFAULT ' ',
  V2 VARCHAR(10),
  PRIMARY KEY(KEY,V1)
);

DROP TABLE DATETYPE;
CREATE TABLE DATETYPE(
  KEY VARCHAR(10),
  DDATE DATE,
  PRIMARY KEY(KEY)
);

DROP TABLE DTIMESTAMP;
CREATE TABLE DTIMESTAMP(
  KEY VARCHAR(10),
  DTIMESTAMP TIMESTAMP,
  PRIMARY KEY(KEY)
);

DROP TABLE DECIMALTYPE;
CREATE TABLE DECIMALTYPE(
  KEY VARCHAR(10),
  DDECIMAL DECIMAL(10,0),
  PRIMARY KEY(KEY)
);

DROP TABLE INTEGERTYPE;
CREATE TABLE INTEGERTYPE(
  KEY VARCHAR(10),
  DINTEGER DECIMAL(10,0),
  PRIMARY KEY(KEY)
);

DROP TABLE LONGTYPE;
CREATE TABLE LONGTYPE(
  KEY VARCHAR(10),
  DLONG LOB,
  PRIMARY KEY(KEY)
);

DROP TABLE SP02;
CREATE TABLE SP02(
DT DATE,
NUM5 NUMBER(5),
NUM52 NUMBER(5,2),
TSTMP TIMESTAMP(6),
VC VARCHAR(255),
CH CHAR(5));

DELETE FROM SP02;
INSERT INTO SP02(DT,NUM5,NUM52,TSTMP,VC,CH) VALUES(
TO_DATE('2016/01/01','YYYY/MM/DD'),
1000,
10.3,
TO_TIMESTAMP('2016/01/01','YYYY/MM/DD'),
'AAA1',
'BBBB1'
);

INSERT INTO SP02(DT,NUM5,NUM52,TSTMP,VC,CH) VALUES(
TO_DATE('2016/01/02','YYYY/MM/DD'),
2000,
20.3,
TO_TIMESTAMP('2016/01/02','YYYY/MM/DD'),
'AAA2',
'BBBB2'
);

INSERT INTO SP02(DT,NUM5,NUM52,TSTMP,VC,CH) VALUES(
TO_DATE('2016/01/03','YYYY/MM/DD'),
3000,
30.3,
TO_TIMESTAMP('2016/01/03','YYYY/MM/DD'),
'AAA3',
'BBBB3'
);

INSERT INTO SP02(DT,NUM5,NUM52,TSTMP,VC,CH) VALUES(
NULL,
NULL,
NULL,
NULL,
NULL,
NULL
);


COMMIT;
