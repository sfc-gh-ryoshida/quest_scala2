drop table insertNotExist;
create table insertNotExist(
  "DT_D2KMKDTTM"                   DATE             NOT NULL ENABLE,
  "ID_D2KMKUSR"                    VARCHAR2(15)     NOT NULL ENABLE,
  "DT_D2KUPDDTTM"                  DATE             NOT NULL ENABLE,
  "ID_D2KUPDUSR"                   VARCHAR2(15)     NOT NULL ENABLE,
  "NM_D2KUPDTMS"                   NUMBER(3)        NOT NULL ENABLE,
  "FG_D2KDELFLG"                   VARCHAR2(1)      NOT NULL ENABLE,
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);

drop table insertNotExistNotSetColumns;
create table insertNotExistNotSetColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  TEST2 VARCHAR(64),
  primary key(KEY)
);

drop table insertNotSetCommonColumns;
create table insertNotSetCommonColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);

drop table updateNotSetCommonColumns;
create table updateNotSetCommonColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);

drop table upsertNotSetCommonColumns;
create table upsertNotSetCommonColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);

drop table deleteLNotSetCommonColumns;
create table deleteLNotSetCommonColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);

drop table delPNotSetCommonColumns;
create table delPNotSetCommonColumns(
  KEY VARCHAR(64),
  TEST VARCHAR(64),
  primary key(KEY)
);
