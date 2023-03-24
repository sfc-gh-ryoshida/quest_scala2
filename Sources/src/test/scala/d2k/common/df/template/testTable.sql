drop table jdg_test;

create table jdg_test (
  "DT_D2KMKDTTM"                   DATE             NOT NULL ENABLE,
  "ID_D2KMKUSR"                    VARCHAR2(15)     NOT NULL ENABLE,
  "DT_D2KUPDDTTM"                  DATE             NOT NULL ENABLE,
  "ID_D2KUPDUSR"                   VARCHAR2(15)     NOT NULL ENABLE,
  "NM_D2KUPDTMS"                   NUMBER(3)        NOT NULL ENABLE,
  "FG_D2KDELFLG"                   VARCHAR2(1)      NOT NULL ENABLE,
  item1 varchar2(255),
  item2 varchar2(255)
);
