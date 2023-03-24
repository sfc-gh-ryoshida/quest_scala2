drop table MAA300;
CREATE TABLE MAA300
(
  "DT_D2KMKDTTM"                   DATE             ,
  "ID_D2KMKUSR"                    VARCHAR2(15)     ,
  "DT_D2KUPDDTTM"                  DATE             ,
  "ID_D2KUPDUSR"                   VARCHAR2(15)     ,
  "NM_D2KUPDTMS"                   NUMBER(3)        ,
  "FG_D2KDELFLG"                   VARCHAR2(1)      ,
  "VC_DISPOYMD"                    VARCHAR2(8)      ,
  "DV_DISPODIV"                    VARCHAR2(2)          NULL,
  "DV_DISCRDIV"                    VARCHAR2(2)          NULL,
  "CD_CHNLCD"                      VARCHAR2(3)          NULL,
  "DV_OUTOBJDIV"                   VARCHAR2(1)          NULL,
  "DV_TRICALCOBJDIV"               VARCHAR2(1)          NULL,
  "DV_DIV3"                        VARCHAR2(1)          NULL,
  "DV_DIV4"                        VARCHAR2(1)          NULL,
  "DV_DIV5"                        VARCHAR2(1)          NULL
);

insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('01','01','600','1','0');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('01','02','641','0','1');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('02','01','600','2','0');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('02','02','641','0','2');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('03','01','600','3','0');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('03','02','641','0','3');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('04','01','600','4','0');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('04','02','641','0','4');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('05','01','600','5','0');
insert into MAA300(DV_DISPODIV,DV_DISCRDIV,CD_CHNLCD,DV_OUTOBJDIV,DV_TRICALCOBJDIV) values('05','02','641','0','5');

commit;
