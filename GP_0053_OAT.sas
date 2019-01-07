options merror mlogic mprint nosymbolgen minoperator;
libname gout "Y:\GP_0053_oat_inj\output";
libname COMB "Z:\COHORTS\DATA\AAV\COM_MQ";
libname master "Z:\COHORTS\DATA\AAV_DerivedVar\Master_files\MASTER_DATA";
libname track "Z:\COHORTS\DATA\TRACKING";
libname Access_m "Z:\COHORTS\DATA\AAV\ACC_MQ";
libname Vidus_m "Z:\COHORTS\DATA\AAV\VD2_MQ";

%let ods_file_DD ="Y:\GP_0053_oat_inj\output\GP_0053_Vars_desc.doc";
%Let path=Y:\GP_0053_oat_inj\output\;

data av_master0;
set master.master_mq_22Aug18
 	master.master_mq_from15_22Aug18
	master.master_mq_from19_22Aug18
	master.master_mq_from22_15oct18;

 if survey<=22 and '01Jun2006'd<=int_date<'01Jun2017'd;
 if jail_l6m=1;
 keep code access_code vidus2_code survey int_date jail_l6m;
run;

data acc_master;
 set av_master0;
 if access_code ne '';
 drop vidus2_code;
run;

data vd2_master;
 set av_master0;
 if vidus2_code ne '';
 drop access_code;
run;

proc sql noprint;
select distinct "'"||strip(access_code)||"'" into :keep_acc separated by "," from acc_master where access_code ne '';
select distinct "'"||strip(vidus2_code)||"'" into :keep_vd2 separated by "," from vd2_master where vidus2_code ne '';
quit;


%macro mqf_bl(i,k) / mindelimiter=',';
data mqf_bl&i&k;
%if &i=0 %then
%do;
	%if &k=1 %then
	%do;
		set comb.aav_bl;
	%end;
	%else %if &k>=1 %then
	%do;
		set comb.aav_bl&k;
	%end;
%end;
%else
%do;
	set comb.aav_f&k;
%end;

survey=&i;
if access_code ne '' or vidus2_code ne '';

int_date_mq=datepart(int_date);
format int_date_mq date9.;

unsafe_inj_l6m=.;
Non_Engagement_OAT_l6m=.;

%if &i>=1 and &i<=13 or (&i=0 and &k in 1,18) %then
%do;
	if N14_FIX_USED=4 then unsafe_inj_l6m=1;
	else if N14_FIX_USED in(2,5) then unsafe_inj_l6m=0;

	if N21_METH in(9,10) then Non_Engagement_OAT_l6m=1; 
	else if N21_METH in(4,5,6) then Non_Engagement_OAT_l6m=0;
	else if N21_METH in(2,7,8,11) then Non_Engagement_OAT_l6m=77;
%end;
%if &i>=14 and &i<=18 %then
%do;
	if N14_FIX_USED=4 then unsafe_inj_l6m=1;
	else if N14_FIX_USED in(2,5) then unsafe_inj_l6m=0;

	if N21_METH in(9,10) then Non_Engagement_OAT_l6m=1;
	else if N21_METH in(4,5,6) or N21_METH_OTH_TXT in ("SUBOXONE") then Non_Engagement_OAT_l6m=0;
	else if N21_METH in(2,7,8,11) and N21_METH_OTH_TXT not in ("SUBOXONE") then Non_Engagement_OAT_l6m=77;
%end;
%if &i in 19,20,21,22 or (&i=0 and &k in 19,20,21,22) %then
%do;
	if FIX_USED_RIG=4 then unsafe_inj_l6m=1;
	else if FIX_USED_RIG in(2,5) then unsafe_inj_l6m=0;

	if JAIL_METH in (9,10) then Non_Engagement_OAT_l6m=1;
	else if JAIL_METH in(4,5,6) then Non_Engagement_OAT_l6m=0;
	else if JAIL_METH in(2,7,8,11) then Non_Engagement_OAT_l6m=77; 
%end;
keep access_code vidus2_code survey int_date_mq unsafe_inj_l6m Non_Engagement_OAT_l6m;
run;
%mend mqf_bl;

%mqf_bl(0,1);
%mqf_bl(0,18);
%mqf_bl(0,19);
%mqf_bl(0,20);
%mqf_bl(0,21);
%mqf_bl(0,22);

%macro loop_fups;
%do i=1 %to 22;
 %mqf_bl(&i,&i); 	
%end;
%mend loop_fups;

%loop_fups;

data all_ds;
 set mqf_bl:;
run;

data acc_ds;
 set all_ds;
 if access_code ne '' and access_code in(&keep_acc);
 drop vidus2_code;
run;

data vd2_ds;
 set all_ds;
 if vidus2_code ne '' and vidus2_code in(&keep_vd2);
 drop access_code;
run;

proc sql;
create table acc_vd2
as
select a.*,b.unsafe_inj_l6m, b.Non_Engagement_OAT_l6m
from acc_master a
left join acc_ds b
on a.access_code=b.access_code
and a.survey=b.survey
outer union corr
select a.*,b.unsafe_inj_l6m, b.Non_Engagement_OAT_l6m
from vd2_master a
left join vd2_ds b
on a.vidus2_code=b.vidus2_code
and a.survey=b.survey;
quit;

proc sort data=acc_vd2; by code survey; run;

proc sql;
create table gout.final_GP_0053
as
select distinct code, 
access_code,
vidus2_code,
int_date label="int_date: Interview Date",
survey label="survey: survey till 22",
jail_l6m label="jail_l6m: Incarcerated in L6M (only Yes(1) if >=1 Events in L6M)",
unsafe_inj_l6m label="unsafe_inj_l6m: Fixing with a rig that had already been used by someone else while in any correctional facility in L6M- Yes(1) vs No(0)",
Non_Engagement_OAT_l6m label="Non_Engagement_OAT_l6m: Not receiving OAT in any correctional facility in L6M - 1: (No / No, released before receiving OAT) vs 0: (Yes, continued MMT or suboxone, started in detention, court ordered) vs 77: (NA, Don't use opiates, Took it illicitly, Other)"

from acc_vd2;
quit;

ods output "Position" = data_dnry;
proc contents data= gout.final_GP_0053 position;
run;
ods output close;

data data_dnry;
set data_dnry;
keep Variable label;
run;

ods rtf file=&ods_file_DD;
title FONT='Times New Roman' JUSTIFY=CENTER color=BLACK "Data Dictionary for GP_0053 OAT";
proc print data=data_dnry noobs;
run;
ods rtf close;

/*create CSV files*/ 
%macro csv_out(dsn,opfile);
proc export data=gout.&dsn
outfile="'&path&opfile'"
dbms=csv
replace;
run;
%mend csv_out;

%csv_out(final_GP_0053,final_GP_0053.csv); 

