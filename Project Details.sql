--QA UC4 Chain : GL_DASHBOARD_DAILY_REFRESH_C


create table RECON_CONTROL
(
  rc_run_level         VARCHAR2(10) not null,
  rc_prev_ref_date     DATE,
  rc_curr_ref_date     DATE,
  rc_prev_bf_trig_date DATE,
  rc_curr_bf_trig_date DATE,
  rc_prev_recon_date   DATE,
  rc_curr_recon_date   DATE,
  timestamp            DATE
);

INSERT INTO RECON_CONTROL
VALUES ('GL',NULL,NULL,NULL,NULL,NULL,NULL,SYSDATE);

create index GCARS.INV_EXTENSION_WM_SEQ on GCARS.INVOICE_EXTENSION (inv_wm_sequence_no_730);

create sequence CLASSIC_DAILY_SUMM_SEQ
minvalue 1
maxvalue 999999
start with 1
increment by 1;

create sequence RECON_IMBALANCE_DET_SEQ
minvalue 1
maxvalue 999999
start with 1
increment by 1;

ALTER TABLE RECON_CONTROL
ADD CONSTRAINT RUN_LEVEL PRIMARY KEY (RC_RUN_LEVEL);

CREATE MATERIALIZED VIEW CLASSIC_DAILY_OI_MVIEW
REFRESH COMPLETE ON DEMAND
AS
SELECT IC_ACCT_GROUP_702 COI_ACCT_GROUP,
      'CLASSIC' COI_MODEL,
      IC_NO_702 COI_IC,
      INV_ID_720 COI_INV_ID,
      INV_CUST_NO_720 COI_CUST_NO,
      INV_FINDER_NO_720 COI_FINDER_NO,
      INV_NO_720 COI_INV_NO,
      INV_AMT_720/100 COI_INV_AMT,
      INV_AMT_PAID_720/100 COI_INV_AMT_PAID,
      (INV_AMT_720 - INV_AMT_PAID_720) / 100 COI_OUTST_AMT,
      INV_CURR3_720 COI_CURR3,
      INV_CURR_720 COI_CURR,
      INV_TRANS_AMT_720 COI_TRANS_AMT,
      INV_TRANS_AMT_PAID_720 COI_TRANS_AMT_PAID,
      INV_TRANS_AMT_720 - INV_TRANS_AMT_PAID_720 COI_OUTST_TRANS_AMT,
      INV_LOC_CURR3_720 COI_LOC_CURR3,
      INV_TRANF_FLAG_720 COI_TRANF_FLAG,
      INV_TRANF_CUST_NO_720 COI_TRANF_CUST,
      INV_TRANF_FINDER_NO_720 COI_TRANF_FINDER,
      INV_TYPE_720 COI_INV_TYPE,
      INV_INIT_TC_720 COI_INV_INIT_TC,
      INV_TC_720 COI_INV_TC,
      INV_CHEQUE_ID_720 COI_CHEQUE_ID,
      INV_INPUT_DT_720 COI_INV_INP_DATE,
      INV_SYSTEM_DT_720 COI_INV_SYS_DATE,
      INV_PAID_FLAG_720 COI_PAID_FLAG,
      INV_CLOSE_DATE_720 COI_CLOSE_DATE,
      INV_PEND_APPL_FLAG_720 COI_PEND_APPL_FLAG,
      INV_JE_FLAG_720 COI_JE_FLAG,
      INV_POT_PAY_720/100 COI_POT_PAY,
      INV_DISC_REC_720 COI_DISC_REC,
      INV_DRAFT_FLAG_720 COI_DRAFT,
      INV_MEMO_SHIPMENT_DATE_720 COI_MEMO_SHIPMENT,
      RC_CURR_REF_DATE COI_REFRESH_DATE
 FROM GCARS.INVOICE, GCARS.INVESTMENT_CODE, RECON_CONTROL
WHERE INV_IC_NO_720 = IC_NO_702
  AND (IC_ACCT_GROUP_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
                             WHERE  SKAT_ACCTG_SW in ('R','Y')
                             AND  ACTIVE ='A'))
  AND ((INV_PAID_FLAG_720 = 'O') OR (INV_PAID_FLAG_720 = 'E' AND INV_PEND_APPL_FLAG_720 = 'N'))
  AND RC_RUN_LEVEL = 'GL';

  
  CREATE MATERIALIZED VIEW CLASSIC_DAILY_UNI_CHQ_MVIEW
REFRESH COMPLETE ON DEMAND
AS
SELECT LOCK_BOX_ACCT_GROUP_766 CCH_ACCT_GROUP,
'CLASSIC' CCH_MODEL,
CPOL_CORP_IC_791 CCH_CORP_IC,
BATCH_IC_NO_716 CCH_BATCH_IC,
CHEQUE_ID_719 CCH_CHQ_ID,
CHEQUE_LOCK_BOX_719 CCH_CHQ_LB,
CHEQUE_BATCH_NO_719 CCH_CHQ_BATCH_NO,
BATCH_JE_DATE_716 CCH_BATCH_JE_DATE,
CHEQUE_NO_719 CCH_CHQ_NO,
CHEQUE_DATE_719 CCH_CHQ_DATE,
CHEQUE_UNIDENT_FLAG_719 CCH_UNI_FLAG,
CHEQUE_UNAPPL_SW_719 CCH_UNAPP_FLAG,
DECODE(SIGN(CHEQUE_BACKEDOUT_719),-1,'OFFSET',1,'ORIGINAL',NULL) CCH_CHQ_BKOUT,
CHEQUE_BACKEDOUT_719 CCH_CHQ_BACKEDOUT,
CHEQUE_BACKEDOUT_DATE_719 CCH_CHQ_BACKEDOUT_DATE,
CHEQUE_APPLIER_CODE_719 CCH_CHQ_APPL_USER,
CHEQUE_DEP_AMT_719/100 CCH_CHQ_DEP_AMT,
CHEQUE_DEP_CURR3_719 CCH_DEP_CURR3,
CHEQUE_TRANS_AMT_719 CCH_CHQ_TRANS_AMT,
CHEQUE_DEP_LOC_CURR3_719 CCH_DEP_LOC_CURR3,
CHEQUE_DEP_CURR_719 CCH_CHQ_CURR,
CPOL_ESGAP_791 CCH_ESGAP,
CPOL_OWN_RATES_791 CCH_OWN_RATES,
CPOL_RATE_TYPE_791 CCH_RATE_TYPE,
RC_CURR_REF_DATE CCH_REFRESH_DATE
FROM CHEQUE,CHEQUE_BATCH_HEADER,LOCK_BOX,CLIENT_POLICY,RECON_CONTROL
WHERE CHEQUE_LOCK_BOX_719 = BATCH_LOCK_BOX_716
AND CHEQUE_BATCH_NO_719 = BATCH_NO_716
AND CHEQUE_LOCK_BOX_719 = LOCK_BOX_NO_766
AND LOCK_BOX_SET_OF_BOOK_766 = CPOL_SET_OF_BOOK_791
AND LOCK_BOX_ACCT_GROUP_766 = CPOL_ACCT_GROUP_791
AND LOCK_BOX_ACCT_GROUP_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
                             WHERE  SKAT_ACCTG_SW in ('R','Y')
                             AND  ACTIVE ='A')
 AND CHEQUE_UNIDENT_FLAG_719 = 'Y'
 AND RC_RUN_LEVEL = 'GL';

 
 CREATE MATERIALIZED VIEW CLASSIC_BILLING_SUMMARY_MVIEW
REFRESH COMPLETE ON DEMAND
AS
SELECT IC_ACCT_GROUP_702 CBS_ACCT_GROUP,
       BATCH_IC_NO_731 CBS_BCO,
       BATCH_SCHE_DATE_731 CBS_SCHE_DATE,
       BATCH_SCHE_NO_731 CBS_SCHE_NO,
       BATCH_AMT_731/100 CBS_AMT,
       SUBSTR(BATCH_CURR_731,1,3) CBS_DEP_CURR3,
       BATCH_TRANS_AMT_731 CBS_TRANS_AMT,
       SUBSTR(BATCH_CURR_731,4,3) CBS_LOC_CURR3,
       BATCH_NO_INV_731 CBS_INV_COUNT,
       BATCH_JE_DATE_731 CBS_JE_DATE,
       BATCH_JE_ID_731 CBS_JE_ID,
       RC_CURR_REF_DATE CBS_REF_DATE
FROM batch_schedule_header,
     investment_code,
     RECON_CONTROL
WHERE ic_no_702 = BATCH_IC_NO_731
and IC_ACCT_GROUP_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and batch_je_date2_731 BETWEEN RC_PREV_REF_DATE AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_BILLING_DETAIL_MVIEW
refresh complete on demand
as
select
inv_bill_sche_ic_no_720 CBD_BCO,
ic_acct_group_702 CBD_ACCT_GROUP,
inv_ic_no_720 CBD_IC,
ic_postable_flag_702 CBD_POSTABLE_FLAG,
inv_bill_sche_date_720 CBD_SCHE_DATE,
inv_bill_sche_no_720 CBD_SCHE_NO,
batch_je_date2_731 CBD_BATCH_JE_DATE,
inv_input_dt2_720 CBD_INP_DATE,
inv_id_720 CBD_INV_ID,
inv_no_720 CBD_INV_NO,
inv_amt_920 CBD_INV_STG_AMT,
inv_amt_720/100 CBD_INV_AMT,
inv_curr3_720 CBD_DEP_CURR3,
inv_trans_amt_920 CBD_TRANS_STG_AMT,
inv_trans_amt_720 CBD_TRANS_AMT,
(inv_trans_amt_920-inv_trans_amt_720) CBD_797_AMT,
inv_loc_curr3_720 CBD_LOC_CURR3,
(inv_amt_720/(inv_trans_amt_720*100)) CBD_EXCH_RATE,
cpol_own_rates_791 CBD_OWN_RATE,
cpol_rate_type_791 CBD_RATE_TYPE,
batch_je_id_731 CBD_JE_ID,
RC_CURR_REF_DATE CBD_REF_DATE
from
webm_in_invoice,
invoice,
invoice_extension,
batch_schedule_header,
investment_code bco,
CLIENT_POLICY,
RECON_CONTROL
where inv_bill_sche_ic_no_720 = batch_ic_no_731
and inv_bill_sche_date_720 = batch_sche_date_731
and inv_bill_sche_no_720 = batch_sche_no_731
and inv_tranf_flag_720 = 'N'
and bco.ic_no_702 = BATCH_IC_NO_731
and inv_id_720 = inv_id_730
and inv_sequence_no_920 = inv_wm_sequence_no_730
AND CPOL_ACCT_GROUP_791 = bco.IC_ACCT_GROUP_702
and cpol_set_of_book_791 = bco.ic_set_of_book_702
and BATCH_JE_DATE_731 BETWEEN RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
and IC_ACCT_GROUP_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
AND RC_RUN_LEVEL = 'GL';


CREATE MATERIALIZED VIEW CLASSIC_CASH_BOOK_SUMM_MVIEW
REFRESH COMPLETE ON DEMAND
AS
SELECT lock_box_ACCT_GROUP_766 CCS_ACCT_GROUP,
       BATCH_IC_NO_716 CCS_IC,
       BATCH_NO_716 CCS_BATCH_NO,
       BATCH_LOCK_BOX_716 CCS_LOCK_BOX,
       BATCH_AMT_716/100 CCS_AMT,
       SUBSTR(BATCH_CURR_716,1,3) CCS_DEP_CURR3,
       BATCH_TRANS_AMT_716 CCS_TRANS_AMT,
       SUBSTR(BATCH_CURR_716,4,3) CCS_LOC_CURR3,
       BATCH_NO_CHEQ_716 CCS_CHQ_COUNT,
       BATCH_JE_DATE_716 CCS_JE_DATE,
       BATCH_JE_ID_716 CCS_JE_ID,
       RC_CURR_REF_DATE CCS_REF_DATE
FROM cheque_batch_header,
     lock_box,
     RECON_CONTROL
WHERE lock_box_no_766 = BATCH_lock_box_716
and lock_box_ACCT_GROUP_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and batch_je_date_716 BETWEEN RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_CASH_BOOK_DETAIL_MVIEW
refresh complete on demand
as
select lock_box_no_766 CSD_LOCK_BOX,
lock_box_acct_group_766 CSD_ACCT_GROUP,
batch_no_716 CSD_BATCH_NO,
batch_je_date_716 CSD_JE_DATE,
batch_ic_no_716 CSD_IC,
cpol_corp_ic_791 CSD_CORP_IC,
cheque_id_719 CSD_CHQ_ID,
cheque_no_719 CSD_CHQ_NO,
cheque_dep_amt_719/100 CSD_AMT,
cheque_dep_curr3_719 CSD_DEP_CURR3,
cheque_trans_amt_719 CSD_TRANS_AMT,
cheque_dep_loc_curr3_719 CSD_LOC_CURR3,
cheque_unident_flag_719 CSD_UNIDENT_FLAG,
cheque_unappl_sw_719 CSD_UNAPP_FLAG,
cpol_own_rates_791 CSD_OWN_RATE,
cpol_rate_type_791 CSD_RATE_TYPE,
batch_je_id_716 CSD_JE_ID,
RC_CURR_REF_DATE CSD_REF_DATE
from cheque,
cheque_batch_header,
lock_box,
client_policy,
recon_control
where cheque_batch_no_719 = batch_no_716
and cheque_lock_box_719 = batch_lock_box_716
and cheque_lock_box_719 = lock_box_no_766
and lock_box_acct_group_766 = cpol_acct_group_791
and lock_box_acct_group_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and batch_je_date_716 BETWEEN RC_PREV_REF_DATE AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_WRITEOFF_DETAIL_MVIEW
refresh complete on demand
as
select 'CJE' CWD_WO_TYPE,
cheq_id_726 CWD_ID,
je.ic_acct_group_702 CWD_JE_ACCT_GROUP,
cheq_ic_no_726 CWD_JE_IC,
lock_box_acct_group_766 CWD_ITEM_ACCT_GROUP,
lock_box_ic_no_766 CWD_ITEM_IC,
cheq_tc_726 CWD_TC,
cheq_amt_726/100 CWD_AMT,
cheque_dep_curr3_719 CWD_DEP_CURR3,
cheq_trans_amt_726 CWD_TRANS_AMT,
cheque_dep_loc_curr3_719 CWD_LOC_CURR3,
cheq_je_flag_726 CWD_JE_FLAG,
cheq_posted_726 CWD_POSTED_FLAG,
cheq_potential_apply_flag_726 CWD_POT_APP_FLAG,
cheq_je_date_726 CWD_JE_DATE,
cheq_je_id_726 CWD_JE_ID,
RC_CURR_REF_DATE CWD_REF_DATE
from cheque_journal_entry,cheque,lock_box,investment_code je,recon_control
where cheq_id_726 = cheque_id_719
and cheque_lock_box_719 = lock_box_no_766
and cheq_ic_no_726 = je.ic_no_702
and cheq_posted_726 in ('N','Y')
and cheq_je_flag_726 != 'C'
and cheq_tc_726 not in ('777','778')
and lock_box_acct_group_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and cheq_je_date_726 between RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL'
UNION ALL
select 'IJE' CWD_WO_TYPE,
inv_id_720 CWD_ID,
je.ic_acct_group_702 CWD_JE_ACCT_GROUP,
inv_ic_no_727 CWD_JE_IC,
inv_acct_group_720 CWD_ITEM_ACCT_GROUP,
inv_ic_no_720 CWD_ITEM_IC,
inv_tc_727 CWD_TC,
inv_amt_727/100 CWD_AMT,
inv_curr3_720 CWD_DEP_CURR,
inv_trans_amt_727 CWD_TRANS_AMT,
inv_loc_curr3_720 CWD_LOC_CURR,
inv_je_flag_727 CWD_JE_FLAG,
inv_posted_727 CWD_POSTED_FLAG,
inv_potential_apply_flag_727 CWD_POT_APP_FLAG,
inv_je_date_727 CWD_JE_DATE,
inv_je_id_727 CWD_JE_ID,
RC_CURR_REF_DATE CWD_REF_DATE
from invoice_journal_entry,invoice,investment_code je,recon_control
where inv_owner_cust_no_727 = inv_cust_no_720
and inv_owner_finder_no_727 = inv_finder_no_720
and inv_ic_no_727 = je.ic_no_702
and inv_posted_727 in ('N','Y')
and inv_je_flag_727 != 'C'
and inv_tc_727 not in ('777','778','771','772')
and ic_acct_group_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and inv_je_date_727 between RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_TRANSFER_DETAIL_MVIEW
refresh complete on demand
as
select 'CT' CTD_TRANF_TYPE,
substr(cheq_offset_pseu_acct_726, 1, 1) CTD_FROM_SOB,
substr(cheq_offset_pseu_acct_726, 21, 1) CTD_TO_SOB,
substr(cheq_offset_pseu_acct_726, 3, 9) CTD_FROM_ACCT_GROUP,
substr(cheq_offset_pseu_acct_726, 12, 9) CTD_TO_ACCT_GROUP,
cheq_id_726 CTD_ID,
je.ic_acct_group_702 CTD_JE_ACCT_GROUP,
cheq_ic_no_726 CTD_JE_IC,
LOCK_BOX_ACCT_GROUP_766 CTD_ITEM_ACCT_GROUP,
BATCH_IC_NO_716 CTD_ITEM_IC,
cheq_tc_726 CTD_TC,
cheq_amt_726/100 CTD_AMT,
cheque_dep_curr3_719 CTD_DEP_CURR3,
cheq_trans_amt_726 CTD_TRANS_AMT,
cheque_dep_loc_curr3_719 CTD_LOC_CURR3,
cheq_je_flag_726 CTD_JE_FLAG,
cheq_posted_726 CTD_POSTED_FLAG,
cheq_potential_apply_flag_726 CTD_POT_APP_FLAG,
cheq_je_date_726 CTD_JE_DATE,
cheq_je_id_726 CTD_JE_ID,
RC_CURR_REF_DATE CTD_REF_DATE
from cheque_journal_entry,cheque,cheque_batch_header,lock_box,investment_code je,recon_control
where cheq_id_726 = cheque_id_719
and cheq_ic_no_726 = je.ic_no_702
and cheque_lock_box_719 = lock_box_no_766
and cheque_lock_box_719 = batch_lock_box_716
and cheque_batch_no_719 = batch_no_716
and lock_box_acct_group_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and cheq_tc_726 IN ('777', '778', '771', '772')
and cheq_potential_apply_flag_726 = 'N'
and lock_box_acct_group_766 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and cheq_je_date_726 between RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL'
UNION ALL
select 'IJ' CTD_TRANF_TYPE,
substr(inv_offset_pseu_acct_727, 1, 1) CTD_FROM_SOB,
substr(inv_offset_pseu_acct_727, 21, 1) CTD_TO_SOB,
substr(inv_offset_pseu_acct_727, 3, 9) CTD_FROM_ACCT_GROUP,
substr(inv_offset_pseu_acct_727, 12, 9) CTD_TO_ACCT_GROUP,
inv_id_720 CTD_ID,
inv_acct_group_720 CTD_JE_ACCT_GROUP,
inv_ic_no_720 CTD_JE_IC,
je.ic_acct_group_702 CTD_ITEM_ACCT_GROUP,
inv_ic_no_727 CTD_ITEM_IC,
inv_tc_727 CTD_TC,
inv_amt_727/100 CTD_AMT,
inv_curr3_720 CTD_DEP_CURR,
inv_trans_amt_727 CTD_TRANS_AMT,
inv_loc_curr3_720 CTD_LOC_CURR,
inv_je_flag_727 CTD_JE_FLAG,
inv_posted_727 CTD_POSTED_FLAG,
inv_potential_apply_flag_727 CTD_POT_APP_FLAG,
inv_je_date_727 CTD_JE_DATE,
inv_je_id_727 CTD_JE_ID,
RC_CURR_REF_DATE CTD_REF_DATE
from invoice_journal_entry,invoice,investment_code je,recon_control
where inv_owner_cust_no_727 = inv_cust_no_720
and inv_owner_finder_no_727 = inv_finder_no_720
and inv_ic_no_727 = je.ic_no_702
and inv_tc_727 in ('777', '778', '771', '772')
and inv_potential_apply_flag_727 = 'N'
and ic_acct_group_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
and inv_je_date_727 between RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
AND RC_RUN_LEVEL = 'GL'
UNION ALL
select 'BSJE',
substr(SCHED_OFFSET_PSEU_ACCT_740,1,1) CTD_FROM_SOB,
substr(SCHED_OFFSET_PSEU_ACCT_740,21,1) CTD_TO_SOB,
substr(SCHED_OFFSET_PSEU_ACCT_740,3,9) CTD_FROM_ACCT_GROUP,
substr(SCHED_OFFSET_PSEU_ACCT_740,12,9) CTD_TO_ACCT_GROUP,
sched_batch_no_740 CTD_ID,
je.ic_acct_group_702 CTD_JE_ACCT_GROUP,
sched_je_ic_no_740 CTD_JE_IC,
sc.ic_acct_group_702 CTD_ITEM_ACCT_GROUP,
batch_ic_no_731 CTD_ITEM_IC,
sched_tc_740 CTD_TC,
sched_amt_740/100 CTD_AMT,
substr(batch_curr_731,1,3) CTD_DEP_CURR,
sched_trans_amt_740 CTD_TRANS_AMT,
substr(batch_curr_731,4,3) CTD_LOC_CURR,
sched_je_flag_740 CTD_JE_FLAG,
sched_posted_740 CTD_POSTED_FLAG,
sched_potential_apply_flag_740 CTD_POT_APP_FLAG,
sched_je_date_740 CTD_JE_DATE,
sched_je_id_740 CTD_JE_ID,
RC_CURR_REF_DATE CTD_REF_DATE
from batch_schedule_journal_entry,investment_code sc,investment_code je,batch_schedule_header,recon_control
where sched_batch_no_740 = batch_sche_no_731
and sched_ic_no_740 = batch_ic_no_731
and sched_batch_date_740 = batch_sche_date_731
and batch_ic_no_731 = sc.ic_no_702
and sched_je_ic_no_740 = je.ic_no_702
and sched_tc_740 in ('771','772')
and sched_potential_apply_flag_740 = 'N'
and sched_je_date_740 between RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
and ((substr(SCHED_OFFSET_PSEU_ACCT_740,3,9) IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))) or
(substr(SCHED_OFFSET_PSEU_ACCT_740,12,9) IN (SELECT SKAT_GROUP FROM GL_SKAT
WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))))
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_CASH_APPL_MVIEW
refresh complete on demand
as
select cac.cash_cheque_id_728 CCA_CHEQ_ID ,
cac.cash_cust_no_728 CCA_CUST_NO,
cac.cash_finder_no_728 CCA_FINDER_NO,
inv_id_720 CCA_INV_ID,
cac.cash_connect_date_728 CCA_CONN_DATE,
cac.cash_cheq_amt_728/100 CCA_CHEQ_AMT,
cheque_dep_curr3_719 CCA_DEP_CURR3,
cac.cash_trans_amt_728 CCA_TRANS_AMT,
cheque_dep_loc_curr3_719 CCA_LOC_CURR3,
cac.cash_inv_amt_728/100 CCA_INV_AMT,
inv_curr3_720 CCA_INV_CURR3,
round((inv_trans_amt_720/inv_amt_720)*cac.cash_inv_amt_728,2) CCA_CASH_TRANS_AMT,
inv_loc_curr3_720 CCA_INV_LOC_CURR3,
cac.cash_var_je_flag_728 CCA_JE_FLAG,
cac.cash_inv_disc_amt_728/100 CCA_CASH_INV_DISC_AMT,
cac.cash_id_728 CCA_CASH_ID,
inv_type_720 CCA_INV_TYPE,
inv_init_tc_720 CCA_INIT_TC,
inv_del_flag_720 CCA_DEL_FLAG,
inv_tranf_flag_720 CCA_TRANF_FLAG,
cac.cash_appl_user_id_728 CCA_APPL_USE_ID,
pers_sso_id_796 CCA_APPL_SSO_ID,
pers_name_796 CCA_APPL_NAME,
cac.timestamp CCA_TIMESTAMP,
cac.cash_connect_date2_728 CCA_CONN_DATE2,
RC_CURR_REF_DATE CCA_REF_DATE
from
cash_apply_connector cac,
invoice,
cheque,
RECON_CONTROL,
personnel,
investment_code
where cac.cash_cust_no_728 = inv_cust_no_720
and cac.cash_finder_no_728 = inv_finder_no_720
and cac.cash_cheque_id_728 = cheque_id_719
and cac.cash_appl_user_id_728 = pers_code_796(+)
and cac.cash_connect_date_728 BETWEEN RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
and inv_ic_no_720=ic_no_702
and IC_ACCT_GROUP_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
                          WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
AND RC_RUN_LEVEL = 'GL';


create materialized view CLASSIC_ZERO_APPL_MVIEW
refresh complete on demand
as
select py.inv_cust_no_720 CZA_PY_CUST_NO,
py.inv_finder_no_720 CZA_PY_FINDER_NO,
pd.inv_cust_no_720 CZA_PD_CUST_NO,
pd.inv_finder_no_720 CZA_PD_FINDER_NO,
py.inv_id_720 CZA_PY_INV_ID,
pd.inv_id_720 CZA_PD_INV_ID,
zap_connection_dt_687 CZA_CONN_DATE,
zap_cr_pay_amt_687/100 CZA_CR_PAY_AMT,
py.inv_curr3_720 CZA_PY_INV_CURR3,
zap_trans_amt_687 CZA_TRANS_AMT,
py.inv_loc_curr3_720 CZA_PY_LOC_CURR,
zap_dr_pay_amt_687/100 CZA_DR_PAY_AMT,
pd.inv_curr3_720 CZA_PD_INV_CURR3,
round((pd.inv_trans_amt_720/pd.inv_amt_720)*zap_dr_pay_amt_687,2) CZA_DR_PAY_TRANS_AMT,
py.inv_type_720 CZA_PY_INV_TYPE,
py.inv_init_tc_720 CZA_PY_INIT_TC,
pd.inv_type_720 CZA_PD_INV_TYPE,
pd.inv_init_tc_720 CZA_PD_INIT_TC,
za.timestamp CZA_TIMESTAMP,
zap_connection_dt2_687 CZA_CONN_DATE2,
zap_id_687 CZA_ZAP_ID,
zap_appl_user_id_687 CZA_APPL_USER_ID,
pers_sso_id_796 CZA_APPL_SSO_ID,
pers_name_796 CZA_APPL_SSO_NAME,
RC_CURR_REF_DATE CZA_REF_DATE
from
zero_application za,
invoice py,
invoice pd,
RECON_CONTROL,
personnel,
investment_code
where zap_py_cust_no_687 = py.inv_cust_no_720
and zap_py_finder_no_687 = py.inv_finder_no_720
and zap_py_cust_no_687 = pd.inv_cust_no_720
and zap_py_finder_no_687 = pd.inv_finder_no_720
and zap_appl_user_id_687 = pers_code_796(+)
and zap_connection_dt_687 BETWEEN RC_PREV_REF_DATE  AND RC_CURR_REF_DATE
and py.inv_ic_no_720=ic_no_702
and IC_ACCT_GROUP_702 IN (SELECT SKAT_GROUP FROM GL_SKAT
                         WHERE ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('R','Y'))
AND RC_RUN_LEVEL = 'GL';


create table CLASSIC_DAILY_OI
(
  coi_acct_group      VARCHAR2(9),
  coi_model           VARCHAR2(7),
  coi_ic              VARCHAR2(9),
  coi_inv_id          NUMBER(9),
  coi_cust_no         VARCHAR2(10),
  coi_finder_no       NUMBER(7),
  coi_inv_no          VARCHAR2(50),
  coi_inv_amt         NUMBER(15),
  coi_inv_amt_paid    NUMBER(15),
  coi_outst_amt       NUMBER(15,2),
  coi_curr3           VARCHAR2(3),
  coi_curr            VARCHAR2(6),
  coi_trans_amt       NUMBER(15,2),
  coi_trans_amt_paid  NUMBER(15,2),
  coi_outst_trans_amt NUMBER(15,2),
  coi_loc_curr3       VARCHAR2(3),
  coi_tranf_flag      CHAR(1),
  coi_tranf_cust      VARCHAR2(10),
  coi_tranf_finder    NUMBER(7),
  coi_inv_type        CHAR(1),
  coi_inv_init_tc     NUMBER(3),
  coi_inv_tc          NUMBER(3),
  coi_cheque_id       NUMBER(9),
  coi_inv_inp_date    DATE,
  coi_inv_sys_date    DATE,
  coi_paid_flag       CHAR(1),
  coi_close_date      DATE,
  coi_pend_appl_flag  CHAR(1),
  coi_je_flag         CHAR(1),
  coi_pot_pay         NUMBER(15,2),
  coi_disc_rec        VARCHAR2(2),
  coi_draft           CHAR(1),
  coi_memo_shipment   DATE,
  coi_refresh_date    DATE
);

create table CLASSIC_DAILY_UNI_CHQ
(
  cch_acct_group         VARCHAR2(9),
  cch_model              VARCHAR2(7),
  cch_corp_ic            VARCHAR2(9),
  cch_batch_ic           VARCHAR2(9),
  cch_chq_id             NUMBER(9),
  cch_chq_lb             VARCHAR2(3),
  cch_chq_batch_no       VARCHAR2(6),
  cch_batch_je_date      DATE,
  cch_chq_no             VARCHAR2(12),
  cch_chq_date           DATE,
  cch_uni_flag           CHAR(1),
  cch_unapp_flag         CHAR(1),
  cch_chq_bkout          VARCHAR2(8),
  cch_chq_backedout      NUMBER(9),
  cch_chq_backedout_date DATE,
  cch_chq_appl_user      VARCHAR2(8),
  cch_chq_dep_amt        NUMBER(15,2),
  cch_dep_curr3          VARCHAR2(3),
  cch_chq_trans_amt      NUMBER(15,2),
  cch_dep_loc_curr3      VARCHAR2(3),
  cch_chq_curr           VARCHAR2(6),
  cch_esgap              CHAR(1),
  cch_own_rates          CHAR(1),
  cch_rate_type          VARCHAR2(30),
  cch_refresh_date       DATE
);

create table CLASSIC_DAILY_SUMMARY
(
  cds_acct_group              VARCHAR2(9),
  cds_start_date              DATE,
  cds_end_date                DATE,
  cds_prev_oi_trans_amt       NUMBER(15,2),
  cds_curr_oi_trans_amt       NUMBER(15,2),
  cds_prev_uni_cash_trans_amt NUMBER(15,2),
  cds_curr_uni_cash_trans_amt NUMBER(15,2),
  cds_billing_trans_amt       NUMBER(15,2),
  cds_cash_book_trans_amt     NUMBER(15,2),
  cds_writeoff_trans_amt      NUMBER(15,2),
  cds_transfer_trans_amt      NUMBER(15,2),
  cds_loc_curr3               VARCHAR2(3),
  cds_calc_curr_oi            NUMBER(15,2),
  cds_trans_imbalance         NUMBER(15,2),
  cds_imbalance_status        VARCHAR2(50),
  cds_recon_date              DATE,
  cds_record_id               NUMBER,
  timestamp                   DATE
)


create table RECON_IMBALANCE_DETAILS
(
  rid_id            NUMBER,
  rid_summ_id       NUMBER,
  rid_error_code    VARCHAR2(10),
  rid_logged_by     VARCHAR2(10),
  rid_created_date  DATE,
  rid_error_details VARCHAR2(1000),
  rid_status        VARCHAR2(1),
  rid_assign_to     VARCHAR2(10),
  rid_resolved_date DATE,
  timestamp         DATE
);

create table RECON_ERROR_MASTER
(
  rem_error_code        VARCHAR2(10),
  rem_error_type        VARCHAR2(50),
  rem_error_description VARCHAR2(500),
  timestamp             DATE
);

create table RECON_STATUS_MASTER
(
  rsm_status_code       VARCHAR2(1),
  rsm_status_desc       VARCHAR2(100),
  timestamp             DATE
);

create table recon_imbalance_tracker
(
RIT_ID        NUMBER(10),
RIT_SUMM_ID   NUMBER(10),
RIT_CIM_ID    NUMBER(10),
RIT_USER_NAME VARCHAR2(50),
RIT_USER_SSO  VARCHAR2(10),
RIT_COMMENTS  VARCHAR2(2000),
RIT_TIMESTAMP DATE
); 

insert into RECON_ERROR_MASTER
values ('BL1','Batch Level Imbalances','INVOICE BATCH AMOUNT AND INVOICE AMOUNT SUM FOR THE BATCH HAS MISMATCH',sysdate);
insert into RECON_ERROR_MASTER
values ('BL2','Batch Level Imbalances','IC LEVEL BATCH SCHEDULE AMOUNT AND INVOICE AMOUNT SUM FOR THE IC HAS MISMATCH',sysdate);
insert into RECON_ERROR_MASTER
values ('BL3','Batch Level Imbalances','CHEQUE BATCH AMOUNT AND CHEQUE AMOUNT SUM FOR THE BATCH HAS MISMATCH',sysdate);
insert into RECON_ERROR_MASTER
values ('BL4','Batch Level Imbalances','UNREALIZED EXCHANGE G/L BATCH AMOUNT HAS MISMATCH',sysdate);

insert into recon_status_master 
values('1','Open',sysdate);

CREATE OR REPLACE PACKAGE BODY PKG_GL_DASHBOARD_DAILY_REFRESH AS

PROCEDURE SPR_RECON_CONTROL_UPDATE(V_RECON_DATE IN VARCHAR2) IS
BEGIN

SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_RECON_CONTROL_UPDATE',
                                V_RECON_DATE,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Recon control table dates updation process started',
                               'Basic',
                               'DAILY');

UPDATE RECON_CONTROL SET RC_PREV_REF_DATE = RC_CURR_REF_DATE,
RC_CURR_REF_DATE = TO_DATE(V_RECON_DATE,'YYYYMMDDHH24MISS'),
RC_PREV_RECON_DATE = RC_CURR_RECON_DATE,
RC_CURR_RECON_DATE = (select trunc(process_run_start_date) from gcars_process_run_variables
where process_run_identifier = 'EOD_BATCH'),
TIMESTAMP = SYSDATE
WHERE RC_RUN_LEVEL = 'GL';

SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_RECON_CONTROL_UPDATE',
                                V_RECON_DATE,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Recon control table dates updation process completed',
                               'Basic',
                               'DAILY');
commit;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_RECON_CONTROL_UPDATE',
                                V_RECON_DATE,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Recon control table dates updation process failed. Error is -' || SQLERRM,
                               'Basic',
                               'DAILY');
    Raise;
END;

PROCEDURE SPR_DAILY_OI_REFRESH IS
BEGIN
SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_OI_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily open item insert process started',
                               'Basic',
                               'DAILY');

INSERT INTO CLASSIC_DAILY_OI
(coi_acct_group,
coi_model,
coi_ic,
coi_inv_id,
coi_cust_no,
coi_finder_no,
coi_inv_no,
coi_inv_amt,
coi_inv_amt_paid,
coi_outst_amt,
coi_curr3,
coi_curr,
coi_trans_amt,
coi_trans_amt_paid,
coi_outst_trans_amt,
coi_loc_curr3,
coi_tranf_flag,
coi_tranf_cust,
coi_tranf_finder,
coi_inv_type,
coi_inv_init_tc,
coi_inv_tc,
coi_cheque_id,
coi_inv_inp_date,
coi_inv_sys_date,
coi_paid_flag,
coi_close_date,
coi_pend_appl_flag,
coi_je_flag,
coi_pot_pay,
coi_disc_rec,
coi_draft,
coi_memo_shipment,
coi_refresh_date
)
SELECT
coi_acct_group,
coi_model,
coi_ic,
coi_inv_id,
coi_cust_no,
coi_finder_no,
coi_inv_no,
coi_inv_amt,
coi_inv_amt_paid,
coi_outst_amt,
coi_curr3,
coi_curr,
coi_trans_amt,
coi_trans_amt_paid,
coi_outst_trans_amt,
coi_loc_curr3,
coi_tranf_flag,
coi_tranf_cust,
coi_tranf_finder,
coi_inv_type,
coi_inv_init_tc,
coi_inv_tc,
coi_cheque_id,
coi_inv_inp_date,
coi_inv_sys_date,
coi_paid_flag,
coi_close_date,
coi_pend_appl_flag,
coi_je_flag,
coi_pot_pay,
coi_disc_rec,
coi_draft,
coi_memo_shipment,
coi_refresh_date
FROM CLASSIC_DAILY_OI_MVIEW;

SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_OI_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily open item insert process completed',
                               'Basic',
                               'DAILY');

SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_OI_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily Uni cash insert process started',
                               'Basic',
                               'DAILY');

INSERT INTO CLASSIC_DAILY_UNI_CHQ
(cch_acct_group,
cch_model,
cch_corp_ic,
cch_batch_ic,
cch_chq_id,
cch_chq_lb,
cch_chq_batch_no,
cch_batch_je_date,
cch_chq_no,
cch_chq_date,
cch_uni_flag,
cch_unapp_flag,
cch_chq_bkout,
cch_chq_backedout,
cch_chq_backedout_date,
cch_chq_appl_user,
cch_chq_dep_amt,
cch_dep_curr3,
cch_chq_trans_amt,
cch_dep_loc_curr3,
cch_chq_curr,
cch_esgap,
cch_own_rates,
cch_rate_type,
cch_refresh_date
)
SELECT
cch_acct_group,
cch_model,
cch_corp_ic,
cch_batch_ic,
cch_chq_id,
cch_chq_lb,
cch_chq_batch_no,
cch_batch_je_date,
cch_chq_no,
cch_chq_date,
cch_uni_flag,
cch_unapp_flag,
cch_chq_bkout,
cch_chq_backedout,
cch_chq_backedout_date,
cch_chq_appl_user,
cch_chq_dep_amt,
cch_dep_curr3,
cch_chq_trans_amt,
cch_dep_loc_curr3,
cch_chq_curr,
cch_esgap,
cch_own_rates,
cch_rate_type,
cch_refresh_date
FROM CLASSIC_DAILY_UNI_CHQ_MVIEW;

SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_OI_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily Uni cash insert process completed',
                               'Basic',
                               'DAILY');
commit;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_OI_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily open item insert process failed. Error is -' || SQLERRM,
                               'Basic',
                               'DAILY');
    Raise;
END;


PROCEDURE SPR_DAILY_SUMMARY_REFRESH IS
BEGIN
SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_SUMMARY_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily Summary insert process started',
                               'Basic',
                               'DAILY');


INSERT INTO CLASSIC_DAILY_SUMMARY
(
cds_acct_group,
cds_start_date,
cds_end_date,
CDS_CURR_OI_TRANS_AMT,
CDS_CURR_UNI_CASH_TRANS_AMT,
CDS_BILLING_TRANS_AMT,
CDS_CASH_BOOK_TRANS_AMT,
CDS_WRITEOFF_TRANS_AMT,
CDS_TRANSFER_TRANS_AMT,
CDS_CALC_CURR_OI,
CDS_TRANS_IMBALANCE,
CDS_LOC_CURR3,
CDS_RECORD_ID,
CDS_RECON_DATE,
timestamp
)
SELECT SKAT_GROUP,
RC_PREV_REF_DATE,
RC_CURR_REF_DATE,
0,
0,
0,
0,
0,
0,
0,
0,
(SELECT IC_LOC_CURR3_702 FROM INVESTMENT_CODE
WHERE IC_ACCT_GROUP_702 = SKAT_GROUP
AND NVL(IC_ACTIVE_FLAG_702,'A') = 'A'
AND ROWNUM < 2),
CLASSIC_DAILY_SUMM_SEQ.NEXTVAL, 
RC_CURR_RECON_DATE,
SYSDATE FROM GL_SKAT,RECON_CONTROL
WHERE RC_RUN_LEVEL = 'GL'
AND ACTIVE = 'A' AND SKAT_ACCTG_SW IN ('Y','R');

MERGE INTO CLASSIC_DAILY_SUMMARY CDS_C
USING (
SELECT CDS_CURR_OI_TRANS_AMT,CDS_CURR_UNI_CASH_TRANS_AMT,CDS_ACCT_GROUP
FROM CLASSIC_DAILY_SUMMARY,RECON_CONTROL
WHERE CDS_END_DATE = RC_PREV_REF_DATE
AND RC_RUN_LEVEL = 'GL') CDS_P
ON (CDS_P.CDS_ACCT_GROUP = CDS_C.cds_acct_group
AND CDS_C.cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
  UPDATE SET CDS_C.CDS_PREV_OI_TRANS_AMT = CDS_P.CDS_CURR_OI_TRANS_AMT,
             CDS_C.CDS_PREV_UNI_CASH_TRANS_AMT = CDS_P.CDS_CURR_UNI_CASH_TRANS_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT COI_ACCT_GROUP,SUM(COI_OUTST_TRANS_AMT) OI_AMT
FROM CLASSIC_DAILY_OI_MVIEW
GROUP BY COI_ACCT_GROUP
) CDOM ON (CDOM.COI_ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
  UPDATE SET CDS_CURR_OI_TRANS_AMT = CDOM.OI_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT CCH_ACCT_GROUP,SUM(CCH_CHQ_TRANS_AMT) UNI_AMT
FROM CLASSIC_DAILY_UNI_CHQ_MVIEW
GROUP BY CCH_ACCT_GROUP
) CDUC ON (CDUC.CCH_ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
  UPDATE SET CDS_CURR_UNI_CASH_TRANS_AMT = CDUC.UNI_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT CBS_ACCT_GROUP,CBS_LOC_CURR3,SUM(CBS_TRANS_AMT) BILLING_AMT
FROM CLASSIC_BILLING_SUMMARY_MVIEW
GROUP BY CBS_ACCT_GROUP,CBS_LOC_CURR3
) CBS ON (CBS.CBS_ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
  UPDATE SET CDS_BILLING_TRANS_AMT = CBS.BILLING_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT CCS_ACCT_GROUP,CCS_LOC_CURR3,SUM(CCS_TRANS_AMT) CASH_AMT
FROM CLASSIC_CASH_BOOK_SUMM_MVIEW
GROUP BY CCS_ACCT_GROUP,CCS_LOC_CURR3
) CCS ON (CCS.CCS_ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
 UPDATE SET CDS_CASH_BOOK_TRANS_AMT = CCS.CASH_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT CWD_JE_ACCT_GROUP,CWD_LOC_CURR3,SUM(CWD_TRANS_AMT) WO_AMT
FROM CLASSIC_WRITEOFF_DETAIL_MVIEW
GROUP BY CWD_JE_ACCT_GROUP,CWD_LOC_CURR3
) CWD ON (CWD.CWD_JE_ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
 UPDATE SET CDS_WRITEOFF_TRANS_AMT = CWD.WO_AMT;

MERGE INTO CLASSIC_DAILY_SUMMARY
USING (
SELECT CTD_FROM_ACCT_GROUP ACCT_GROUP,CTD_LOC_CURR3,SUM(CTD_TRANS_AMT) WO_AMT
FROM CLASSIC_TRANSFER_DETAIL_MVIEW
GROUP BY CTD_FROM_ACCT_GROUP,CTD_LOC_CURR3
) CTD ON (CTD.ACCT_GROUP = cds_acct_group
AND cds_end_date = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL'))
WHEN MATCHED THEN
 UPDATE SET CDS_TRANSFER_TRANS_AMT = CTD.WO_AMT;


UPDATE CLASSIC_DAILY_SUMMARY
SET CDS_TRANS_IMBALANCE = (CDS_PREV_OI_TRANS_AMT + CDS_PREV_UNI_CASH_TRANS_AMT)
                       - CDS_BILLING_TRANS_AMT - CDS_CASH_BOOK_TRANS_AMT
                       - CDS_WRITEOFF_TRANS_AMT - CDS_TRANSFER_TRANS_AMT
                       - (CDS_CURR_OI_TRANS_AMT + CDS_CURR_UNI_CASH_TRANS_AMT),
    CDS_CALC_CURR_OI = (CDS_PREV_OI_TRANS_AMT + CDS_PREV_UNI_CASH_TRANS_AMT)
                       - CDS_BILLING_TRANS_AMT - CDS_CASH_BOOK_TRANS_AMT
                       - CDS_WRITEOFF_TRANS_AMT - CDS_TRANSFER_TRANS_AMT
WHERE CDS_END_DATE = (SELECT RC_CURR_REF_DATE FROM RECON_CONTROL WHERE RC_RUN_LEVEL = 'GL');


SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_SUMMARY_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily Summary insert process completed',
                               'Basic',
                               'DAILY');



commit;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    SPR_GCARS_BATCHPROCESS_LOG('PKG_GL_DASHBOARD_DAILY_REFRESH',
                               'SPR_DAILY_SUMMARY_REFRESH',
                                NULL,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                               'Daily Summary insert process failed. Error is -' || SQLERRM,
                               'Basic',
                               'DAILY');
    Raise;
END;

END PKG_GL_DASHBOARD_DAILY_REFRESH;


CREATE OR REPLACE PROCEDURE SPR_GET_RECON_BILLING_REP(IN_ACCT_GROUP IN VARCHAR2,
                                                    START_DATE    IN VARCHAR2,
                                                    END_DATE      IN VARCHAR2,
                                                    out_message   OUT VARCHAR2,
                                                    AR910_REPORT  OUT SYS_REFCURSOR) AS

  REP_START_DATE VARCHAR2(20);
  REP_END_DATE   VARCHAR2(20);

  ERR_NUM     NUMBER;
  ERR_MESSAGE VARCHAR2(200);
  ERR_MSG     VARCHAR2(200);
  QUERY       VARCHAR2(10000);
  QUERY_START VARCHAR2(2000);
  QUERY_END   VARCHAR2(2000);

BEGIN
  ERR_MSG := '000';
  
  QUERY_START := 'SELECT TO_CHAR(CDS_START_DATE, ''MMDDYYYYHH24MISS'')
    FROM CLASSIC_DAILY_SUMMARY
   WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
     AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
   EXECUTE IMMEDIATE QUERY_START into REP_START_DATE;

  if END_DATE is null then
    QUERY_END := 'SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP  || '''
       AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  else
    QUERY_END := ' SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
       AND CDS_RECON_DATE = TO_DATE(''' || END_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  end if;

  QUERY := 'select ic_acct_group_702 "Account Group",
inv_bill_sche_ic_no_720 "Billing Component",
inv_ic_no_720 "Investment Code",
ic_legal_id_702 "Legal ID",
decode(inv_cost_center_720,null,ic_gl_code_702,inv_cost_center_720) "Cost Center",
inv_input_dt2_720 "Invoice Load Date",
inv_id_720 "Invoice ID",
inv_cust_no_720 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "Business Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Invoice Number",
inv_date_720 "Invoice Date",
inv_due_date_720 "Invoice Due Date",
(inv_amt_720/100) "Invoice Amount",
inv_curr3_720 "Deposit Currency",
inv_trans_amt_720 "Translated Amount",
inv_loc_curr3_720 "Local Currency",
inv_ar_type_720 "AR Type",
art.other_table_name_794 "AR Type Description",
inv_init_tc_720 "Invoice TC",
TC.other_table_name_794 "Invoice TC Description",
inv_term_720 "Term Code",
term_long_name_785 "Term Code Description",
Journal.gl_org_id "Billing Legal ID",
Journal.gl_cost_center "Billing Cost Center",
Journal.gl_acct "Billing GL Account",
Journal.gl_journal_id "Billing ID",
Journal.gl_description "Billing GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
batch_je_id_731 "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
FROM gcars.invoice,
gcars.batch_schedule_header,
       gcars.investment_code,
       gcars.customer,
                   gcars.other_table art,
                   terms_code,
                   gcars.other_table tc,
                   (select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''BH''
          ) journal,
          (select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''BH''
          ) offset
where inv_bill_sche_ic_no_720 = batch_ic_no_731
and inv_bill_sche_date_720 = batch_sche_date_731
and inv_bill_sche_no_720 = batch_sche_no_731
and batch_je_id_731 = Journal.cross_detail_id(+)
and batch_je_id_731 = offset.cross_detail_id(+)
and inv_tranf_flag_720 = ''N''
and ic_no_702 = BATCH_IC_NO_731
and art.other_table_type_794=''AR''
and art.other_table_key_794=inv_ar_type_720
and term_code_785=inv_term_720
and tc.other_table_type_794=''TC''
and tc.other_table_key_794=inv_init_tc_720
and cust_no_717=inv_cust_no_720
and BATCH_JE_DATE_731 BETWEEN TO_DATE(''' || REP_START_DATE || ''',''MMDDYYYYHH24MISS'')
                      AND TO_DATE(''' || REP_END_DATE || ''',''MMDDYYYYHH24MISS'')
and IC_ACCT_GROUP_702 in (''' || IN_ACCT_GROUP || ''')';

  if ERR_MSG = '000' then
      out_message := '000';
      OPEN AR910_REPORT FOR QUERY;
  end if;

EXCEPTION
  WHEN OTHERS THEN
    OPEN AR910_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;

    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_GET_RECON_BILLING_REP',
                               IN_ACCT_GROUP,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'GET NEW BILLING REPORT PROCESS FAILED ' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_GET_RECON_BILLING_REP;


CREATE OR REPLACE PROCEDURE SPR_GET_RECON_WRITEOFF_REP(IN_ACCT_GROUP   IN VARCHAR2,
                                                     START_DATE      IN VARCHAR2,
                                                     END_DATE        IN VARCHAR2,
                                                     out_message     OUT VARCHAR2,
                                                     WRITEOFF_REPORT OUT SYS_REFCURSOR) AS

  REP_START_DATE VARCHAR2(20);
  REP_END_DATE   VARCHAR2(20);

  ERR_NUM     NUMBER;
  ERR_MESSAGE VARCHAR2(200);
  ERR_MSG     VARCHAR2(200);
  QUERY       VARCHAR2(9000);
  QUERY_START VARCHAR2(2000);
  QUERY_END   VARCHAR2(2000);

BEGIN
--We are not considering 797 in the writeoffs as it doesn't hit control account.
  ERR_MSG := '000';
  
  QUERY_START := 'SELECT TO_CHAR(CDS_START_DATE, ''MMDDYYYYHH24MISS'')
    FROM CLASSIC_DAILY_SUMMARY
   WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
     AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
   EXECUTE IMMEDIATE QUERY_START into REP_START_DATE;

  if END_DATE is null then
    QUERY_END := 'SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP  || '''
       AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  else
    QUERY_END := ' SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
       AND CDS_RECON_DATE = TO_DATE(''' || END_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  end if;

  QUERY := 'select ''Cheque Write Off'' "W/O Type",
je.ic_acct_group_702 "Account Group",
cheq_ic_no_726 "Investment Code",
je.ic_legal_id_702 "Legal ID",
je.ic_gl_code_702 "Cost Center",
cheq_je_date_726 "Write Off Date",
cheq_gl_date_726 "GL Date",
cheq_tc_726 "Cheque/Invoice TC"
other_table_name_794 "Cheque/Invoice TC Desc", 
(cheq_amt_726/100) "Write Off Amount",
cheque_dep_curr3_719 "Write Off Currency",
cheq_trans_amt_726 "Write Off Trans Amount"
cheque_dep_loc_curr3_719 "Write Off Local Currency",
CHEQ_REMARK_726 "Remarks",
pers_SSO_ID_796 "W/O User SSO ID",
pers_name_796 "W/O User Name",
cheq_id_726 "Cheque/Invoice ID",
cheque_cust_no_719 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "ERP Customer Number",
cheque_no_719 "Cheque/Inv Number",
cheque_date_719 "Cheque/Inv Date",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Cheque/Inv Name"
(cheque_dep_amt_719/100) "Cheque/Inv Amount",
cheque_dep_curr3_719 "Dep/Bill Currency",
cheque_trans_amt_719 "Trans Amount",
cheque_dep_loc_curr3_719 "Local Currency",
lock_box_no_766 "Lock box",
lock_box_treasury_cd_766 "Treasury Code",
lock_box_bank_name_766 "Bank Name",
Journal.gl_org_id "W/O Legal ID",
Journal.gl_cost_center "W/O Cost Center",
Journal.gl_acct "W/O GL Account",
Journal.gl_journal_id "W/O ID",
Journal.gl_description "W/O GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
cheq_je_id_726 "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
from cheque_journal_entry,cheque,lock_box,investment_code je,
personnel,customer,other_table,
(select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''CJ''
          ) journal,
          (select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''CJ''
          ) offset
where cheq_id_726 = cheque_id_719
and cheque_lock_box_719 = lock_box_no_766
and cheq_ic_no_726 = je.ic_no_702
and pers_code_796 = cheq_user_id_726
and cheque_cust_no_719 = cust_no_717(+)
and cheq_je_id_726 = Journal.cross_detail_id(+)
and cheq_je_id_726 = offset.cross_detail_id(+)
and cheq_posted_726 in (''N'',''Y'')
and cheq_je_flag_726 != ''C''
and cheq_tc_726 not in (''777'',''778'')
and other_table_type_794=''TC''
and other_table_lang_794=''E''
and other_table_key_794=cheq_tc_726
and je.ic_acct_group_702 in (''' || IN_ACCT_GROUP || ''')
and cheq_je_date_726 between TO_DATE(''' || REP_START_DATE ||
           ''',''MMDDYYYYHH24MISS'') AND TO_DATE(''' || REP_END_DATE ||
           ''',''MMDDYYYYHH24MISS'')
UNION ALL
select ''Invoice Write Off'' "W/O Type",
je.ic_acct_group_702 "Account Group",
inv_ic_no_727 "Investment Code",
je.ic_legal_id_702 "Legal ID",
decode(inv_cost_center_720,null,je.ic_gl_code_702,inv_cost_center_720) "Cost Center",
inv_je_date_727 "Write Off Date",
inv_gl_date_727 "GL Date",
inv_tc_727 "Cheque/Invoice TC"
other_table_name_794 "Cheque/Invoice TC Desc",
(inv_amt_727/100) "Write Off Amount",
inv_curr3_720 "Write Off Currency",
inv_trans_amt_727 "Write Off Trans Amount"
inv_loc_curr3_720 "Write Off Local Currency",
INV_REMARK_727 "Remarks",
pers_SSO_ID_796 "W/O User SSO ID",
pers_name_796 "W/O User Name",
inv_id_720 "Cheque/Invoice ID",
inv_cust_no_720 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "ERP Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Cheque/Inv Number",
inv_date_720 "Cheque/Inv Date",
null "Cheque/Inv Name"
9inv_amt_720/100) "Cheque/Inv Amount",
inv_curr3_720 "Dep/Bill Currency",
inv_trans_amt_720 "Trans Amount",
inv_loc_curr3_720 "Local Currency",
ic_no_702 "Lock box",
null "Treasury Code",
ic_name_702 "Bank Name",
Journal.gl_org_id "W/O Legal ID",
Journal.gl_cost_center "W/O Cost Center",
Journal.gl_acct "W/O GL Account",
Journal.gl_journal_id "W/O ID",
Journal.gl_description "W/O GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
inv_je_id_727 "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
from invoice_journal_entry,invoice,investment_code je,customer,
personnel,other_table,
(select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''IJ''
          ) journal,
          (select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''IJ''
          ) offset
where inv_owner_cust_no_727 = inv_cust_no_720
and inv_owner_finder_no_727 = inv_finder_no_720
and inv_cust_no_720 = cust_no_717
and inv_ic_no_727 = je.ic_no_702
and pers_code_796 = inv_user_id_727
and inv_je_id_727 = Journal.cross_detail_id(+)
and inv_je_id_727 = offset.cross_detail_id(+)
and inv_posted_727 in (''N'',''Y'')
and inv_je_flag_727 != ''C''
and inv_tc_727 not in (''777'',''778'',''771'',''772'')
and other_table_type_794=''TC''
and other_table_lang_794=''E''
and other_table_key_794=inv_tc_727
and inv_je_date_727 between TO_DATE(''' || REP_START_DATE ||
           ''',''MMDDYYYYHH24MISS'') AND TO_DATE(''' || REP_END_DATE ||
           ''',''MMDDYYYYHH24MISS'')
and je.ic_acct_group_702 in (''' || IN_ACCT_GROUP || ''')';

  if ERR_MSG = '000' then
      out_message := '000';
      OPEN WRITEOFF_REPORT FOR QUERY;
  end if;

EXCEPTION
  WHEN OTHERS THEN
    OPEN WRITEOFF_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;

    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_GET_RECON_WRITEOFF_REP',
                               IN_ACCT_GROUP,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'GET NEW BILLING REPORT PROCESS FAILED ' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_GET_RECON_WRITEOFF_REP;


CREATE OR REPLACE PROCEDURE SPR_GET_RECON_TRANSFER_REP(IN_ACCT_GROUP   IN VARCHAR2,
                                                     START_DATE      IN VARCHAR2,
                                                     END_DATE        IN VARCHAR2,
                                                     out_message   OUT VARCHAR2,
                                                     TRANSFER_REPORT OUT SYS_REFCURSOR) AS

  REP_START_DATE VARCHAR2(20);
  REP_END_DATE   VARCHAR2(20);

  ERR_NUM     NUMBER;
  ERR_MESSAGE VARCHAR2(200);
  ERR_MSG     VARCHAR2(200);
  QUERY       VARCHAR2(30000);
  QUERY_START VARCHAR2(2000);
  QUERY_END   VARCHAR2(2000);

BEGIN
  ERR_MSG := '000';
  
  QUERY_START := 'SELECT TO_CHAR(CDS_START_DATE, ''MMDDYYYYHH24MISS'')
    FROM CLASSIC_DAILY_SUMMARY
   WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
     AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
   EXECUTE IMMEDIATE QUERY_START into REP_START_DATE;

  if END_DATE is null then
    QUERY_END := 'SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP  || '''
       AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  else
    QUERY_END := ' SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
       AND CDS_RECON_DATE = TO_DATE(''' || END_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  end if;

  QUERY := 'select ''Cash Transfer'' "Transfer Type",
substr(cheq_offset_pseu_acct_726, 3, 9) "From Account Group",
substr(cheq_offset_pseu_acct_726, 12, 9) "To Account Group",
substr(cheq_pseu_acct_726,3,9) "From IC",
substr(cheq_pseu_acct_726,12,9) "To IC",
je.ic_legal_ID_702 "Legal ID",
je.ic_gl_code_702 "Cost Center",
cheq_je_date_726 "Transfer Date",
cheq_gl_date_726 "GL Date",
cheq_tc_726 "Cheque/Invoice TC",
(cheq_amt_726/100) "Transfer Amount",
cheque_dep_curr3_719 "Transfer Currency",
cheq_trans_amt_726 "Transfer Trans Amt",
cheque_dep_loc_curr3_719 "Transfer Local Currency",
pers_SSO_ID_796 "Transferred by SSO",
pers_name_796 "Transferred by Name",
cheq_ID_726 "Cheque/Invoice ID",
cheque_cust_no_719 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                           ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "ERP Customer Number",
cheque_no_719 "Cheque/Inv Number",
cheque_date_719 "Cheque/Inv Date",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Cheque/Inv Name",
(cheque_dep_amt_719/100) "Deposit/Inv Amount",
cheque_dep_curr3_719 "Deposit/Inv Currency",
cheque_trans_amt_719 "Trans Amount",
cheque_dep_loc_curr3_719 "Local Currency",
Journal.gl_org_ID "DTDF Legal ID",
Journal.gl_cost_center "DTDF Cost Center",
Journal.gl_acct "DTDF GL Account",
Journal.gl_journal_ID "DTDF ID",
Journal.gl_description "DTDF GL Description",
offset.gl_org_ID "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_ID "Offset ID",
Offset.gl_description "Offset GL Description",
cheq_je_ID_726 "JE ID",
Journal.gl_summary_ID "Journal Summary ID",
Offset.gl_summary_ID "Offset Summary ID",
substr(cheq_offset_pseu_acct_726, 1, 1) "From SOB",
substr(cheq_offset_pseu_acct_726, 21, 1) "To SOB"
from cheque_journal_entry,cheque,cheque_batch_header,lock_box,investment_code je,customer,
personnel,(select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''CJ''
          ) journal,
          (select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''CJ''
          ) offset
where cheq_ID_726 = cheque_ID_719
and cheq_ic_no_726 = je.ic_no_702
and cheque_lock_box_719 = lock_box_no_766
and cheque_lock_box_719 = batch_lock_box_716
and cheque_batch_no_719 = batch_no_716
and cheq_user_id_726= pers_code_796(+)
and cheque_cust_no_719 = cust_no_717(+)
and cheq_je_ID_726 = Journal.cross_detail_ID(+)
and cheq_je_ID_726 = offset.cross_detail_ID(+)
and cheq_tc_726 IN (''777'', ''778'', ''771'', ''772'')
and cheq_potential_apply_flag_726 = ''N''
and cheq_posted_726 in (''N'',''Y'')
and trim(substr(cheq_offset_pseu_acct_726, 3, 9)) in (''' || IN_ACCT_GROUP || ''')
and cheq_je_date_726 between TO_DATE(''' || REP_START_DATE || ''',''MMDDYYYYHH24MISS'')
                     AND TO_DATE(''' || REP_END_DATE || ''',''MMDDYYYYHH24MISS'')
UNION ALL
select ''Invoice Transfer'' "Transfer Type",
substr(inv_offset_pseu_acct_727, 3, 9) "From Account Group",
substr(inv_offset_pseu_acct_727, 12, 9) "To Account Group",
substr(inv_pseu_acct_727, 3, 9) "From IC",
substr(inv_pseu_acct_727, 12, 9) "To IC",
je.ic_legal_ID_702 "Legal ID",
decode(inv_cost_center_720,null,je.ic_gl_code_702,inv_cost_center_720) "Cost Center",
inv_je_date_727 "Transfer Date",
inv_gl_date_727 "GL Date",
inv_tc_727 "Cheque/Invoice TC",
(inv_amt_727/100) "Transfer Amount",
inv_curr3_720 "Transfer Currency",
inv_trans_amt_727 "Transfer Trans Amt",
inv_loc_curr3_720 "Transfer Local Currency",
pers_SSO_ID_796 "Transferred by SSO",
pers_name_796 "Transferred by Name",
inv_ID_720 "Cheque/Invoice ID",
inv_cust_no_720 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "ERP Customer Number",
inv_no_720 "Cheque/Inv Number",
inv_date_720 "Cheque/Inv Date",
null "Cheque/Inv Name",
(inv_amt_720/100) "Deposit/Inv Amount",
inv_curr3_720 "Deposit/Inv Currency",
inv_trans_amt_720 "Trans Amount",
inv_loc_curr3_720 "Local Currency",
Journal.gl_org_ID "DTDF Legal ID",
Journal.gl_cost_center "DTDF Cost Center",
Journal.gl_acct "DTDF GL Account",
Journal.gl_journal_ID "DTDF ID",
Journal.gl_description "DTDF GL Description",
offset.gl_org_ID "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_ID "Offset ID",
Offset.gl_description "Offset GL Description",
inv_je_ID_727 "JE ID",
Journal.gl_summary_ID "Journal Summary ID",
Offset.gl_summary_ID "Offset Summary ID",
substr(inv_offset_pseu_acct_727, 1, 1) "From SOB",
substr(inv_offset_pseu_acct_727, 21, 1) "To SOB"
from invoice_journal_entry,invoice,investment_code je,customer,personnel,
(select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''IJ''
          ) journal,
          (select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''IJ''
          ) offset
where inv_owner_cust_no_727 = inv_cust_no_720
and inv_owner_finder_no_727 = inv_finder_no_720
and inv_ic_no_727 = je.ic_no_702
and inv_tc_727 in (''777'', ''778'', ''771'', ''772'')
and inv_potential_apply_flag_727 = ''N''
and inv_posted_727 in (''N'',''Y'')
and inv_cust_no_720 = cust_no_717
and inv_user_id_727 = pers_code_796(+)
and inv_je_ID_727 = Journal.cross_detail_ID(+)
and inv_je_ID_727 = offset.cross_detail_ID(+)
and trim(substr(inv_offset_pseu_acct_727, 3, 9)) in (''' || IN_ACCT_GROUP || ''')
and inv_je_date_727 between TO_DATE(''' || REP_START_DATE || ''',''MMDDYYYYHH24MISS'')
                    AND TO_DATE(''' || REP_END_DATE || ''',''MMDDYYYYHH24MISS'')
UNION ALL
select ''Batch Schedule'' "Transfer Type",
substr(sched_offset_pseu_acct_740,3,9) "From Account Group",
substr(sched_offset_pseu_acct_740,12,9) "To Account Group",
sched_je_ic_no_740 "From IC",
batch_ic_no_731 "To IC",
je.ic_legal_ID_702 "Legal ID",
je.ic_gl_code_702 "Cost Center",
sched_je_date_740 "Transfer Date",
sched_gl_date_740 "GL Date",
sched_tc_740 "Cheque/Invoice TC",
(sched_amt_740/100) "Transfer Amount",
substr(batch_curr_731,1,3) "Transfer Currency",
sched_trans_amt_740 "Transfer Trans Amt",
substr(batch_curr_731,4,3)  "Transfer Local Currency",,
pers_SSO_ID_796 "Transferred by SSO",
pers_name_796 "Transferred by Name",
sched_batch_no_740 "Cheque/Invoice ID",
SCHED_CUST_NO_740 "GECARS Customer Number",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
cust_client_717 "ERP Customer Number",
sched_inv_no_740 "Cheque/Inv Number",
sched_inv_date_740 "Cheque/Inv Date",
null "Cheque/Inv Name",
(batch_amt_731/100) "Deposit/Inv Amount",
substr(batch_curr_731,1,3) "Deposit/Inv Currency",
batch_trans_amt_731 "Trans Amount",
substr(batch_curr_731,4,3) "Local Currency",
Journal.gl_org_ID "DTDF Legal ID",
Journal.gl_cost_center "DTDF Cost Center",
Journal.gl_acct "DTDF GL Account",
Journal.gl_journal_ID "DTDF ID",
Journal.gl_description "DTDF GL Description",
offset.gl_org_ID "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_ID "Offset ID",
Offset.gl_description "Offset GL Description",
sched_je_ID_740 "JE ID",
Journal.gl_summary_ID "Journal Summary ID",
Offset.gl_summary_ID "Offset Summary ID",
substr(sched_offset_pseu_acct_740,1,1) "From SOB",
substr(sched_offset_pseu_acct_740,21,1) "To SOB"
from batch_schedule_journal_entry,investment_code sc,investment_code je,
batch_schedule_header,customer,invoice,personnel,
(select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''BJ''
          ) journal,
          (select cross_detail_ID,
                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                   gl_summary_ID, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_ID = gl_summary_ID
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''BJ''
          ) offset
where sched_batch_no_740 = batch_sche_no_731
and sched_ic_no_740 = batch_ic_no_731
and sched_batch_date_740 = batch_sche_date_731
and sched_user_id_740 = pers_code_796(+)
and batch_ic_no_731 = sc.ic_no_702
and sched_je_ic_no_740 = je.ic_no_702
and sched_tc_740 in (''771'',''772'')
and SCHED_CUST_NO_740 = cust_no_717(+)
and sched_je_ID_740 = Journal.cross_detail_ID(+)
and sched_je_ID_740 = offset.cross_detail_ID(+)
and sched_potential_apply_flag_740 = ''N''
and sched_posted_740 in (''N'',''Y'')
and sched_je_date_740 between TO_DATE(''' || REP_START_DATE || ''',''MMDDYYYYHH24MISS'')
                      AND TO_DATE(''' || REP_END_DATE || ''',''MMDDYYYYHH24MISS'')
and trim(substr(sched_offset_pseu_acct_740,3,9)) in (''' || IN_ACCT_GROUP || ''')';

if ERR_MSG = '000' then
      out_message := '000';
      OPEN TRANSFER_REPORT FOR QUERY;
  end if;

EXCEPTION
  WHEN OTHERS THEN
    OPEN TRANSFER_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;

    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_GET_RECON_TRANSFER_REP',
                               IN_ACCT_GROUP,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'GET NEW BILLING REPORT PROCESS FAILED ' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_GET_RECON_TRANSFER_REP;


CREATE OR REPLACE PROCEDURE SPR_GET_RECON_CASH_BOOK_REP(IN_ACCT_GROUP    IN VARCHAR2,
                                                      START_DATE       IN VARCHAR2,
                                                      END_DATE         IN VARCHAR2,
                                                      out_message      OUT VARCHAR2,
                                                      CASH_BOOK_REPORT OUT SYS_REFCURSOR) AS

  REP_START_DATE VARCHAR2(20);
  REP_END_DATE   VARCHAR2(20);

  ERR_NUM     NUMBER;
  ERR_MESSAGE VARCHAR2(200);
  ERR_MSG     VARCHAR2(200);
  QUERY       VARCHAR2(4000);
  QUERY_START VARCHAR2(2000);
  QUERY_END   VARCHAR2(2000);

BEGIN
  ERR_MSG := '000';
  
  QUERY_START := 'SELECT TO_CHAR(CDS_START_DATE, ''MMDDYYYYHH24MISS'')
    FROM CLASSIC_DAILY_SUMMARY
   WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
     AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
   EXECUTE IMMEDIATE QUERY_START into REP_START_DATE;

  if END_DATE is null then
    QUERY_END := 'SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP  || '''
       AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  else
    QUERY_END := ' SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
      FROM CLASSIC_DAILY_SUMMARY
     WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
       AND CDS_RECON_DATE = TO_DATE(''' || END_DATE || ''', ''MM/DD/YYYY'')';
   
       EXECUTE IMMEDIATE QUERY_END into REP_END_DATE;
  end if;

  QUERY := 'select lock_box_acct_group_766 "Account Group",
batch_ic_no_716 "Investment Code",
batch_je_date_716 "Cash Book Date",
lock_box_no_766 "Lock Box",
lock_box_treasury_cd_766 "Treasury Code",
lock_box_bank_name_766 "Bank Name",
batch_no_716 "Batch Number",
batch_date_716 "Batch Date",
pers_SSO_ID_796 "Booked By User SSO ID",
pers_name_796 "Booked By User Name",
cheque_id_719 "Cheque ID",
cheque_no_719 "Cheque Number",
cheque_date_719 "Cheque Date",
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Cheque Name",
(cheque_dep_amt_719/100) "Deposit Amount",
cheque_dep_curr3_719 "Deposit Currency",
cheque_trans_amt_719 "Cheque Trans Amount",
cheque_dep_loc_curr3_719 "Local Currency",
(CASE WHEN cheque_unident_flag_719 = ''Y'' THEN ''Unidentified''
                       WHEN Cheque_Unappl_sw_719 = ''Y'' THEN ''Unapplied''
end) "Payment Status",
Journal.gl_org_id "Bank Legal ID",
Journal.gl_cost_center "Bank Cost Center",
Journal.gl_acct "Bank GL Account",
Journal.gl_journal_id "Bank ID",
Journal.gl_description "Bank GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
batch_je_id_716 "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
from cheque,
cheque_batch_header,
lock_box,
personnel,
(select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''CH''
          ) journal,
          (select cross_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id, gl_description
          from je_cross_reference, je_summary_staging
          where cross_summary_id = gl_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''CH''
          ) offset
where cheque_batch_no_719 = batch_no_716
and cheque_lock_box_719 = batch_lock_box_716
and cheque_lock_box_719 = lock_box_no_766
and batch_oper_code_716 = pers_code_796(+)
and batch_je_id_716 = journal.cross_detail_id(+)
and batch_je_id_716 = offset.cross_detail_id(+)
and batch_je_date_716 between TO_DATE(''' || REP_START_DATE || ''',''MMDDYYYYHH24MISS'')
                      AND TO_DATE(''' || REP_END_DATE || ''',''MMDDYYYYHH24MISS'')
and lock_box_acct_group_766 in (''' || IN_ACCT_GROUP || ''')';

dbms_output.put_line(QUERY);

  if ERR_MSG = '000' then
      out_message := '000';
      OPEN CASH_BOOK_REPORT FOR QUERY;
  end if;

EXCEPTION
  WHEN OTHERS THEN
    OPEN CASH_BOOK_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;

    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_GET_RECON_CASH_BOOK_REP',
                               IN_ACCT_GROUP,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'GET NEW BILLING REPORT PROCESS FAILED ' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_GET_RECON_CASH_BOOK_REP;

create or replace procedure SPR_GCARS_OI_WALK_SEARCH(v_SSO_ID            IN VARCHAR2,
                                                 v_business       IN VARCHAR2,
                                                 v_legal_id       IN VARCHAR2,
                                                 v_cost_center    IN VARCHAR2,
                                                 v_account_group  IN VARCHAR2,
                                                 v_IC             IN VARCHAR2,
                                                 v_pole           IN VARCHAR2,
                                                 v_gl_period_from IN VARCHAR2,
                                                 v_gl_period_to   IN VARCHAR2,
                                                 v_gl_date_from   IN VARCHAR2,
                                                 v_gl_date_to     IN VARCHAR2,
                                                 out_message      out VARCHAR2,
                                                 out_summary      out sys_refcursor) AS

  v_bussiness_nw      VARCHAR2(3);
  v_legal_id_nw       VARCHAR2(200);
  v_check_res         varchar2(2000);
  v_cost_center_nw    VARCHAR2(200);
  v_account_group_nw  VARCHAR2(200);
  v_IC_nw             VARCHAR2(200);
  v_pole_nw           VARCHAR2(4);
  v_gl_period_from_nw VARCHAR2(8);
  v_gl_period_to_nw   VARCHAR2(8);
  v_gl_date_from_nw   date;
  v_gl_date_to_nw     date;
  v_user_code         varchar2(10);
  v_start_date        date;
  v_end_date          date;
  v_acct_groups       VARCHAR2(10000);
  v_out_warning       varchar2(2000);

  v_final_query       varchar2(30000);
  v_where_clause      varchar2(2000);
  v_select_query      varchar2(2000);
  v_check_query       varchar2(2000);

  ERR_MSG             varchar2(100);
  ERR_NUM             NUMBER;
  ERR_MESSAGE         VARCHAR2(200);

begin
 ERR_MSG        := '000';
 v_check_res    := NULL;

  select pers_code_796 into v_user_code from personnel
  where pers_sso_id_796 = v_SSO_ID;

  v_select_query := 'select listagg(acct_group, '','') within group(order by acct_group)
            from (select distinct trim(skat_group) acct_group
                from investment_code,gl_skat,client_entity
                where ic_acct_group_702 = trim(skat_group)
                  and CLIENT_ID_208 = ic_client_id_702
                  and ic_no_702 in (select ic_no_702 from investment_code
                          start with ic_no_702 in (select connection_ic_no_694 from personnel_ic_connector
                          where connection_pers_code_694 = '''|| v_user_code ||''')
                          connect by prior ic_no_702 = ic_owner_no_702)';


  v_bussiness_nw := TRIM(v_business);
  if v_bussiness_nw is null then
    ERR_MSG := 'Business is mandetory';
    goto return_exception;
  else
    v_where_clause := ' and ic_h2_702 = ''' || v_business || '''';
  end if;

-- in case of multiple inputs, extra spaces should be removed before sending the inputs from online
-- these all can be null, not mandatory , only business ID and date range is also allowed
-- one one of these can be multiple input
  v_legal_id_nw       := TRIM(v_legal_id);
  v_cost_center_nw    := TRIM(v_cost_center);
  v_account_group_nw  := TRIM(v_account_group);
  v_IC_nw             := TRIM(v_IC);

  if (((instr(v_legal_id_nw, ',') > 0) and (instr(v_cost_center_nw, ',') > 0)) or
  ((instr(v_legal_id_nw, ',') > 0) and (instr(v_account_group_nw, ',') > 0)) or
  ((instr(v_legal_id_nw, ',') > 0) and (instr(v_IC_nw, ',') > 0)) or
  ((instr(v_cost_center_nw, ',') > 0) and (instr(v_account_group_nw, ',') > 0)) or
  ((instr(v_cost_center_nw, ',') > 0) and (instr(v_IC_nw, ',') > 0)) or
  ((instr(v_account_group_nw, ',') > 0) and (instr(v_IC_nw, ',') > 0)))
  then
    ERR_MSG := 'Only one of Legal ID, Cost Center, Account Group and Investment Code can have multiple input values';
    goto return_exception;
  end if;

  if v_legal_id_nw is not null then
    if substr(v_legal_id_nw, LENGTH(v_legal_id_nw), 1) = ',' then
      v_legal_id_nw := substr(v_legal_id_nw, LENGTH(v_legal_id_nw), 1);
    end if;

    if instr(v_legal_id_nw, ',') > 0 then
      --000320,IF1003
      v_legal_id_nw := '''' || replace(v_legal_id_nw, ',', ''',''') || '''';
    else
      v_legal_id_nw := ''''|| v_legal_id_nw ||'''';
    end if;

    v_check_query := 'select listagg(org_id, '','') within group(order by org_id) from (
                      select distinct(trim(skat_org_id)) org_id
                      from gl_skat,investment_code
                      where trim(SKAT_GROUP)=IC_ACCT_GROUP_702
                      and trim(SKAT_ORG_ID) in (' || v_legal_id_nw || ')
                      and IC_H2_702 = ''' || v_bussiness_nw || ''')';

    EXECUTE IMMEDIATE v_check_query into v_check_res;

    dbms_output.put_line(v_check_res);
    dbms_output.put_line(v_check_query);
    
    if v_check_res is null then
       v_out_warning := v_out_warning || ' : Results will not have any data as the provided Org IDs are not mapped with ' || v_bussiness_nw || ' Business or the Input Values are Incorrect';
    elsif  regexp_count(v_check_res,',') < regexp_count(v_legal_id_nw,',') then
       v_out_warning := ' : Results will not have data for Legal IDs/Org IDs other than ' || v_check_res ||
                        ', Those are either invalid Inputs or are not mapped with ' || v_bussiness_nw || ' Business';
    end if;
    
    v_where_clause := v_where_clause || ' and trim(skat_org_id) in (' || v_legal_id_nw || ')';

  end if;

  if v_cost_center_nw is not null then
    if substr(v_cost_center_nw, LENGTH(v_cost_center_nw), 1) = ',' then
      v_cost_center_nw := substr(v_cost_center_nw,
                                 LENGTH(v_cost_center_nw),
                                 1);
    end if;
    if instr(v_cost_center_nw, ',') > 0 then
      --000320,IF1003
      v_cost_center_nw := '''' || replace(v_cost_center_nw, ',', ''',''') || '''';
    else
      v_cost_center_nw := ''''|| v_cost_center_nw ||'''';
    end if;
    
    v_check_query := 'select listagg(cost_center, '','') within group(order by cost_center) from (
                      select distinct(trim(skat_cost_center)) cost_center
                      from gl_skat,investment_code
                      where trim(SKAT_GROUP)=IC_ACCT_GROUP_702
                      and trim(SKAT_COST_CENTER) in (' || v_cost_center_nw || ')
                      and IC_H2_702 = ''' || v_bussiness_nw || ''')';

    EXECUTE IMMEDIATE v_check_query into v_check_res;

    dbms_output.put_line(v_check_res);
    dbms_output.put_line(v_check_query);
    if v_check_res is null then
       v_out_warning := v_out_warning || ' : Results will not have any data as the provided Cost Centers are not mapped with ' || v_bussiness_nw || ' Business or the Input Values are Incorrect';
    elsif regexp_count(v_check_res,',') < regexp_count(v_cost_center_nw,',') then
       v_out_warning := v_out_warning || ' : Results will not have data for Cost Centers/MEs other than ' || v_check_res ||
                        ', Those are either invalid Inputs or are not mapped with ' || v_bussiness_nw || ' Business';
    end if;
    
    v_where_clause := v_where_clause || ' and trim(SKAT_COST_CENTER) in (' ||
                      v_cost_center_nw || ')';
  end if;

  if v_account_group_nw is not null then
    if substr(v_account_group_nw, LENGTH(v_account_group_nw), 1) = ',' then
      v_account_group_nw := substr(v_account_group_nw, LENGTH(v_account_group_nw), 1);
    end if;
    if instr(v_account_group_nw, ',') > 0 then
      --000320,IF1003
      v_account_group_nw := '''' || replace(v_account_group_nw, ',', ''',''') || '''';
    else
      v_account_group_nw := ''''|| v_account_group_nw ||'''';
    end if;
    
    v_check_query := 'select listagg(acct_group, '','') within group(order by acct_group) from (
                      select distinct(trim(skat_group)) acct_group
                      from gl_skat,investment_code
                      where trim(SKAT_GROUP) = ic_acct_group_702
                      and trim(SKAT_GROUP) in (' || v_account_group_nw || ')
                      and IC_H2_702 = ''' || v_bussiness_nw || ''')';

    EXECUTE IMMEDIATE v_check_query into v_check_res;

    dbms_output.put_line(v_check_res);
    dbms_output.put_line(v_check_query);
    
    
    if v_check_res is null then
       v_out_warning := v_out_warning || ' : Results will not have any data as the provided Account Groups are not mapped with ' || v_bussiness_nw || ' Business or the Inputs are Incorrect';
    elsif regexp_count(v_check_res,',') < regexp_count(v_cost_center_nw,',') then
       v_out_warning := v_out_warning || ' : Results will not have data for Account Groups other than ' || v_check_res ||
                        ', Those are either invalid Inputs or are not mapped with ' || v_bussiness_nw || ' Business';
    end if;
    v_where_clause := v_where_clause || ' and trim(skat_group) in (' ||
                      v_account_group_nw || ')';
  end if;

  if v_IC_nw is not null then
    if substr(v_IC_nw, LENGTH(v_IC_nw), 1) = ',' then
      v_IC_nw := substr(v_IC_nw, LENGTH(v_IC_nw), 1);
    end if;
    if instr(v_IC_nw, ',') > 0 then
      --000320,IF1003
      v_IC_nw := '''' || replace(v_IC_nw, ',', ''',''') || '''';
    else
      v_IC_nw := ''''|| v_IC_nw ||'''';
    end if;
    v_where_clause := v_where_clause || ' and ic_no_702 in (' ||
                      v_IC_nw || ')';
  end if;

  v_pole_nw := TRIM(v_pole);

  if v_pole_nw is not null then
    v_where_clause := v_where_clause || ' and client_pole_208 in (''' ||
                      v_pole_nw || ''')';
  end if;

  v_gl_period_from_nw := TRIM(v_gl_period_from);
  v_gl_period_to_nw   := TRIM(v_gl_period_to);

  if v_gl_period_from_nw is null and v_gl_date_from is null then
    ERR_MSG := 'Atleast one GL date range or period input should be provided';
    goto return_exception;
  end if;

-- only allow one input for date, either period or date range from screen
  if v_gl_period_from_nw is not null then
    v_gl_period_from_nw := substr(v_gl_period_from_nw, 1, 2) || '01' ||
                           substr(v_gl_period_from_nw, 3);
    if v_gl_period_to_nw is null then
      --from 042018
      v_gl_period_to_nw := to_char(SYSDATE, 'MMDDYYYY');
    else
      v_gl_period_to_nw := substr(v_gl_period_to_nw, 1, 2) ||
                           to_char(LAST_DAY(to_date(v_gl_period_to_nw,
                                                    'MMYYYY')),
                                   'DD') || substr(v_gl_period_to_nw, 3);
    end if;
    if ((to_date(v_gl_period_to_nw, 'MMDDYYYY') -
       to_date(v_gl_period_from_nw, 'MMDDYYYY')) > 365) then
      ERR_MSG := 'Selected date range is more than 365 days, Please decrease the date range';
      goto return_exception;
    end if;

    v_start_date := to_date(v_gl_period_from_nw,'mm/dd/yyyy');
    v_end_date   := to_date(v_gl_period_to_nw,'mm/dd/yyyy');

  end if;

  if v_gl_date_from is not null then
    v_gl_date_from_nw := to_date(v_gl_date_from,'mm/dd/yyyy');
    if v_gl_date_to is null then
      v_gl_date_to_nw := trunc(SYSDATE);
    else
      v_gl_date_to_nw  := to_date(v_gl_date_to,'mm/dd/yyyy');
    end if;
    if ((v_gl_date_to_nw - v_gl_date_from_nw) > 365) then
      ERR_MSG := 'Selected date range is more than 365 days, Please decrease the date range';
      goto return_exception;
    end if;

  v_start_date := v_gl_date_from_nw;
  v_end_date   := v_gl_date_to_nw;

  end if;

    v_select_query := v_select_query || v_where_clause || ')';

    EXECUTE IMMEDIATE v_select_query into v_acct_groups;

    v_acct_groups :=  '''' || replace(v_acct_groups, ',', ''',''') || '''';

  v_final_query := 'select summ.cds_acct_group "Account Group",
         start_cds.cds_recon_date || '' To '' || end_cds.cds_recon_date "Selected Period",
         summ.cds_loc_curr3,
         (start_cds.cds_prev_oi_trans_amt + start_cds.cds_prev_uni_cash_trans_amt) "Opening Balance",
         billing "New Items Loaded",
         cash "Payment Booked",
         writeoff "Adjustments/Write-Off",
         transfer "Transfer In/Out",
         ((start_cds.cds_prev_oi_trans_amt + start_cds.cds_prev_uni_cash_trans_amt)
            -billing - cash - writeoff - transfer) "Closing Balance (Calculated)",
         (end_cds.cds_prev_oi_trans_amt + end_cds.cds_prev_uni_cash_trans_amt) "Closing Balance (System)",
     imbalance "Details of Mismatch"
     from
(select cds_acct_group,
            cds_loc_curr3,
            min(cds_recon_date) start_date,
            sum(cds_billing_trans_amt) billing,
            sum(cds_cash_book_trans_amt) cash,
            sum(cds_writeoff_trans_amt) writeoff,
            sum(cds_transfer_trans_amt) transfer,
            sum(cds_trans_imbalance) imbalance,
            max(cds_recon_date) end_date
from classic_daily_summary
where cds_acct_group in ('|| v_acct_groups || ')
and CDS_RECON_DATE between ''' || v_start_date || ''' and ''' || v_end_date || '''
group by cds_acct_group,cds_loc_curr3) summ,
      classic_daily_summary start_cds,
      classic_daily_summary end_cds
where summ.cds_acct_group = start_cds.cds_acct_group
        and summ.cds_acct_group = end_cds.cds_acct_group
        and summ.start_date = start_cds.cds_recon_date
        and summ.end_date = end_cds.cds_recon_date
		order by imbalance desc';


dbms_output.put_line(v_final_query);
  <<return_exception>>
  ERR_MESSAGE := ERR_MSG;

  if ERR_MSG = '000' then
     out_message := '000';
     if v_out_warning is not null then
        out_message := out_message || v_out_warning;
     end if;
      open out_summary for v_final_query;
  else
      out_message := ERR_MSG;
      open out_summary for select * from dual where 1=2;
  end if;

  SPR_GCARS_BATCHPROCESS_LOG('SPR_GCARS_OI_WALK_SEARCH',
                             'SPR_GCARS_OI_WALK_SEARCH',
                             v_SSO_ID,
                             To_Char(SYSDATE, 'MMDDYYYY'),
                             ERR_MESSAGE,
                             'Basic',
                             'DAILY');
EXCEPTION
  when others then
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;
    open out_summary for select * from dual where 1=2;

    SPR_GCARS_BATCHPROCESS_LOG('SPR_GCARS_OI_WALK_SEARCH',
                                'SPR_GCARS_OI_WALK_SEARCH',
                                v_SSO_ID,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                                ERR_NUM || '. ' || ERR_MESSAGE,
                                'Basic',
                                'DAILY');
end SPR_GCARS_OI_WALK_SEARCH;

create or replace procedure SPR_GCARS_JE_ENQUIRY_SEARCH(v_SSO_ID         IN VARCHAR2,
                                                 v_business       IN VARCHAR2,
                                                 v_legal_id       IN VARCHAR2,
                                                 v_cost_center    IN VARCHAR2,
                                                 v_gl_account     IN VARCHAR2,
                                                 v_journal_id     IN VARCHAR2,
                                                 v_gl_source      IN VARCHAR2,
                                                 v_amount_from    IN NUMBER,
                                                 v_amount_to      IN NUMBER,
                                                 v_tc             IN VARCHAR2,
                                                 v_gl_period_from IN VARCHAR2,
                                                 v_gl_period_to   IN VARCHAR2,
                                                 v_gl_date_from   IN VARCHAR2,
                                                 v_gl_date_to     IN VARCHAR2,
                                                 out_message      out VARCHAR2,
                                                 out_gl_line_online      out sys_refcursor,
                                                 out_gl_line_download    out sys_refcursor) AS

-- add multiple TC inputs, and multiple Journal ID

  v_bussiness_nw      VARCHAR2(3);
  v_legal_id_nw       VARCHAR2(1000);
  v_check_res         varchar2(2000);
  v_cost_center_nw    VARCHAR2(1000);
  v_gl_account_nw     VARCHAR2(1000);
  v_journal_id_nw     VARCHAR2(1000);
  v_gl_source_nw      VARCHAR2(1000);
  v_amount_from_nw    NUMBER;
  v_amount_to_nw      NUMBER;
  v_tc_nw             VARCHAR2(1000);
  inp_count           NUMBER;
  comma_count         NUMBER;
  v_tc_split          VARCHAR2(1000);
  first_inp           boolean;
  i                   number;
  v_gl_period_from_nw VARCHAR2(8);
  v_gl_period_to_nw   VARCHAR2(8);
  v_gl_date_from_nw   date;
  v_gl_date_to_nw     date;
  v_user_code         varchar2(10);
  v_out_warning       varchar2(2000);

  v_where_clause               varchar2(15000);
  v_select_query_download      varchar2(15000);
  v_select_query_online        varchar2(15000);
  v_final_query_download       varchar2(15000);
  v_final_query_online         varchar2(15000);
  v_check_query                varchar2(2000);

  ERR_MSG             varchar2(100);
  ERR_NUM             NUMBER;
  ERR_MESSAGE         VARCHAR2(200);

begin
 ERR_MSG        := '000';
 v_check_res    := NULL;

  select pers_code_796 into v_user_code from personnel
  where pers_sso_id_796 = v_SSO_ID;

  v_select_query_download := 'select distinct
                                      gl_date,
                                      gl_summary_id,
                                      gl_outfile,
                                      gl_journ_type,
                                      gl_sb_id,
                                      gl_source_id,
                                      gl_tran_type,
                                      gl_tran_date,
                                      gl_journal_id,
                                      gl_sub_acct,
                                      gl_currency,
                                      gl_conversion_type,
                                      gl_conversion_rate,
                                      gl_entered_amount,
                                      gl_cr_dr,
                                      gl_org_id,
                                      gl_acct,
                                      gl_function,
                                      gl_analytic,
                                      gl_cost_center,
                                      gl_intercompany,
                                      gl_reserved1,
                                      gl_reserved2,
                                      gl_indicator,
                                      gl_translate,
                                      gl_futureuse1,
                                      gl_accounted_amount,
                                      gl_description,
                                      gl_coa_product_line,
                                      gl_coa_project_code,
                                      gl_coa_geography,
                                      gl_coa_intercompany,
                                      gl_coa_reference,
                                      gl_coa_sob,
                                      gl_coa_future_use1,
                                      gl_coa_future_use2
                     from je_summary_staging,investment_code,gl_skat
                     where skat_group = ic_acct_group_702
                     and gl_org_id = trim(skat_org_id)
                     and ic_no_702 in (select ic_no_702 from investment_code
                     start with ic_no_702 in (select connection_ic_no_694 from personnel_ic_connector
                     where connection_pers_code_694 = '''|| v_user_code ||''')
                     connect by prior ic_no_702 = ic_owner_no_702)';

  v_select_query_online := 'select distinct to_date(gl_tran_date,''mmddyyyy'') "GL Date",
                                   gl_source_id "Source ID",
                                   gl_acct "GL Account No",
                                   gl_org_id "Legal Enity",
                                   gl_cost_center "Cost Center",
                                   gl_cr_dr "Debit/Credit",
                                   gl_entered_amount "Amount",
                                   gl_currency "Currency",
                                   decode(gl_accounted_amount,null,gl_entered_amount,gl_accounted_amount) "Trans Amount",
                                   gl_journal_id "JE Identifier",
                                   gl_description "JE Description",
                                   gl_summary_id "Summary ID"
                     from je_summary_staging,investment_code,gl_skat
                     where skat_group = ic_acct_group_702
                     and gl_org_id = trim(skat_org_id)
                     and ic_no_702 in (select ic_no_702 from investment_code
                     start with ic_no_702 in (select connection_ic_no_694 from personnel_ic_connector
                     where connection_pers_code_694 = '''|| v_user_code ||''')
                     connect by prior ic_no_702 = ic_owner_no_702)';


  v_bussiness_nw := TRIM(v_business);
  if v_bussiness_nw is null then
    ERR_MSG := 'Business is mandetory';
    goto return_exception;
  else
    v_where_clause := ' and ic_h2_702 = ''' || v_business || '''';
  end if;

-- in case of multiple inputs, extra spaces should be removed before sending the inputs from online
  v_legal_id_nw    := TRIM(v_legal_id);
  v_cost_center_nw := TRIM(v_cost_center);
  v_gl_account_nw  := TRIM(v_gl_account);

  if    (((instr(v_legal_id_nw, ',') > 0) and instr(v_cost_center_nw, ',') > 0)
     or ((instr(v_legal_id_nw, ',') > 0) and instr(v_gl_account_nw, ',') > 0)
     or ((instr(v_cost_center_nw, ',') > 0) and instr(v_gl_account_nw, ',') > 0)
     or ((instr(v_cost_center_nw, ',') > 0) and instr(v_gl_account_nw, ',') > 0) and (instr(v_legal_id_nw, ',') > 0))
  then
    ERR_MSG := 'Only one of Legal ID, Cost Center and GL Account can have multiple input values';
    goto return_exception;
  end if;

  if v_legal_id_nw is not null then
    if substr(v_legal_id_nw, LENGTH(v_legal_id_nw), 1) = ',' then
      v_legal_id_nw := substr(v_legal_id_nw, LENGTH(v_legal_id_nw), 1);
    end if;
    if instr(v_legal_id_nw, ',') > 0 then
      --000320,IF1003
      v_legal_id_nw := '''' || replace(v_legal_id_nw, ',', ''',''') || '''';
    else
      v_legal_id_nw := ''''|| v_legal_id_nw ||'''';
    end if;
    v_check_query := 'select listagg(org_id, '','') within group(order by org_id) from (
                      select distinct(trim(skat_org_id)) org_id
                      from gl_skat,investment_code
                      where trim(SKAT_GROUP)=IC_ACCT_GROUP_702
                      and trim(SKAT_ORG_ID) in (' || v_legal_id_nw || ')
                      and IC_H2_702 = ''' || v_bussiness_nw || ''')';
                      
    EXECUTE IMMEDIATE v_check_query into v_check_res;

    dbms_output.put_line(v_check_res);
    dbms_output.put_line(v_check_query);
    
    if v_check_res is null then
       v_out_warning := v_out_warning || ' : Results will not have any data as the provided Org IDs are not mapped with ' || v_bussiness_nw || ' Business or the Input Values are Incorrect';
    elsif  regexp_count(v_check_res,',') < regexp_count(v_legal_id_nw,',') then
       v_out_warning := ' : Results will not have data for Legal IDs/Org IDs other than ' || v_check_res ||
                        ', Those are either invalid Inputs or are not mapped with ' || v_bussiness_nw || ' Business';
    end if;
    
    v_where_clause := v_where_clause || ' and gl_org_id in (' ||
                      v_legal_id_nw || ')';
  end if;

  if v_cost_center_nw is not null then
    if substr(v_cost_center_nw, LENGTH(v_cost_center_nw), 1) = ',' then
      v_cost_center_nw := substr(v_cost_center_nw,
                                 LENGTH(v_cost_center_nw),
                                 1);
    end if;
    if instr(v_cost_center_nw, ',') > 0 then
      --000320,IF1003
      v_cost_center_nw := '''' || replace(v_cost_center_nw, ',', ''',''') || '''';
    else
      v_cost_center_nw := ''''|| v_cost_center_nw ||'''';
    end if;
    
    v_check_query := 'select listagg(cost_center, '','') within group(order by cost_center) from (
                      select distinct(trim(skat_cost_center)) cost_center
                      from gl_skat,investment_code
                      where trim(SKAT_GROUP)=IC_ACCT_GROUP_702
                      and trim(SKAT_COST_CENTER) in (' || v_cost_center_nw || ')
                      and IC_H2_702 = ''' || v_bussiness_nw || ''')';

    EXECUTE IMMEDIATE v_check_query into v_check_res;

    dbms_output.put_line(v_check_res);
    dbms_output.put_line(v_check_query);
    if v_check_res is null then
       v_out_warning := v_out_warning || ' : Results will not have any data as the provided Cost Centers are not mapped with ' || v_bussiness_nw || ' Business or the Input Values are Incorrect';
    elsif regexp_count(v_check_res,',') < regexp_count(v_cost_center_nw,',') then
       v_out_warning := v_out_warning || ' : Results will not have data for Cost Centers/MEs other than ' || v_check_res ||
                        ', Those are either invalid Inputs or are not mapped with ' || v_bussiness_nw || ' Business';
    end if;
    
    v_where_clause := v_where_clause || ' and trim(gl_cost_center) in (' ||
                      v_cost_center_nw || ')';
  end if;

  v_journal_id_nw := TRIM(v_journal_id);

  if v_journal_id_nw is not null then
    if substr(v_journal_id_nw, LENGTH(v_journal_id_nw), 1) = ',' then
      v_journal_id_nw := substr(v_journal_id_nw, LENGTH(v_journal_id_nw), 1);
    end if;
    if instr(v_journal_id_nw, ',') > 0 then
      --000320,IF1003
      v_journal_id_nw := '''' || replace(v_journal_id_nw, ',', ''',''') || '''';
    else
      v_journal_id_nw := ''''|| v_journal_id_nw ||'''';
    end if;
    
    v_where_clause := v_where_clause || ' and gl_journal_id in (' ||
                      v_journal_id_nw || ')';
  end if;

  v_gl_source_nw := TRIM(v_gl_source);

  if v_gl_source_nw is not null then
    v_where_clause := v_where_clause || '
    and trim(gl_source_id) in (''' ||
                      v_gl_source_nw || ''')';
  end if;

  --validate from and to amounts if from amount is entered to amount should not be null
  v_amount_from_nw := TRIM(v_amount_from);
  v_amount_to_nw   := TRIM(v_amount_to);

  if ((v_amount_from_nw is not null) and (v_amount_to_nw is not null)) then
    --v_amount_from_nw = 2523
    --v_amount_to = 3000
    if v_amount_from_nw > v_amount_to_nw then
      ERR_MSG := 'To amount should be greater than From amount';
      goto return_exception;
    end if;
    v_where_clause := v_where_clause ||
                      '
                      and ((gl_accounted_amount between ' ||
                      v_amount_from_nw || ' and ' || v_amount_to_nw || ') or (gl_entered_amount between ' ||
                      v_amount_from_nw || ' and ' || v_amount_to_nw || '))';
  end if;

  v_gl_period_from_nw := TRIM(v_gl_period_from);
  v_gl_period_to_nw   := TRIM(v_gl_period_to);

  if v_gl_period_from_nw is null and v_gl_date_from is null then
    ERR_MSG := 'Atleast one GL date range or period input should be provided';
    goto return_exception;
  end if;

-- only allow one input for date, either period or date range from screen
  if v_gl_period_from_nw is not null then
    v_gl_period_from_nw := substr(v_gl_period_from_nw, 1, 2) || '01' ||
                           substr(v_gl_period_from_nw, 3);
    if v_gl_period_to_nw is null then
      --from 042018
      v_gl_period_to_nw := to_char(SYSDATE, 'MMDDYYYY');
    else
      v_gl_period_to_nw := substr(v_gl_period_to_nw, 1, 2) ||
                           to_char(LAST_DAY(to_date(v_gl_period_to_nw,
                                                    'MMYYYY')),
                                   'DD') || substr(v_gl_period_to_nw, 3);
    end if;
    if ((to_date(v_gl_period_to_nw, 'MMDDYYYY') -
       to_date(v_gl_period_from_nw, 'MMDDYYYY')) > 365) then
      ERR_MSG := 'Selected date range is more than 365 days, Please decrease the date range';
      goto return_exception;
    end if;
    v_where_clause := v_where_clause ||
                      '
                      and to_date(gl_tran_date,''mmddyyyy'') between to_date(''' ||
                      v_gl_period_from_nw || ''',''mmddyyyy'') and to_date(''' ||
                      v_gl_period_to_nw || ''',''mmddyyyy'')';
  end if;

  if v_gl_date_from is not null then
    v_gl_date_from_nw := to_date(v_gl_date_from,'mm/dd/yyyy');
    if v_gl_date_to is null then
      v_gl_date_to_nw := trunc(SYSDATE);
    else
      v_gl_date_to_nw  := to_date(v_gl_date_to,'mm/dd/yyyy');
    end if;
    if ((v_gl_date_to_nw - v_gl_date_from_nw) > 365) then
      ERR_MSG := 'Selected date range is more than 365 days, Please decrease the date range';
      goto return_exception;
    end if;
    v_where_clause := v_where_clause ||
                      '
    and to_date(gl_tran_date,''mmddyyyy'') between ''' ||
                      v_gl_date_from_nw || ''' and ''' || v_gl_date_to_nw || '''';
  end if;

-- only one tc allowed, and add code to fetch offset entry of the TC entry as well
  v_tc_nw := TRIM(v_tc);
  if v_tc_nw is not null then
    if substr(v_tc_nw, LENGTH(v_tc_nw), 1) = ',' then
      --771,772
      v_tc_nw := substr(v_tc_nw, 1,v_tc_nw);
    end if;
    v_select_query_download := 'select distinct
                                        gl_date,
                                        gl_summary_id,
                                        gl_outfile,
                                        gl_journ_type,
                                        gl_sb_id,
                                        gl_source_id,
                                        gl_tran_type,
                                        gl_tran_date,
                                        gl_journal_id,
                                        gl_sub_acct,
                                        gl_currency,
                                        gl_conversion_type,
                                        gl_conversion_rate,
                                        gl_entered_amount,
                                        gl_cr_dr,
                                        gl_org_id,
                                        gl_acct,
                                        gl_function,
                                        gl_analytic,
                                        gl_cost_center,
                                        gl_intercompany,
                                        gl_reserved1,
                                        gl_reserved2,
                                        gl_indicator,
                                        gl_translate,
                                        gl_futureuse1,
                                        gl_accounted_amount,
                                        gl_description,
                                        gl_coa_product_line,
                                        gl_coa_project_code,
                                        gl_coa_geography,
                                        gl_coa_intercompany,
                                        gl_coa_reference,
                                        gl_coa_sob,
                                        gl_coa_future_use1,
                                        gl_coa_future_use2
 from je_summary_staging
            where gl_summary_id in (
            select cross_summary_id from je_cross_reference
            where (cross_table,cross_detail_id ) in (
            select cross_table,cross_detail_id from je_cross_reference
            where cross_summary_id in (
            select gl_summary_id from je_summary_staging,investment_code,gl_skat
           where trim(skat_group) = ic_acct_group_702
                 and gl_org_id = trim(skat_org_id)
                 and ic_no_702  in (select ic_no_702 from investment_code
           start with ic_no_702 in (select connection_ic_no_694 from personnel_ic_connector
           where connection_pers_code_694 = '''|| v_user_code ||''')
           connect by prior ic_no_702 = ic_owner_no_702)';


    v_select_query_online  := 'select distinct to_date(gl_tran_date,''mmddyyyy'') "GL Date",
                                   gl_source_id "Source ID",
                                   gl_acct "GL Account No",
                                   gl_org_id "Legal Enity",
                                   gl_cost_center "Cost Center",
                                   gl_cr_dr "Debit/Credit",
                                   gl_entered_amount "Amount",
                                   gl_currency "Currency",
                                   decode(gl_accounted_amount,null,gl_entered_amount,gl_accounted_amount) "Trans Amount",
                                   gl_journal_id "JE Identifier",
                                   gl_description "JE Description",
                                   gl_summary_id "Summary ID" from je_summary_staging
            where gl_summary_id in (
            select cross_summary_id from je_cross_reference
            where (cross_table,cross_detail_id ) in (
            select cross_table,cross_detail_id from je_cross_reference
            where cross_summary_id in (
            select gl_summary_id from je_summary_staging,investment_code,gl_skat
           where trim(skat_group) = ic_acct_group_702
                 and gl_org_id = trim(skat_org_id)
                 and ic_no_702  in (select ic_no_702 from investment_code
           start with ic_no_702 in (select connection_ic_no_694 from personnel_ic_connector
           where connection_pers_code_694 = '''|| v_user_code ||''')
           connect by prior ic_no_702 = ic_owner_no_702)';


    select REGEXP_COUNT(v_tc_nw, ',') into inp_count from dual;

    if inp_count = 0 then
            v_where_clause := v_where_clause || '
            and gl_description like ''%' ||
                              v_tc_nw || '%'')))';
    elsif inp_count > 0 then
          first_inp := true;
          for i in 1..inp_count+1  loop

            comma_count := instr(v_tc_nw,',');
            if comma_count = 0 then
            v_tc_split := v_tc_nw;
            else
            v_tc_split := substr(v_tc_nw,1,comma_count-1);
            end if;

                    if first_inp then
                      first_inp := false;
                    v_where_clause := v_where_clause || '
                    and ( gl_description like ''%' ||
                                      v_tc_split || '%''';
                    else
                    v_where_clause := v_where_clause || '
                    or gl_description like ''%' ||
                                      v_tc_split || '%''';
                    end if;

            v_tc_nw := substr(v_tc_nw,comma_count+1);
          end loop;

          v_where_clause := v_where_clause || '
          ))))';

    end if;

  end if;

  if v_gl_account_nw is not null then
    if substr(v_gl_account_nw, LENGTH(v_gl_account_nw), 1) = ',' then
      v_gl_account_nw := substr(v_gl_account_nw, LENGTH(v_gl_account_nw), 1);
    end if;
    if instr(v_gl_account_nw, ',') > 0 then
      --000320,IF1003
      v_gl_account_nw := '''' || replace(v_gl_account_nw, ',', ''',''') || '''';
    else
      v_gl_account_nw := ''''|| v_gl_account_nw ||'''';
    end if;
    
    v_where_clause := v_where_clause || ' and trim(gl_acct) in (' ||
                      v_gl_account_nw || ')';
  end if;

  v_final_query_download := v_select_query_download || v_where_clause || ' order by gl_summary_id';
  v_final_query_online   := v_select_query_online || v_where_clause || ' order by gl_summary_id';
  dbms_output.put_line(v_final_query_online);

  <<return_exception>>
  ERR_MESSAGE := ERR_MSG;

  if ERR_MSG = '000' then
      out_message := '000';
      
      if v_out_warning is not null then
        out_message := out_message || v_out_warning;
      end if;
      
      open out_gl_line_download for v_final_query_download;
      open out_gl_line_online for v_final_query_online;
  else
      out_message := ERR_MSG;
      open out_gl_line_online for select * from dual where 1=2;
      open out_gl_line_download for select * from dual where 1=2;
  end if;

  SPR_GCARS_BATCHPROCESS_LOG('SPR_GCARS_JE_ENQUIRY_SEARCH',
                             'SPR_GCARS_JE_ENQUIRY_SEARCH',
                             v_SSO_ID,
                             To_Char(SYSDATE, 'MMDDYYYY'),
                             'Finished with status : '|| ERR_MESSAGE,
                             'Basic',
                             'DAILY');
EXCEPTION
  when others then
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
      open out_gl_line_online for select * from dual where 1=2;
      open out_gl_line_download for select * from dual where 1=2;

    SPR_GCARS_BATCHPROCESS_LOG('SPR_GCARS_JE_ENQUIRY_SEARCH',
                                'SPR_GCARS_JE_ENQUIRY_SEARCH',
                                v_SSO_ID,
                                To_Char(SYSDATE, 'MMDDYYYY'),
                                ERR_NUM || '. ' || ERR_MESSAGE,
                                'Basic',
                                'DAILY');
end SPR_GCARS_JE_ENQUIRY_SEARCH;

CREATE OR REPLACE PROCEDURE SPR_CLASSIC_BATCH_IMBALANCE
AS
 ERR_NUM     NUMBER;
 ERR_MESSAGE VARCHAR2(200);

BEGIN
          BEGIN

          SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',

                                     'INVOICE BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'INVOICE BATCH VALIDATION PROCESS STARTED ',
                                     'Basic',
                                     'DAILY');

          insert into RECON_IMBALANCE_DETAILS
          (
          RID_ID,
          RID_SUMM_ID,
          RID_ERROR_CODE,
          RID_ERROR_DETAILS,
		  RID_LOGGED_BY,
		  RID_CREATED_DATE,
      RID_STATUS,
          TIMESTAMP
          )
          SELECT   RECON_IMBALANCE_DET_SEQ.NEXTVAL,
              CDS_RECORD_ID,
              'BL1',
              'BCO : '|| BCO ||', BATCH SCHED NO : '|| SCHE_NO || ', SCHED DATE' || SCHE_DATE
              ||', BATCH AMT : ' || BATCH_TRANS_AMT || ', INV AMT : ' || INV_TRANS_AMT
              || ', BATCH INV COUNT : '|| BATCH_INV_COUNT || ', INV COUNT : ' || INV_COUNT ||'.','SYSTEM',SYSDATE,'1',SYSDATE
          from (select summ.acct_group ACCT_GROUP,summ.bco BCO,summ.sche_date SCHE_DATE,summ.sche_no SCHE_NO,summ.ref_date REF_DATE,
                 NVL(summ.trans_amt,0) BATCH_TRANS_AMT,NVL(summ.inv_count,0) BATCH_INV_COUNT,
                 nvl(det.trans_amt,0) INV_TRANS_AMT,nvl(det.inv_count,0) INV_COUNT from
          (select cbs_acct_group acct_group,
                  cbs_bco bco,
                  cbs_sche_date sche_date,
                  cbs_sche_no sche_no,
                  cbs_ref_date ref_date,
                  cbs_trans_amt trans_amt,
                  cbs_inv_count inv_count
          from classic_billing_summary_mview) summ,
          (select cbd_acct_group acct_group,
                  cbd_bco bco,
                  cbd_sche_date sche_date,
                  cbd_sche_no sche_no,
                  cbd_ref_date ref_date,
                  sum(cbd_trans_amt) trans_amt,
                  count(cbd_inv_id) inv_count
          from classic_billing_detail_mview
          group by cbd_acct_group,cbd_bco,cbd_sche_date,cbd_sche_no,cbd_ref_date) det
          where summ.acct_group = det.acct_group(+)
                and summ.bco = det.bco(+)
                and summ.sche_date = det.sche_date(+)
                and summ.sche_no = det.sche_no(+)
                and summ.ref_date = det.ref_date(+)
                and nvl((-1*summ.trans_amt),0) != nvl(det.trans_amt(+),0)) INV_BATCH,
              CLASSIC_DAILY_SUMMARY
              WHERE CDS_ACCT_GROUP = INV_BATCH.ACCT_GROUP
              AND CDS_END_DATE = INV_BATCH.REF_DATE;

          SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                     'INVOICE BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'INVOICE BATCH VALIDATION PROCESS FINISHED ',
                                     'Basic',
                                     'DAILY');

          EXCEPTION WHEN OTHERS THEN
             ERR_NUM     := SQLCODE;
             ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
            ROLLBACK;
            SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                     'INVOICE BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'INVOICE BATCH VALIDATION PROCESS IN ERROR ' || ERR_MESSAGE,
                                     'Basic',
                                     'DAILY');
      RAISE;
          END;

          BEGIN
            SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                     'IC LEVEL BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'IC LEVEL BATCH VALIDATION PROCESS STARTED ',
                                     'Basic',
                                     'DAILY');
          INSERT INTO RECON_IMBALANCE_DETAILS
          (
          RID_ID,
          RID_SUMM_ID,
          RID_ERROR_CODE,
          RID_ERROR_DETAILS,
      RID_LOGGED_BY,
      RID_CREATED_DATE,
      RID_STATUS,
          TIMESTAMP
          )
          SELECT   RECON_IMBALANCE_DET_SEQ.NEXTVAL,
              CDS_RECORD_ID,
              'BL2',
              'BCO : '|| BCO ||', BATCH_SCHE_NO : '|| SCHE_NO || ', SCHED DATE' || SCHE_DATE || '., IC : ' || IC
              ||', BATCH AMT FOR IC: ' || SCHED_IC_TRANS_AMT || ', INV AMT FOR IC: ' || INV_IC_TRANS_AMT ||'.','SYSTEM',SYSDATE,'1',SYSDATE
          FROM (select det.acct_group ACCT_GROUP, 
       det.bco BCO, 
       det.sche_date SCHE_DATE, 
       det.sche_no SCHE_NO, 
       det.ic IC, 
       det.ref_date REF_DATE, 
       NVL(det.ic_lvl_trans_amt,0) INV_IC_TRANS_AMT, 
       nvl(sched_trans_amt_740,0)  SCHED_IC_TRANS_AMT 
from 
   (select inv_acct_group_720 acct_group, 
            inv_bill_sche_ic_no_720 bco, 
            inv_bill_sche_date_720 sche_date, 
            inv_bill_sche_no_720 sche_no, 
            inv_ic_no_720 ic,
            rc_curr_ref_date ref_date,
            sum(inv_trans_amt_720) ic_lvl_trans_amt, 
            count(inv_id_720) inv_count 
    from invoice ,recon_control 
    where (inv_bill_sche_ic_no_720, inv_bill_sche_no_720, inv_bill_sche_date_720) 
          in 
          (select inv_bill_sche_ic_no_720, 
                  inv_bill_sche_no_720, 
                  inv_bill_sche_date_720 
          from invoice,recon_control 
          where inv_system_dt_720 between rc_prev_ref_date and rc_curr_ref_date 
          and inv_init_tc_720 in (10, 12) 
          and inv_tranf_flag_720 = 'N' 
          and rc_run_level = 'GL'
          )
          and rc_run_level = 'GL' 
    group by inv_acct_group_720, 
             inv_bill_sche_ic_no_720, 
             inv_bill_sche_date_720, 
             inv_bill_sche_no_720, 
             inv_ic_no_720 
    ) det, 
    batch_schedule_journal_entry bsje,investment_code 
    where det.bco = sched_ic_no_740 (+) 
          and det.sche_date = sched_batch_date_740(+) 
          and det.sche_no = sched_batch_no_740(+) 
          and det.ic = sched_je_ic_no_740(+) 
          and sched_tc_740 is null 
          and det.bco = ic_no_702 
          and ic_postable_flag_702 = 'D' 
          and nvl(det.ic_lvl_trans_amt,0) <> nvl((-1*sched_trans_amt_740),0)) BSJE_IC,
              CLASSIC_DAILY_SUMMARY
              WHERE CDS_END_DATE = BSJE_IC.REF_DATE
              AND CDS_ACCT_GROUP = BSJE_IC.ACCT_GROUP;

          SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                     'IC LEVEL BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'IC LEVEL BATCH VALIDATION PROCESS FINISHED ',
                                     'Basic',
                                     'DAILY');

          EXCEPTION WHEN OTHERS THEN
                      ERR_NUM     := SQLCODE;
                      ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
                      ROLLBACK;
            SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                     'IC LEVEL BATCH VALIDATION',
                                     NULL,
                                     To_Char(SYSDATE, 'MMDDYYYY'),
                                     'IC LEVEL BATCH VALIDATION PROCESS IN ERROR '|| ERR_MESSAGE,
                                     'Basic',
                                     'DAILY');
      RAISE;
          END;

          BEGIN
            SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                               'PAYMENT BATCH VALIDATION',
                                               NULL,
                                               To_Char(SYSDATE, 'MMDDYYYY'),
                                               'PAYMENT BATCH VALIDATION PROCESS STARTED ',
                                               'Basic',
                                               'DAILY');
          INSERT INTO RECON_IMBALANCE_DETAILS
          (
           RID_ID,
          RID_SUMM_ID,
          RID_ERROR_CODE,
          RID_ERROR_DETAILS,
      RID_LOGGED_BY,
      RID_CREATED_DATE,
      RID_STATUS,
          TIMESTAMP
          )
          SELECT   RECON_IMBALANCE_DET_SEQ.NEXTVAL,
              CDS_RECORD_ID,
              'BL3',
              'LB : ' ||LB|| ',BATCH_NO : '|| BATCH_NO
              ||', BATCH AMT : '|| BATCH_TRANS_AMT || ', CHQ_AMT : ' || CHQ_TRANS_AMT
              || ', BATCH_INV_COUNT : '|| BATCH_CHQ_COUNT || ', CHQ_COUNT : ' || CHQ_COUNT ||'.','SYSTEM',SYSDATE,'1',SYSDATE
          FROM (select summ.acct_group ACCT_GROUP,summ.lb LB,summ.batch_no BATCH_NO,summ.ref_date REF_DATE,
                 NVL(summ.trans_amt,0) BATCH_TRANS_AMT,NVL(summ.chq_count,0) BATCH_CHQ_COUNT,
                 nvl(det.trans_amt,0) CHQ_TRANS_AMT,nvl(det.chq_count,0) CHQ_COUNT from
          (select ccs_acct_group acct_group,
                  ccs_lock_box lb,
                  ccs_batch_no batch_no,
                  ccs_ref_date ref_date,
                  ccs_trans_amt trans_amt,
                  ccs_chq_count chq_count
          from classic_cash_book_summ_mview) summ,
          (select csd_acct_group acct_group,
                  csd_lock_box lb,
                  csd_batch_no batch_no,
                  csd_ref_date ref_date,
                  sum(csd_trans_amt) trans_amt,
                  count(csd_chq_id) chq_count
          from classic_cash_book_detail_mview
          group by csd_acct_group,csd_lock_box,csd_batch_no,csd_ref_date) det
          where summ.acct_group = det.acct_group(+)
                and summ.lb = det.lb(+)
                and summ.batch_no = det.batch_no(+)
                and summ.ref_date = det.ref_date(+)
                and nvl((-1*summ.trans_amt),0) != nvl(det.trans_amt(+),0)) PAYMENT_BATCH,
                CLASSIC_DAILY_SUMMARY
              WHERE CDS_ACCT_GROUP = PAYMENT_BATCH.ACCT_GROUP
              AND CDS_END_DATE = PAYMENT_BATCH.REF_DATE;


            SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                               'PAYMENT BATCH VALIDATION',
                                               NULL,
                                               To_Char(SYSDATE, 'MMDDYYYY'),
                                               'PAYMENT BATCH VALIDATION PROCESS FINISHED ',
                                               'Basic',
                                               'DAILY');

          EXCEPTION WHEN OTHERS THEN
                                ERR_NUM     := SQLCODE;
                                ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
                                ROLLBACK;
              SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                                               'PAYMENT BATCH VALIDATION',
                                               NULL,
                                               To_Char(SYSDATE, 'MMDDYYYY'),
                                               'PAYMENT BATCH VALIDATION PROCESS IN ERROR '||ERR_MESSAGE,
                                               'Basic',
                                               'DAILY');
      RAISE;
          END;

      BEGIN
        SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                               'UNREALIZED EXCH GAIN VALIDATION',
                               NULL,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'UNREALIZED EXCH GAIN VALIDATION PROCESS STARTED ',
                               'Basic',
                               'DAILY');

       INSERT INTO RECON_IMBALANCE_DETAILS
            (
             RID_ID,
          RID_SUMM_ID,
          RID_ERROR_CODE,
          RID_ERROR_DETAILS,
      RID_LOGGED_BY,
      RID_CREATED_DATE,
      RID_STATUS,
          TIMESTAMP
            )
            SELECT   RECON_IMBALANCE_DET_SEQ.NEXTVAL,
              CDS_RECORD_ID,
              'BL4',
              'BCO : ' ||BCO|| ',BATCH_NO : '|| SCHE_NO || ', SCHED DATE' || SCHE_DATE
              ||', BATCH AMT : '|| BATCH_TRANS_AMT || ', RECEIVED AMOUNT : ' || stg_trans_amt || ', LOADED AMOUNT : ' || inv_trans_amt ||
        ', DIFFERENCE(REALIZED EXCH G/L) AMOUNT : ' || (stg_trans_amt-inv_trans_amt) ||'.','SYSTEM',SYSDATE,'1',SYSDATE
      FROM (select det.acct_group ACCT_GROUP,
           det.ref_date REF_DATE,
           det.bco BCO,
           det.sche_no SCHE_NO,
           det.sche_date SCHE_DATE,
           det.stg_trans_amt,
           det.inv_trans_amt,
           det.amt_797 EXCH_AMT,
           bsje.sche_trans_amt BATCH_TRANS_AMT
        from (select sched_IC_NO_740        bco,
               sched_batch_no_740     sche_no,
               sched_batch_date_740   sche_date,
               sched_tc_740           sche_tc,
               sched_trans_amt_740    sche_trans_amt
            from batch_schedule_journal_entry) bsje,
           (select CBD_ACCT_GROUP    acct_group,
               CBD_BCO           bco,
               CBD_SCHE_NO       sche_no,
               CBD_SCHE_DATE     sche_date,
         SUM(CBD_TRANS_STG_AMT) stg_trans_amt,
         sum(CBD_TRANS_AMT) inv_trans_amt,
               sum(CBD_797_AMT)  amt_797,
               CBD_REF_DATE      ref_date
            from CLASSIC_BILLING_DETAIL_MVIEW
            group by CBD_ACCT_GROUP,CBD_BCO,CBD_SCHE_NO,CBD_SCHE_DATE,CBD_REF_DATE) det
       where bsje.bco(+) = det.bco
         and bsje.sche_no(+) = det.sche_no
         and bsje.sche_date(+) = det.sche_date
         and bsje.sche_tc='797'
         and nvl(det.amt_797,0) != nvl(bsje.sche_trans_amt(+),0)) EXCH_GL,
         CLASSIC_DAILY_SUMMARY
         WHERE CDS_END_DATE = EXCH_GL.REF_DATE
         AND CDS_ACCT_GROUP = EXCH_GL.ACCT_GROUP;


        SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                               'UNREALIZED EXCH GAIN VALIDATION',
                               NULL,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'UNREALIZED EXCH GAIN VALIDATION PROCESS FINIESHED ',
                               'Basic',
                               'DAILY');

      EXCEPTION WHEN OTHERS THEN
            ERR_NUM     := SQLCODE;
            ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
            ROLLBACK;
              SPR_GCARS_BATCHPROCESS_LOG('SPR_CLASSIC_BATCH_IMBALANCE',
                               'UNREALIZED EXCH GAIN VALIDATION',
                               NULL,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'UNREALIZED EXCH GAIN VALIDATION PROCESS IN ERROR '||ERR_MESSAGE,
                               'Basic',
                               'DAILY');
      RAISE;
      END;

COMMIT;
END SPR_CLASSIC_BATCH_IMBALANCE;

CREATE OR REPLACE PROCEDURE SPR_GET_RECON_OPEN_ITEM_REP(IN_ACCT_GROUP    IN VARCHAR2,
                                                      START_DATE       IN VARCHAR2,
                                                      END_DATE         IN VARCHAR2,
                                                      out_message      OUT VARCHAR2,
                                                      OPEN_ITEM_REPORT OUT SYS_REFCURSOR) AS

  REP_DATE VARCHAR2(20);

  ERR_NUM     NUMBER;
  ERR_MESSAGE VARCHAR2(200);
  ERR_MSG     VARCHAR2(200);
  QUERY       VARCHAR2(30000);
  QUERY_START VARCHAR2(2000);
  QUERY_END   VARCHAR2(2000);

BEGIN

  ERR_MSG := '000';

  if START_DATE is not null then
     QUERY_START := 'SELECT TO_CHAR(CDS_START_DATE, ''MMDDYYYYHH24MISS'')
                    FROM CLASSIC_DAILY_SUMMARY
                    WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
                    AND CDS_RECON_DATE = TO_DATE(''' || START_DATE || ''', ''MM/DD/YYYY'')';

      EXECUTE IMMEDIATE QUERY_START into REP_DATE;
  else
      QUERY_END := 'SELECT TO_CHAR(CDS_END_DATE, ''MMDDYYYYHH24MISS'')
                FROM CLASSIC_DAILY_SUMMARY
                WHERE CDS_ACCT_GROUP = ''' || IN_ACCT_GROUP || '''
                AND CDS_RECON_DATE = TO_DATE(''' || END_DATE || ''', ''MM/DD/YYYY'')';

       EXECUTE IMMEDIATE QUERY_END into REP_DATE;
  end if;

  QUERY := 'SELECT COI_ACCT_GROUP "Account Group",
       ''Invoice'' "Item Type",
       COI_IC "Investment Code",
       inv_bill_sche_ic_no_720 "Billing Component",
       null "Batch Number",
       ic_legal_id_702 "Legal ID",
       decode(inv_cost_center_720,null,ic_gl_code_702,inv_cost_center_720) "Cost Center",
       COI_INV_ID "Invoice ID",
       COI_CUST_NO "GECARS Customer Number",
       COI_FINDER_NO "Finder Number",
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Customer Name",
       inv_alias_720 "Business Customer Number",
       inv_ar_type_720 "AR Type",
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(COI_INV_NO),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "Invoice/Cheque Number",
       inv_date_720 "Invoice/Cheque Date",
       inv_due_date_720 "Invoice Due Date",
       COI_INV_AMT "Invoice Amount",
       COI_OUTST_AMT "Invoice Outstanding Amount",
       COI_CURR3 "Deposite Currency",
       COI_CURR "Invoice Crrency",
       COI_TRANS_AMT "Trans Amount",
       COI_OUTST_TRANS_AMT "Trans Outstanding Amount",
       COI_LOC_CURR3 "Local Currency",
       INV_PAYMENT_SCHED_ID_720 "Invoice Payment Schedule ID",
       INV_CLIENT_INV_ID_720 "Client Invoice ID",
       INV_SHIP_COUNTRY_720 "Shipping Country",
       INV_BILL_COUNTRY_720 "Billing Country",
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(INV_PO_NO_720),
                                                                    Chr(10),
                                                                    '' ''),
                                                            Chr(13),
                                                            '' ''),
                                                    Chr(9),
                                                    '' ''),
                                            Chr(126),
                                            ''-''),
                                    ''"'',
                                    '''') "PO Number",
       COI_TRANF_CUST "Transfer Customer Number",
       COI_TRANF_FINDER "Transfer Finder Number",
       DECODE(INV_TYPE_720,
                ''B'',
                ''Billed Invoice'',
                ''C'',
                ''Billed Credit Note'',
                ''N'',
                ''Memo Invoice'',
                ''U'',
                ''Unapplied Cash'',
                ''V'',
                ''Variance (Cash Appln)'',
                ''W'',
                ''Variance(Zero Appln)'',
                ''X'',
                ''Reverse Variance (Cash)'',
                ''Y'',
                ''REVERSE VARIANCE(ZERO)'',
                ''E'',
                ''Charge Entry'',
                ''I'',
                ''Interest Invoice'',
                ''M'',
                ''Miscellaneous'') "Invoice Type",
       COI_INV_INIT_TC "Invoice Initial TC",
       COI_INV_INP_DATE "Invoice Input Date",
       COI_INV_SYS_DATE "Invoice System Date",
     Journal.gl_org_id "Billing Legal ID",
Journal.gl_cost_center "Billing Cost Center",
Journal.gl_acct "Billing GL Account",
Journal.gl_journal_id "Billing ID",
Journal.gl_description "Billing GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
NULL "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
  FROM CLASSIC_DAILY_OI,invoice,customer,investment_code,
  (select crossa_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging,je_assessment_cross_reference
          where cross_summary_id = gl_summary_id
          and cross_detail_id = crossa_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''AC''
          and crossa_table = ''IN''
          and crossa_vat_flag = ''N''
          and gl_tran_date = to_char(last_day(TO_DATE(''' ||
           REP_DATE ||
           ''',''MMDDYYYYHH24MISS'')),''mmddyyyy'')
          ) journal,
          (select crossa_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging,je_assessment_cross_reference
          where cross_summary_id = gl_summary_id
          and cross_detail_id = crossa_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''AC''
          and crossa_table = ''IN''
          and crossa_vat_flag = ''N''
          and gl_tran_date = to_char(last_day( TO_DATE(''' ||
           REP_DATE || ''',''MMDDYYYYHH24MISS'')),''mmddyyyy'')
          ) offset
  where coi_inv_id = inv_id_720
  and inv_cust_no_720 = cust_no_717
  and ic_no_702 = COI_IC
  and inv_id_720 = Journal.crossa_detail_id (+)
  and inv_id_720 = offset.crossa_detail_id (+)
  and COI_ACCT_GROUP in (''' || IN_ACCT_GROUP || ''')
  and COI_REFRESH_DATE = TO_DATE(''' || REP_DATE || ''',''MMDDYYYYHH24MISS'')
  union all
  SELECT CCH_ACCT_GROUP "Account Group",
  ''Uni Cash'' "Item Type",
       CCH_BATCH_IC  "Investment Code",
       CCH_CHQ_LB  "Billing Component",
       CCH_CHQ_BATCH_NO "Batch Number",
       ic_legal_id_702 "Legal ID",
       ic_gl_code_702 "Cost Center",
       CCH_CHQ_ID "Invoice ID",
       NULL "GECARS Customer Number",
       NULL "Finder Number",
       NULL "Customer Name",
       NULL "Business Customer Number",
       NULL "AR Type",
       CCH_CHQ_NO "Invoice/Cheque Number",
       CCH_CHQ_DATE "Invoice/Cheque Date",
       NULL "Invoice Due Date",
       NULL "Invoice/Cheque Amount",
       CCH_CHQ_DEP_AMT "Invoice/Cheque Outstng Amount",
       CCH_DEP_CURR3 "Deposit Currency",
       CCH_CHQ_CURR "Invoice/Cheque Currency",
       NULL "Trans Amount",
       CCH_CHQ_TRANS_AMT "Trans Outstanding Amount",
       CCH_DEP_LOC_CURR3 "Local Currency",
       NULL "Invoice Payment Schedule ID",
       NULL "Client Invoice ID",
       NULL "Shipping Country",
       NULL "Billing Country",
       NULL "PO Number",
       NULL "Transfer Customer Number",
       NULL "Transfer Finder Number",
       ''Unidentified Cash'' "Invoice/Cheque Type",
       cheque_tc_719 "Invoice/Cheque Initial TC",
       batch_je_date_716 "Invoice/Cheque Input Date",
       batch_je_date2_716 "Invoice/Cheque System Date",
     Journal.gl_org_id "Billing Legal ID",
Journal.gl_cost_center "Billing Cost Center",
Journal.gl_acct "Billing GL Account",
Journal.gl_journal_id "Billing ID",
Journal.gl_description "Billing GL Description",
offset.gl_org_id "Offset Legal ID",
Offset.gl_cost_center "Offset Cost Center",
Offset.gl_acct "Offset GL Account",
Offset.gl_journal_id "Offset ID",
Offset.gl_description "Offset GL Description",
batch_je_id_716 "JE ID",
Journal.gl_summary_id "Journal Summary ID",
Offset.gl_summary_id "Offset Summary ID"
  FROM CLASSIC_DAILY_UNI_CHQ,cheque,cheque_batch_header,investment_code,
  (select crossa_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging,je_assessment_cross_reference
          where cross_summary_id = gl_summary_id
          and cross_detail_id = crossa_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''J''
          and cross_table = ''AC''
          and crossa_table = ''CQ''
          and gl_tran_date = to_char(last_day( TO_DATE(''' ||
           REP_DATE ||
           ''',''MMDDYYYYHH24MISS'')),''mmddyyyy'')
          ) journal,
          (select crossa_detail_id,
                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                   gl_summary_id,gl_description
          from je_cross_reference, je_summary_staging,je_assessment_cross_reference
          where cross_summary_id = gl_summary_id
          and cross_detail_id = crossa_summary_id
          and cross_gl_date = gl_date
          and gl_journ_type = ''O''
          and cross_table = ''AC''
          and crossa_table = ''CQ''
          and gl_tran_date = to_char(last_day(TO_DATE(''' ||
           REP_DATE || ''',''MMDDYYYYHH24MISS'')),''mmddyyyy'')
          ) offset
  where cch_chq_id = cheque_id_719
  and cheque_lock_box_719 = batch_lock_box_716
  and cheque_batch_no_719 = batch_no_716
  and ic_no_702 = CCH_BATCH_IC
  and CCH_CHQ_ID = Journal.crossa_detail_id (+)
  and CCH_CHQ_ID = offset.crossa_detail_id (+)
  and CCH_ACCT_GROUP in (''' || IN_ACCT_GROUP || ''')
  and CCH_REFRESH_DATE = TO_DATE(''' || REP_DATE || ''',''MMDDYYYYHH24MISS'')';

  if ERR_MSG = '000' then
      out_message := '000';
      OPEN OPEN_ITEM_REPORT FOR QUERY;
  end if;

EXCEPTION
  WHEN OTHERS THEN
    OPEN OPEN_ITEM_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    ERR_NUM     := SQLCODE;
    ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    out_message := ERR_MESSAGE;

    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_GET_RECON_OPEN_ITEM_REP',
                               IN_ACCT_GROUP,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'GET NEW BILLING REPORT PROCESS FAILED ' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_GET_RECON_OPEN_ITEM_REP;

CREATE OR REPLACE PROCEDURE SPR_JE_ENQUIRY_DETAILS(IN_SUMM_ID IN NUMBER,
                                                   ERR_NUM OUT number,
                                                   ERR_MESSAGE OUT varchar2,
                                                   CUR_REPORT OUT SYS_REFCURSOR) AS
  V_ERR_NUM       number;
  V_ERR_MESSAGE   varchar2(100);
  QUERY           VARCHAR2(10000);
  v_cross_table   je_cross_reference.cross_table%type;
  v_journal_id    je_summary_staging.gl_journal_id%type;

BEGIN
  V_ERR_NUM := 000;
  V_ERR_MESSAGE := '';

select CROSS_TABLE into v_cross_table
       from je_cross_reference
       where cross_summary_id = IN_SUMM_ID
       and rownum < 2;

select gl_journal_id into v_journal_id
       from je_summary_staging
       where gl_summary_id = IN_SUMM_ID;

if v_cross_table = 'BH' then

QUERY := 'select ic_acct_group_702 "Account Group",
                inv_bill_sche_ic_no_720 "Billing Component",
                inv_ic_no_720 "Investment Code",
                ic_legal_id_702 "Legal ID",
                decode(inv_cost_center_720,null,ic_gl_code_702,inv_cost_center_720) "Cost Center",
                BATCH_SCHE_NO_731 "Batch Number",
                inv_input_dt2_720 "Invoice Load Date",
                inv_id_720 "Invoice ID",
                inv_cust_no_720 "GECARS Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Customer Name",
                cust_client_717 "Business Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Invoice Number",
                inv_date_720 "Invoice Date",
                inv_due_date_720 "Invoice Due Date",
                (inv_amt_720/100) "Invoice Amount",
                inv_curr3_720 "Deposit Currency",
                inv_trans_amt_720 "Trans Amount",
                inv_loc_curr3_720 "Local Currency",
                inv_ar_type_720 "AR Type",
                art.other_table_name_794 "AR Type Description",
                inv_init_tc_720 "Invoice TC",
                TC.other_table_name_794 "Invoice TC Description",
                inv_term_720 "Term Code",
                term_long_name_785 "Term Code Description",
                Journal.gl_org_id "Billing Legal ID",
                Journal.gl_cost_center "Billing Cost Center",
                Journal.gl_acct "Billing GL Account",
                Journal.gl_journal_id "Billing ID",
                Journal.gl_description "Billing GL Description",
                offset.gl_org_id "Offset Legal ID",
                Offset.gl_cost_center "Offset Cost Center",
                Offset.gl_acct "Offset GL Account",
                Offset.gl_journal_id "Offset ID",
                Offset.gl_description "Offset GL Description",
                batch_je_id_731 "JE ID",
                Journal.gl_summary_id "Journal Summary ID",
                Offset.gl_summary_id "Offset Summary ID"
                FROM gcars.invoice,
                gcars.batch_schedule_header,
                       gcars.investment_code,
                       gcars.customer,
                                   gcars.other_table art,
                                   terms_code,
                                   gcars.other_table tc,
                                   (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id,gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''J''
                          and cross_table = ''BH''
                          ) journal,
                          (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id, gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''O''
                          and cross_table = ''BH''
                          ) offset
                where inv_bill_sche_ic_no_720 = batch_ic_no_731
                and inv_bill_sche_date_720 = batch_sche_date_731
                and inv_bill_sche_no_720 = batch_sche_no_731
                and batch_je_id_731 = Journal.cross_detail_id
                and batch_je_id_731 = offset.cross_detail_id
                and inv_tranf_flag_720 = ''N''
                and ic_no_702 = BATCH_IC_NO_731
                and art.other_table_type_794=''AR''
                and art.other_table_key_794=inv_ar_type_720
                and term_code_785=inv_term_720
                and tc.other_table_type_794=''TC''
                and tc.other_table_key_794=inv_init_tc_720
                and cust_no_717=inv_cust_no_720
                and batch_je_id_731 in (select CROSS_DETAIL_ID
                                        from je_summary_staging,je_cross_reference
                                        where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                                        and CROSS_TABLE = ''BH''
                                        and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'CH' then

QUERY := 'select lock_box_acct_group_766 "Account Group",
                  batch_ic_no_716 "Investment Code",
                  batch_je_date_716 "Cash Book Date",
                  lock_box_no_766 "Lock Box",
                  lock_box_treasury_cd_766 "Treasury Code",
                  lock_box_bank_name_766 "Bank Name",
                  batch_no_716 "Batch Number",
                  batch_date_716 "Batch Date",
                  pers_SSO_ID_796 "Booked By User SSO ID",
                  pers_name_796 "Booked By User Name",
                  cheque_id_719 "Cheque ID",
                  cheque_no_719 "Cheque Number",
                  cheque_date_719 "Cheque Date",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                                      Chr(10),
                                                                                      '' ''),
                                                                              Chr(13),
                                                                              '' ''),
                                                                      Chr(9),
                                                                      '' ''),
                                                              Chr(126),
                                                              ''-''),
                                                      ''"'',
                                                      '''') "Cheque Name",
                  (cheque_dep_amt_719/100) "Deposit Amount",
                  cheque_dep_curr3_719 "Deposit Currency",
                  cheque_trans_amt_719 "Cheque Trans Amount",
                  cheque_dep_loc_curr3_719 "Local Currency",
                  (CASE WHEN cheque_unident_flag_719 = ''Y'' THEN ''Unidentified''
                       WHEN Cheque_Unappl_sw_719 = ''Y'' THEN ''Unapplied''
                  end) "Payment Application Status",
                  Journal.gl_org_id "Bank Legal ID",
                  Journal.gl_cost_center "Bank Cost Center",
                  Journal.gl_acct "Bank GL Account",
                  Journal.gl_journal_id "Bank ID",
                  Journal.gl_description "Bank GL Description",
                  offset.gl_org_id "Offset Legal ID",
                  Offset.gl_cost_center "Offset Cost Center",
                  Offset.gl_acct "Offset GL Account",
                  Offset.gl_journal_id "Offset ID",
                  Offset.gl_description "Offset GL Description",
                  batch_je_id_716 "JE ID",
                  Journal.gl_summary_id "Journal Summary ID",
                  Offset.gl_summary_id "Offset Summary ID"
                  from cheque,
                  cheque_batch_header,
                  lock_box,
                  personnel,
                  (select cross_detail_id,
                                    gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                     gl_summary_id,gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_id = gl_summary_id
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''J''
                            and cross_table = ''CH''
                            ) journal,
                            (select cross_detail_id,
                                    gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                     gl_summary_id, gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_id = gl_summary_id
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''O''
                            and cross_table = ''CH''
                            ) offset
                  where cheque_batch_no_719 = batch_no_716
                  and cheque_lock_box_719 = batch_lock_box_716
                  and cheque_lock_box_719 = lock_box_no_766
                  and batch_oper_code_716 = pers_code_796(+)
                  and batch_je_id_716 = journal.cross_detail_id
                  and batch_je_id_716 = offset.cross_detail_id
                  and batch_je_id_716 in (select CROSS_DETAIL_ID
                              from je_summary_staging,je_cross_reference
                              where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                              and CROSS_TABLE = ''CH''
                              AND GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'CJ' and v_journal_id = 'AR980' then

QUERY := 'select ''Cash Transfer'' "Transfer Type",
                  substr(cheq_offset_pseu_acct_726, 3, 9) "From Account Group",
                  substr(cheq_offset_pseu_acct_726, 12, 9) "To Account Group",
                  substr(cheq_pseu_acct_726,3,9) "From IC",
                  substr(cheq_pseu_acct_726,12,9) "To IC",
                  je.ic_legal_ID_702 "Legal ID",
                  je.ic_gl_code_702 "Cost Center",
                  cheq_je_date_726 "Transfer Date",
                  cheq_gl_date_726 "GL Date",
                  cheq_tc_726 "Cheque TC",
                  (cheq_amt_726/100) "Transfer Amount",
                  cheque_dep_curr3_719 "Transfer Currency",
                  cheq_trans_amt_726 "Transfer Trans Amt",
                  cheque_dep_loc_curr3_719 "Transfer Local Currency",
                  pers_SSO_ID_796 "Transferred By SSO",
                  pers_name_796 "Transferred By Name",
                  cheq_ID_726 "Cheque ID",
                  cheque_cust_no_719 "GECARS Customer Number",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                      Chr(10),
                                                                                      '' ''),
                                                                              Chr(13),
                                                                              '' ''),
                                                                      Chr(9),
                                                                      '' ''),
                                                              Chr(126),
                                                             ''-''),
                                                      ''"'',
                                                      '''') "Customer Name",
                  cust_client_717 "ERP Customer Number",
                  cheque_no_719 "Cheque Number",
                  cheque_date_719 "Cheque Date",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                                      Chr(10),
                                                                                      '' ''),
                                                                              Chr(13),
                                                                              '' ''),
                                                                      Chr(9),
                                                                      '' ''),
                                                              Chr(126),
                                                              ''-''),
                                                      ''"'',
                                                      '''') "Cheque Name",
                  (cheque_dep_amt_719/100) "Deposit Currency",
                  cheque_dep_curr3_719 "Deposit Currency",
                  cheque_trans_amt_719 "Local Currency",
                  cheque_dep_loc_curr3_719 "Local Currency",
                  Journal.gl_org_ID "DTDF Legal ID",
                  Journal.gl_cost_center "DTDF Cost Center",
                  Journal.gl_acct "DTDF GL Account",
                  Journal.gl_journal_ID "DTDF ID",
                  Journal.gl_description "DTDF GL Description",
                  offset.gl_org_ID "Offset Legal ID",
                  Offset.gl_cost_center "Offset Cost Center",
                  Offset.gl_acct "Offset GL Account",
                  Offset.gl_journal_ID "Offset ID",
                  Offset.gl_description "Offset GL Description",
                  cheq_je_ID_726 "JE ID",
                  Journal.gl_summary_ID "Journal Summary ID",
                  Offset.gl_summary_ID "Offset Summary ID",
                  substr(cheq_offset_pseu_acct_726, 1, 1) "From SOB",
                  substr(cheq_offset_pseu_acct_726, 21, 1) "To SOB"
                  from cheque_journal_entry,cheque,cheque_batch_header,lock_box,investment_code je,customer,
                  personnel,(select cross_detail_ID,
                                    gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                     gl_summary_ID,gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_ID = gl_summary_ID
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''J''
                            and cross_table = ''CJ''
                            ) journal,
                            (select cross_detail_ID,
                                    gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                     gl_summary_ID, gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_ID = gl_summary_ID
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''O''
                            and cross_table = ''CJ''
                            ) offset
                  where cheq_ID_726 = cheque_ID_719
                  and cheq_ic_no_726 = je.ic_no_702
                  and cheque_lock_box_719 = lock_box_no_766
                  and cheque_lock_box_719 = batch_lock_box_716
                  and cheque_batch_no_719 = batch_no_716
                  and cheq_user_id_726= pers_code_796(+)
                  and cheque_cust_no_719 = cust_no_717(+)
                  and cheq_je_ID_726 = Journal.cross_detail_ID
                  and cheq_je_ID_726 = offset.cross_detail_ID
                  and cheq_tc_726 IN (''777'', ''778'', ''771'', ''772'')
                  and cheq_potential_apply_flag_726 = ''N''
                  and cheq_posted_726 in (''N'',''Y'')
                  and cheq_je_id_726 in (select CROSS_DETAIL_ID
                               from je_summary_staging,je_cross_reference
                               where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                               and CROSS_TABLE = ''CJ''
                               and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'IJ' and v_journal_id = 'AR980' then

QUERY := 'select ''Invoice Transfer'' "Transfer Type",
                substr(inv_offset_pseu_acct_727, 3, 9) "From Account Group",
                substr(inv_offset_pseu_acct_727, 12, 9) "From Account Group",
                substr(inv_pseu_acct_727, 3, 9) "From IC",
                substr(inv_pseu_acct_727, 12, 9) "To IC",
                je.ic_legal_ID_702 "Legal ID",
                decode(inv_cost_center_720,null,je.ic_gl_code_702,inv_cost_center_720) "Cost Center",
                inv_je_date_727 "Transfer Date",
                inv_gl_date_727 "GL Date",
                inv_tc_727 "Invoice TC",
                (inv_amt_727/100) "Transfer Amount",
                inv_curr3_720 "Transfer Currency",
                inv_trans_amt_727 "Transfer Trans Amt",
                inv_loc_curr3_720 "Transfer Local Currency",
                pers_SSO_ID_796 "Transferred By SSO",
                pers_name_796 "Transferred By Name",
                inv_ID_720 "Invoice ID",
                inv_cust_no_720 "GECARS Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Customer Name",
                cust_client_717 "ERP Customer Number",
                inv_no_720 "Invoice Number",
                inv_date_720 "Invoice Date",
                (inv_amt_720/100) "Invoice Amount",
                inv_curr3_720 "Invoice Currency",
                inv_trans_amt_720 "Trans Amount",
                inv_loc_curr3_720 "Local Currency",
                Journal.gl_org_ID "DTDF Legal ID",
                Journal.gl_cost_center "DTDF Cost Center",
                Journal.gl_acct "DTDF GL Account",
                Journal.gl_journal_ID "DTDF ID",
                Journal.gl_description "DTDF GL Description",
                offset.gl_org_ID "Offset Legal ID",
                Offset.gl_cost_center "Offset Cost Center",
                Offset.gl_acct "Offset GL Account",
                Offset.gl_journal_ID "Offset ID",
                Offset.gl_description "Offset GL Description",
                inv_je_ID_727 "JE ID",
                Journal.gl_summary_ID "Journal Summary ID",
                Offset.gl_summary_ID "Offset Summary ID",
                substr(inv_offset_pseu_acct_727, 1, 1) "From SOB",
                substr(inv_offset_pseu_acct_727, 21, 1) "To SOB"
                from invoice_journal_entry,invoice,investment_code je,customer,personnel,
                (select cross_detail_ID,
                                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                   gl_summary_ID,gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_ID = gl_summary_ID
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''J''
                          and cross_table = ''IJ''
                          ) journal,
                          (select cross_detail_ID,
                                  gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                   gl_summary_ID, gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_ID = gl_summary_ID
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''O''
                          and cross_table = ''IJ''
                          ) offset
                where inv_owner_cust_no_727 = inv_cust_no_720
                and inv_owner_finder_no_727 = inv_finder_no_720
                and inv_ic_no_727 = je.ic_no_702
                and inv_tc_727 in (''777'', ''778'', ''771'', ''772'')
                and inv_potential_apply_flag_727 = ''N''
                and inv_posted_727 in (''N'',''Y'')
                and inv_cust_no_720 = cust_no_717(+)
                and inv_user_id_727 = pers_code_796(+)
                and inv_je_ID_727 = Journal.cross_detail_ID
                and inv_je_ID_727 = offset.cross_detail_ID
                and inv_je_ID_727 in (select CROSS_DETAIL_ID
                            from je_summary_staging,je_cross_reference
                            where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                            and CROSS_TABLE = ''IJ''
                            and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'BJ' and v_journal_id = 'AR980' then

QUERY := 'select ''Batch Schedule'' "Transfer Type",
                  substr(sched_offset_pseu_acct_740,3,9) "From Account Group",
                  substr(sched_offset_pseu_acct_740,12,9) "To Account Group",
                  sched_je_ic_no_740 "From IC",
                  batch_ic_no_731 "To IC",
                  je.ic_legal_ID_702 "Legal ID",
                  je.ic_gl_code_702 "Cost Center",
                  sched_je_date_740 "Transfer Date",
                  sched_gl_date_740 "GL Date",
                  sched_tc_740 "Schedule TC",
                  (sched_amt_740/100) "Transfer Amount",
                  substr(batch_curr_731,1,3) "Transfer Currency",
                  sched_trans_amt_740 "Transfer Trans Amt",
                  substr(batch_curr_731,4,3) "Transfer Local Currency",
                  pers_SSO_ID_796 "Transferred By SSO",
                  pers_name_796 "Transferred By Name",
                  sched_batch_no_740 "Batch no",
                  SCHED_CUST_NO_740 "GECARS Customer Number",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                      Chr(10),
                                                                                      '' ''),
                                                                              Chr(13),
                                                                              '' ''),
                                                                      Chr(9),
                                                                      '' ''),
                                                              Chr(126),
                                                              ''-''),
                                                      ''"'',
                                                      '''') "Customer Name",
                  cust_client_717 "ERP Customer Number",
                  sched_inv_no_740 "Invoice Number",
                  sched_inv_date_740 "Invoice Date",
                  (batch_amt_731/100) "Batch Amount",
                  substr(batch_curr_731,1,3) "Schedule Currency",
                  batch_trans_amt_731 "Trans Amount",
                  substr(batch_curr_731,4,3) "Local Currency",
                  Journal.gl_org_ID "DTDF Legal ID",
                  Journal.gl_cost_center "DTDF Cost Center",
                  Journal.gl_acct "DTDF GL Account",
                  Journal.gl_journal_ID "DTDF ID",
                  Journal.gl_description "DTDF GL Description",
                  offset.gl_org_ID "Offset Legal ID",
                  Offset.gl_cost_center "Offset Cost Center",
                  Offset.gl_acct "Offset GL Account",
                  Offset.gl_journal_ID "Offset ID",
                  Offset.gl_description "Offset GL Description",
                  sched_je_ID_740 "JE ID",
                  Journal.gl_summary_ID "Journal Summary ID",
                  Offset.gl_summary_ID "Offset Summary ID",
                  substr(sched_offset_pseu_acct_740,1,1) "From SOB",
                  substr(sched_offset_pseu_acct_740,21,1) "To SOB"
                  from batch_schedule_journal_entry,investment_code sc,investment_code je,
                  batch_schedule_header,customer,personnel,
                  (select cross_detail_ID,
                                    gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                     gl_summary_ID,gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_ID = gl_summary_ID
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''J''
                            and cross_table = ''BJ''
                            ) journal,
                            (select cross_detail_ID,
                                    gl_journal_ID, gl_acct, gl_org_ID, gl_cost_center,
                                     gl_summary_ID, gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_ID = gl_summary_ID
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''O''
                            and cross_table = ''BJ''
                            ) offset
                  where sched_batch_no_740 = batch_sche_no_731
                  and sched_ic_no_740 = batch_ic_no_731
                  and sched_batch_date_740 = batch_sche_date_731
                  and sched_user_id_740 = pers_code_796(+)
                  and batch_ic_no_731 = sc.ic_no_702
                  and sched_je_ic_no_740 = je.ic_no_702
                  and sched_tc_740 in (''771'',''772'')
                  and SCHED_CUST_NO_740 = cust_no_717(+)
                  and sched_je_ID_740 = Journal.cross_detail_ID
                  and sched_je_ID_740 = offset.cross_detail_ID
                  and sched_potential_apply_flag_740 = ''N''
                  and sched_posted_740 in (''N'',''Y'')
                  and sched_je_ID_740 in (select CROSS_DETAIL_ID
                              from je_summary_staging,je_cross_reference
                              where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                              and CROSS_TABLE = ''BJ''
                              and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'CJ' and v_journal_id <> 'AR980' then

QUERY := 'select ''Cheque Write Off'' "W/O Type",
                    je.ic_acct_group_702 "Account Group",
                    cheq_ic_no_726 "Investment Code",
                    je.ic_legal_id_702 "Legal ID",
                    je.ic_gl_code_702 "Cost Center",
                    cheq_je_date_726 "Write Off Date",
                    cheq_gl_date_726 "GL Date",
                    cheq_tc_726 "Cheque TC",
                    other_table_name_794 "TC Description",
                    (cheq_amt_726/100) "Write Off Amount",
                    cheque_dep_curr3_719 "Write Off Currency",
                    cheq_trans_amt_726 "Write Off Trans Amount",
                    cheque_dep_loc_curr3_719 "Write Off Local Currency",
                    CHEQ_REMARK_726 "Remarks",
                    pers_SSO_ID_796 "W/O User SSO ID",
                    pers_name_796 "W/O User Name",
                    cheq_id_726 "Cheque ID",
                    cheque_cust_no_719 "GECARS Customer Number",
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                        Chr(10),
                                                                                        '' ''),
                                                                                Chr(13),
                                                                                '' ''),
                                                                        Chr(9),
                                                                        '' ''),
                                                                Chr(126),
                                                                ''-''),
                                                        ''"'',
                                                        '''') "Customer Name",
                    cust_client_717 "ERP Customer Number",
                    cheque_no_719 "Cheque Number",
                    cheque_date_719 "Cheque Date",
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                                        Chr(10),
                                                                                        '' ''),
                                                                                Chr(13),
                                                                                '' ''),
                                                                        Chr(9),
                                                                        '' ''),
                                                                Chr(126),
                                                                ''-''),
                                                        ''"'',
                                                        '''') "Cheque Name",
                    (cheque_dep_amt_719/100) "Cheque Amount",
                    cheque_dep_curr3_719 "Dep Currency",
                    cheque_trans_amt_719 "Trans Amount",
                    cheque_dep_loc_curr3_719 "Local Currency",
                    lock_box_no_766 "Lock box",
                    lock_box_treasury_cd_766 "Treasury Code",
                    lock_box_bank_name_766 "Bank Name",
                    Journal.gl_org_id "W/O Legal ID",
                    Journal.gl_cost_center "W/O Cost Center",
                    Journal.gl_acct "W/O GL Account",
                    Journal.gl_journal_id "W/O ID",
                    Journal.gl_description "W/O GL Description",
                    offset.gl_org_id "Offset Legal ID",
                    Offset.gl_cost_center "Offset Cost Center",
                    Offset.gl_acct "Offset GL Account",
                    Offset.gl_journal_id "Offset ID",
                    Offset.gl_description "Offset GL Description",
                    cheq_je_id_726 "JE ID",
                    Journal.gl_summary_id "Journal Summary ID",
                    Offset.gl_summary_id "Offset Summary ID"
                    from cheque_journal_entry,cheque,lock_box,investment_code je,
                    personnel,customer,other_table,
                    (select cross_detail_id,
                                      gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                       gl_summary_id,gl_description
                              from je_cross_reference, je_summary_staging
                              where cross_summary_id = gl_summary_id
                              and cross_gl_date = gl_date
                              and gl_journ_type = ''J''
                              and cross_table = ''CJ''
                              ) journal,
                              (select cross_detail_id,
                                      gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                       gl_summary_id, gl_description
                              from je_cross_reference, je_summary_staging
                              where cross_summary_id = gl_summary_id
                              and cross_gl_date = gl_date
                              and gl_journ_type = ''O''
                              and cross_table = ''CJ''
                              ) offset
                    where cheq_id_726 = cheque_id_719
                    and cheque_lock_box_719 = lock_box_no_766
                    and cheq_ic_no_726 = je.ic_no_702
                    and cheq_user_id_726 = pers_code_796(+)
                    and cheque_cust_no_719 = cust_no_717(+)
                    and cheq_je_id_726 = Journal.cross_detail_id
                    and cheq_je_id_726 = offset.cross_detail_id
                    and cheq_posted_726 in (''N'',''Y'')
                    and cheq_je_flag_726 != ''C''
                    and cheq_tc_726 not in (''777'',''778'')
                    and other_table_type_794=''TC''
                    and other_table_lang_794=''E''
                    and other_table_key_794=cheq_tc_726
                    and cheq_je_id_726 in (select CROSS_DETAIL_ID
                                 from je_summary_staging,je_cross_reference
                                 where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                                 and CROSS_TABLE = ''CJ''
                                 and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';


elsif v_cross_table = 'IJ' and v_journal_id <> 'AR980' then

QUERY := 'select ''Invoice Write Off'' "W/O Type",
                je.ic_acct_group_702 "Account Group",
                inv_ic_no_727 "Investment Code",
                je.ic_legal_id_702 "Legal ID",
                decode(inv_cost_center_720,null,je.ic_gl_code_702,inv_cost_center_720) "Cost Center",
                inv_je_date_727 "Write Off Date",
                inv_gl_date_727 "GL Date",
                inv_tc_727 "Invoice TC",
                other_table_name_794 "TC Description",
                (inv_amt_727/100) "Write Off Amount",
                inv_curr3_720 "Write Off Currency",
                inv_trans_amt_727 "Write Off Trans Amount",
                inv_loc_curr3_720 "Write Off Local Currency",
                INV_REMARK_727 "Remarks",
                pers_SSO_ID_796 "W/O User SSO ID",
                pers_name_796 "W/O User Name",
                inv_id_720 "Invoice ID",
                inv_cust_no_720 "GECARS Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Customer Name",
                cust_client_717 "ERP Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Invoice Number",
                inv_date_720 "Invoice Date",
                (inv_amt_720/100) "Invoice Amount",
                inv_curr3_720 "Bill Currency",
                inv_trans_amt_720 "Trans Amount",
                inv_loc_curr3_720 "Local Currency",
                ic_no_702 "Investment Code",
                ic_name_702 "Investment Code Name",
                Journal.gl_org_id "W/O Legal ID",
                Journal.gl_cost_center "W/O Cost Center",
                Journal.gl_acct "W/O GL Account",
                Journal.gl_journal_id "W/O ID",
                Journal.gl_description "W/O GL Description",
                offset.gl_org_id "Offset Legal ID",
                Offset.gl_cost_center "Offset Cost Center",
                Offset.gl_acct "Offset GL Account",
                Offset.gl_journal_id "Offset ID",
                Offset.gl_description "Offset GL Description",
                inv_je_id_727 "JE ID",
                Journal.gl_summary_id "Journal Summary ID",
                Offset.gl_summary_id "Offset Summary ID"
                from invoice_journal_entry,invoice,investment_code je,customer,
                personnel,other_table,
                (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id,gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''J''
                          and cross_table = ''IJ''
                          ) journal,
                          (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id, gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''O''
                          and cross_table = ''IJ''
                          ) offset
                where inv_owner_cust_no_727 = inv_cust_no_720
                and inv_owner_finder_no_727 = inv_finder_no_720
                and inv_cust_no_720 = cust_no_717(+)
                and inv_ic_no_727 = je.ic_no_702
                and inv_user_id_727 = pers_code_796(+)
                and inv_je_id_727 = Journal.cross_detail_id
                and inv_je_id_727 = offset.cross_detail_id
                and inv_posted_727 in (''N'',''Y'')
                and inv_je_flag_727 != ''C''
                and inv_tc_727 not in (''777'',''778'',''771'',''772'')
                and other_table_type_794=''TC''
                and other_table_lang_794=''E''
                and other_table_key_794=inv_tc_727
                and inv_je_ID_727 in (select CROSS_DETAIL_ID
                            from je_summary_staging,je_cross_reference
                            where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                            and CROSS_TABLE = ''IJ''
                            and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'BJ' and v_journal_id <> 'AR980' then

QUERY := 'select ''Unrealized Write Off'' "W/O Type",
                je.ic_acct_group_702 "Account Group",
                sched_ic_no_740 "Investment Code",
                je.ic_legal_id_702 "Legal ID",
                je.ic_gl_code_702 "Cost Center",
                sched_je_date_740 "Write Off Date",
                sched_gl_date_740 "GL Date",
                sched_tc_740 "Schedule TC",
                other_table_name_794 "TC Description",
                (sched_amt_740/100) "Write Off Amount",
                substr(SCHED_PSEU_ACCT_740,24,3) "Write Off Currency",
                sched_trans_amt_740 "Write Off Trans Amount",
                substr(SCHED_PSEU_ACCT_740,27,3) "Write Off Local Currency",
                sched_remark_740 "Remarks",
                pers_SSO_ID_796 "W/O User SSO ID",
                pers_name_796 "W/O User Name",
                sched_batch_no_740 "Batch Number",
                sched_batch_date_740 "Batch Date",
                Journal.gl_org_id "W/O Legal ID",
                Journal.gl_cost_center "W/O Cost Center",
                Journal.gl_acct "W/O GL Account",
                Journal.gl_journal_id "W/O ID",
                Journal.gl_description "W/O GL Description",
                offset.gl_org_id "Offset Legal ID",
                Offset.gl_cost_center "Offset Cost Center",
                Offset.gl_acct "Offset GL Account",
                Offset.gl_journal_id "Offset ID",
                Offset.gl_description "Offset GL Description",
                sched_je_ID_740 "JE ID",
                Journal.gl_summary_id "Journal Summary ID",
                Offset.gl_summary_id "Offset Summary ID"
                from batch_schedule_journal_entry,investment_code je,
                personnel,other_table,
                (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id,gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''J''
                          and cross_table = ''BJ''
                          ) journal,
                          (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id, gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''O''
                          and cross_table = ''BJ''
                          ) offset
                where sched_ic_no_740 = je.ic_no_702
                and sched_user_id_740 = pers_code_796(+)
                and sched_je_id_740 = Journal.cross_detail_id
                and sched_je_id_740 = offset.cross_detail_id
                and sched_posted_740 in (''N'',''Y'')
                and sched_je_flag_740 != ''C''
                and sched_tc_740 not in (''777'',''778'',''771'',''772'')
                and other_table_type_794 = ''TC''
                and other_table_lang_794 = ''E''
                and other_table_key_794 = sched_tc_740
                and sched_je_ID_740 in (select CROSS_DETAIL_ID
                                      from je_summary_staging,je_cross_reference
                                      where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                                      and CROSS_TABLE = ''BJ''
                                      and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';

elsif v_cross_table = 'AC' then

QUERY := 'select gl_journal_id "Journal ID",
                   gl_date "GL Date",
                   gl_acct "GL Account",
                   gl_org_id "LEgal ID",
                   gl_cost_center "Cost Center",
                   gl_entered_amount "Base Amount",
                   inv_curr3_720 "Invoice Crrency",
                   NVL(gl_accounted_amount,gl_entered_amount) "Functional Amount",
                   inv_loc_curr3_720 "Local Currency",
                   inv_id_720 "Invoce ID",
                   inv_cust_no_720 "GECARS Customer Number",
                   NVL(inv_alias_720,cust_client_717)"ERP Customer Number",
                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                Chr(10),
                                                                                '' ''),
                                                                        Chr(13),
                                                                        '' ''),
                                                                Chr(9),
                                                                '' ''),
                                                        Chr(126),
                                                        ''-''),
                                                ''"'',
                                                '''') "Customer Name",
                  cust_class_717 "Customer Class",
                  cust_cc_no_717 "CC",
                  cc_manager_coll_name_705 "Collector Name",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                                Chr(10),
                                                                                '' ''),
                                                                        Chr(13),
                                                                        '' ''),
                                                                Chr(9),
                                                                '' ''),
                                                        Chr(126),
                                                        ''-''),
                                                ''"'',
                                                '''') "Invoice Number",
                   inv_date_720 "Invoice Date",
                   inv_due_date_720 "Invoice Due Date",
                   inv_ar_type_720 "Invoice AR Type",
                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(INV_PO_NO_720),
                                                                                Chr(10),
                                                                                '' ''),
                                                                        Chr(13),
                                                                        '' ''),
                                                                Chr(9),
                                                                '' ''),
                                                        Chr(126),
                                                        ''-''),
                                                ''"'',
                                                '''') "Purchase Order Number",
                   inv_req_no_720 "Requisition Number",
                   DECODE(INV_TYPE_720,
                            ''B'',
                            ''Billed Invoice'',
                            ''C'',
                            ''Billed Credit Note'',
                            ''N'',
                            ''Memo Invoice'',
                            ''U'',
                            ''Unapplied Cash'',
                            ''V'',
                            ''Variance (Cash Appln)'',
                            ''W'',
                            ''Variance(Zero Appln)'',
                            ''X'',
                            ''Reverse Variance (Cash)'',
                            ''Y'',
                            ''REVERSE VARIANCE(ZERO)'',
                            ''E'',
                            ''Charge Entry'',
                            ''I'',
                            ''Interest Invoice'',
                            ''M'',
                            ''Miscellaneous'') "Invoice Type",
                   (crossa_amt/100) "Invoice Outstanding Amount",
                   inv_curr3_720 "Invoice Crrency",
                   crossa_trans_amt "Translated Outstanding Amount",
                   inv_loc_curr3_720 "Local Currency",
                   inv_ic_no_720 "Investment Code",
                   inv_cost_center_720 "Cost Center",
                   INV_PAYMENT_SCHED_ID_720 "Payment Schedule ID",
                   INV_CLIENT_INV_ID_720 "Client Invoice ID",
                   gl_summary_id "Journal Summary ID",
                   cross_summary_id "Cross Summary ID",
                   cross_detail_id "Invoice ID"
            from je_cross_reference, je_summary_staging, JE_ASSESSMENT_CROSS_REFERENCE,invoice,customer,cc_organization
            where cross_summary_id = gl_summary_id
            and cross_gl_date = gl_date
            --and gl_journ_type = ''J''
            and cross_table = ''AC''
            and cross_detail_id = crossa_summary_id
            and inv_id_720 = CROSSA_DETAIL_ID
            and inv_cust_no_720 = cust_no_717
            and cc_no_705 = cust_cc_no_717
            and gl_summary_id = ' || IN_SUMM_ID || '';
            
elsif v_cross_table = 'IN' then
              
QUERY := 'select ic_acct_group_702 "Account Group",
                inv_bill_sche_ic_no_720 "Billing Component",
                inv_ic_no_720 "Investment Code",
                ic_legal_id_702 "Legal ID",
                decode(inv_cost_center_720,null,ic_gl_code_702,inv_cost_center_720) "Cost Center",
                inv_input_dt2_720 "Invoice Load Date",
                inv_id_720 "Invoice ID",
                inv_cust_no_720 "GECARS Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cust_long_name_717),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Customer Name",
                cust_client_717 "Business Customer Number",
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(inv_no_720),
                                                                                    Chr(10),
                                                                                    '' ''),
                                                                            Chr(13),
                                                                            '' ''),
                                                                    Chr(9),
                                                                    '' ''),
                                                            Chr(126),
                                                            ''-''),
                                                    ''"'',
                                                    '''') "Invoice Number",
                inv_date_720 "Invoice Date",
                inv_due_date_720 "Invoice Due Date",
                (inv_amt_720/100) "Invoice Amount",
                inv_curr3_720 "Deposit Currency",
                inv_trans_amt_720 "Trans Amount",
                inv_loc_curr3_720 "Local Currency",
                inv_ar_type_720 "AR Type",
                art.other_table_name_794 "AR Type Description",
                inv_init_tc_720 "Invoice TC",
                TC.other_table_name_794 "Invoice TC Description",
                inv_term_720 "Term Code",
                term_long_name_785 "Term Code Description",
                Journal.gl_org_id "Billing Legal ID",
                Journal.gl_cost_center "Billing Cost Center",
                Journal.gl_acct "Billing GL Account",
                Journal.gl_journal_id "Billing ID",
                Journal.gl_description "Billing GL Description",
                offset.gl_org_id "Offset Legal ID",
                Offset.gl_cost_center "Offset Cost Center",
                Offset.gl_acct "Offset GL Account",
                Offset.gl_journal_id "Offset ID",
                Offset.gl_description "Offset GL Description",
                batch_je_id_731 "JE ID",
                Journal.gl_summary_id "Journal Summary ID",
                Offset.gl_summary_id "Offset Summary ID"
                FROM gcars.invoice,
                gcars.batch_schedule_header,
                       gcars.investment_code,
                       gcars.customer,
                                   gcars.other_table art,
                                   terms_code,
                                   gcars.other_table tc,
                                   (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id,gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''J''
                          and cross_table = ''IN''
                          ) journal,
                          (select cross_detail_id,
                                  gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                   gl_summary_id, gl_description
                          from je_cross_reference, je_summary_staging
                          where cross_summary_id = gl_summary_id
                          and cross_gl_date = gl_date
                          and gl_journ_type = ''O''
                          and cross_table = ''IN''
                          ) offset
                where inv_bill_sche_ic_no_720 = batch_ic_no_731
                and inv_bill_sche_date_720 = batch_sche_date_731
                and inv_bill_sche_no_720 = batch_sche_no_731
                and inv_id_720 = Journal.cross_detail_id
                and inv_id_720 = offset.cross_detail_id
                and inv_tranf_flag_720 = ''N''
                and ic_no_702 = BATCH_IC_NO_731
                and art.other_table_type_794=''AR''
                and art.other_table_key_794=inv_ar_type_720
                and term_code_785=inv_term_720
                and tc.other_table_type_794=''TC''
                and tc.other_table_key_794=inv_init_tc_720
                and cust_no_717=inv_cust_no_720
                and inv_id_720 in (select CROSS_DETAIL_ID
                                        from je_summary_staging,je_cross_reference
                                        where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                                        and CROSS_TABLE = ''IN''
                                        and GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';
                                                      
elsif v_cross_table = 'CQ' then

QUERY := 'select lock_box_acct_group_766 "Account Group",
                  batch_ic_no_716 "Investment Code",
                  batch_je_date_716 "Activity Date",
                  lock_box_no_766 "Lock Box",
                  lock_box_treasury_cd_766 "Treasury Code",
                  lock_box_bank_name_766 "Bank Name",
                  batch_no_716 "Batch Number",
                  batch_date_716 "Batch Date",
                  pers_SSO_ID_796 "Booked By User SSO ID",
                  pers_name_796 "Booked By User Name",
                  cheque_id_719 "Cheque ID",
                  cheque_no_719 "Cheque Number",
                  cheque_date_719 "Cheque Date",
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cheque_name_719),
                                                                                      Chr(10),
                                                                                      '' ''),
                                                                              Chr(13),
                                                                              '' ''),
                                                                      Chr(9),
                                                                      '' ''),
                                                              Chr(126),
                                                              ''-''),
                                                      ''"'',
                                                      '''') "Cheque Name",
                  (cheque_dep_amt_719/100) "Deposit Amount",
                  cheque_dep_curr3_719 "Deposit Currency",
                  cheque_trans_amt_719 "Cheque Trans Amount",
                  cheque_dep_loc_curr3_719 "Local Currency",
                  (CASE WHEN cheque_unident_flag_719 = ''Y'' THEN ''Unidentified''
                       WHEN Cheque_Unappl_sw_719 = ''Y'' THEN ''Unapplied''
                  end) "Payment Status",
                  Journal.gl_org_id "Bank Legal ID",
                  Journal.gl_cost_center "Bank Cost Center",
                  Journal.gl_acct "Bank GL Account",
                  Journal.gl_journal_id "Bank ID",
                  Journal.gl_description "Bank GL Description",
                  offset.gl_org_id "Offset Legal ID",
                  Offset.gl_cost_center "Offset Cost Center",
                  Offset.gl_acct "Offset GL Account",
                  Offset.gl_journal_id "Offset ID",
                  Offset.gl_description "Offset GL Description",
                  batch_je_id_716 "JE ID",
                  Journal.gl_summary_id "Journal Summary ID",
                  Offset.gl_summary_id "Offset Summary ID"
                  from cheque,
                  cheque_batch_header,
                  lock_box,
                  personnel,
                  (select cross_detail_id,
                                    gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                     gl_summary_id,gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_id = gl_summary_id
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''J''
                            and cross_table = ''CQ''
                            ) journal,
                            (select cross_detail_id,
                                    gl_journal_id, gl_acct, gl_org_id, gl_cost_center,
                                     gl_summary_id, gl_description
                            from je_cross_reference, je_summary_staging
                            where cross_summary_id = gl_summary_id
                            and cross_gl_date = gl_date
                            and gl_journ_type = ''O''
                            and cross_table = ''CQ''
                            ) offset
                  where cheque_batch_no_719 = batch_no_716
                  and cheque_lock_box_719 = batch_lock_box_716
                  and cheque_lock_box_719 = lock_box_no_766
                  and batch_oper_code_716 = pers_code_796(+)
                  and cheque_id_719 = journal.cross_detail_id
                  and cheque_id_719 = offset.cross_detail_id
                  and cheque_id_719 in (select CROSS_DETAIL_ID
                              from je_summary_staging,je_cross_reference
                              where GL_SUMMARY_ID = CROSS_SUMMARY_ID
                              and CROSS_TABLE = ''CQ''
                              AND GL_SUMMARY_ID = ' || IN_SUMM_ID || ')';          

end if;

  OPEN CUR_REPORT FOR QUERY;
  
  ERR_NUM     := V_ERR_NUM;
  ERR_MESSAGE := V_ERR_MESSAGE;

EXCEPTION
  WHEN OTHERS THEN
    OPEN CUR_REPORT FOR
      SELECT * FROM DUAL WHERE 1 = 2;
    V_ERR_NUM     := SQLCODE;
    V_ERR_MESSAGE := SUBSTR(SQLERRM, 1, 100);
    
    ERR_NUM     := V_ERR_NUM;
    ERR_MESSAGE := V_ERR_MESSAGE;
    
    SPR_GCARS_BATCHPROCESS_LOG('GL_DASHBOARD_GET_REPORTS',
                               'SPR_JE_ENQUIRY_DETAILS',
                               IN_SUMM_ID,
                               To_Char(SYSDATE, 'MMDDYYYY'),
                               'JE ENQUIRY DETAILS' ||
                               ERR_MESSAGE,
                               'Basic',
                               'DAILY');

END SPR_JE_ENQUIRY_DETAILS;

CREATE PUBLIC SYNONYM RECON_CONTROL for GCARS_BATCH.RECON_CONTROL;
GRANT ALL ON GCARS_BATCH.RECON_CONTROL to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM CLASSIC_DAILY_OI for GCARS_BATCH.CLASSIC_DAILY_OI;
GRANT ALL ON GCARS_BATCH.CLASSIC_DAILY_OI to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM CLASSIC_DAILY_UNI_CHQ for GCARS_BATCH.CLASSIC_DAILY_UNI_CHQ;
GRANT ALL ON GCARS_BATCH.CLASSIC_DAILY_UNI_CHQ to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM CLASSIC_DAILY_SUMMARY for GCARS_BATCH.CLASSIC_DAILY_SUMMARY;
GRANT ALL ON GCARS_BATCH.CLASSIC_DAILY_SUMMARY to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM RECON_IMBALANCE_DETAILS for GCARS_BATCH.RECON_IMBALANCE_DETAILS;
GRANT ALL ON GCARS_BATCH.RECON_IMBALANCE_DETAILS to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM RECON_ERROR_MASTER for GCARS_BATCH.RECON_ERROR_MASTER;
GRANT ALL ON GCARS_BATCH.RECON_ERROR_MASTER to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM RECON_STATUS_MASTER for GCARS_BATCH.RECON_STATUS_MASTER;
GRANT ALL ON GCARS_BATCH.RECON_STATUS_MASTER to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM recon_imbalance_tracker for GCARS_BATCH.recon_imbalance_tracker;
GRANT ALL ON GCARS_BATCH.recon_imbalance_tracker to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM PKG_GL_DASHBOARD_DAILY_REFRESH for GCARS_BATCH.PKG_GL_DASHBOARD_DAILY_REFRESH ;
GRANT ALL ON GCARS_BATCH.PKG_GL_DASHBOARD_DAILY_REFRESH to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_RECON_CONTROL_UPDATE for GCARS_BATCH.SPR_RECON_CONTROL_UPDATE ;
GRANT ALL ON GCARS_BATCH.SPR_RECON_CONTROL_UPDATE to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_DAILY_SUMMARY_REFRESH  for GCARS_BATCH.SPR_DAILY_SUMMARY_REFRESH ;
GRANT ALL ON GCARS_BATCH.SPR_DAILY_SUMMARY_REFRESH  to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_DAILY_OI_REFRESH   for GCARS_BATCH.SPR_DAILY_OI_REFRESH  ;
GRANT ALL ON GCARS_BATCH.SPR_DAILY_OI_REFRESH   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GET_RECON_BILLING_REP   for GCARS_BATCH.SPR_GET_RECON_BILLING_REP  ;
GRANT ALL ON GCARS_BATCH.SPR_GET_RECON_BILLING_REP   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GET_RECON_WRITEOFF_REP   for GCARS_BATCH.SPR_GET_RECON_WRITEOFF_REP  ;
GRANT ALL ON GCARS_BATCH.SPR_GET_RECON_WRITEOFF_REP   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GET_RECON_TRANSFER_REP   for GCARS_BATCH.SPR_GET_RECON_TRANSFER_REP  ;
GRANT ALL ON GCARS_BATCH.SPR_GET_RECON_TRANSFER_REP   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GET_RECON_CASH_BOOK_REP   for GCARS_BATCH.SPR_GET_RECON_CASH_BOOK_REP  ;
GRANT ALL ON GCARS_BATCH.SPR_GET_RECON_CASH_BOOK_REP   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GCARS_OI_WALK_SEARCH   for GCARS_BATCH.SPR_GCARS_OI_WALK_SEARCH  ;
GRANT ALL ON GCARS_BATCH.SPR_GCARS_OI_WALK_SEARCH   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GCARS_JE_ENQUIRY_SEARCH   for GCARS_BATCH.SPR_GCARS_JE_ENQUIRY_SEARCH  ;
GRANT ALL ON GCARS_BATCH.SPR_GCARS_JE_ENQUIRY_SEARCH   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_CLASSIC_BATCH_IMBALANCE   for GCARS_BATCH.SPR_CLASSIC_BATCH_IMBALANCE  ;
GRANT ALL ON GCARS_BATCH.SPR_CLASSIC_BATCH_IMBALANCE   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_GET_RECON_OPEN_ITEM_REP   for GCARS_BATCH.SPR_GET_RECON_OPEN_ITEM_REP  ;
GRANT ALL ON GCARS_BATCH.SPR_GET_RECON_OPEN_ITEM_REP   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_JE_ENQUIRY_DETAILS   for GCARS_BATCH.SPR_JE_ENQUIRY_DETAILS  ;
GRANT ALL ON GCARS_BATCH.SPR_JE_ENQUIRY_DETAILS   to GCARS_WEB, GCARS;

CREATE PUBLIC SYNONYM SPR_JE_ENQUIRY_DETAILS   for GCARS_BATCH.SPR_JE_ENQUIRY_DETAILS  ;
GRANT ALL ON GCARS_BATCH.SPR_JE_ENQUIRY_DETAILS   to GCARS_WEB, GCARS;

