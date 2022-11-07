

--backup

spark.sql("""create table all_all_r_all_bkp.ods_child_acct_16thOct like all_all_b_usa_crmods.ods_child_acct""")
spark.sql("""insert into all_all_r_all_bkp.ods_child_acct_16thOct select * from all_all_b_usa_crmods.ods_child_acct""")


-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_child_acct_t like ALL_ALL_B_USA_CRMODS.ods_child_acct")
spark.sql("truncate table all_all_r_all_bkp.ods_child_acct_t")

-- removing duplicates on basis of  child_record_type_vod__c and parent_record_type_vod__c

spark.sql("""with dup_rec as (select child_account,parent_account,rel_type,count(1) from all_all_b_usa_crmods.ods_child_acct group by child_account,parent_account,rel_type having count(1) > 1),rec_type as (select  ods.* from all_all_b_usa_crmods.ods_child_acct ods join dup_rec d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type where ods.parent_record_type_vod__c is not null or ods.child_record_type_vod__c is not null)

insert into all_all_r_all_bkp.ods_child_acct_t (select * from rec_type)""")

spark.sql("insert into all_all_r_all_bkp.ods_child_acct_t select ods.* from all_all_b_usa_crmods.ods_child_acct ods left anti join all_all_r_all_bkp.ods_child_acct_t d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type")

spark.sql("truncate table all_all_b_usa_crmods.ods_child_acct")

spark.sql("insert into all_all_b_usa_crmods.ods_child_acct select * from  all_all_r_all_bkp.ods_child_acct_t")




-- VALIDATION ( do not run while executing SOP) -----------------------------------------------


select child_account,parent_account,rel_type,count(1) from all_all_b_usa_crmods.ods_child_acct group by child_account,parent_account,rel_type having count(1) > 1

select * from all_all_b_usa_crmods.ods_child_acct where child_account = '33801100' and parent_account ='1804764' and rel_type ='HCP_to_UNIV'

select * from all_all_b_usa_crmods.ods_child_acct where trim(child_account) = '43690461' and trim(parent_account) ='2113139' and rel_type ='HCP_to_HOSP'

with dup_rec as (select child_account,parent_account,rel_type,count(1) from all_all_b_usa_crmods.ods_child_acct group by child_account,parent_account,rel_type having count(1) > 1)

select distinct ods.sf_id,d.parent_account,d.child_account,d.rel_type from all_all_b_usa_crmods.ods_child_acct ods join dup_rec d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type

invalidate metadata singhp2j.ods_child_acct_t

select child_account,parent_account,rel_type,count(1) from singhp2j.ods_child_acct_t group by child_account,parent_account,rel_type having count(1) > 1

select * from singhp2j.ods_child_acct_t where trim(child_account) = '43690461' and trim(parent_account) ='2113139' and rel_type ='HCP_to_HOSP'

select * from all_all_b_usa_crmods.ods_child_acct where trim(child_account) = '33801100' and trim(parent_account) ='1804764' and rel_type ='HCP_to_UNIV'