--backup

spark.sql("""create table all_all_r_all_bkp.ods_child_acct_10thOct_2 like all_all_b_usa_crmods.ods_child_acct""")
spark.sql("""insert into all_all_r_all_bkp.ods_child_acct_10thOct_2 select * from all_all_b_usa_crmods.ods_child_acct""")


-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_child_acct_t like all_all_b_usa_crmods.ods_child_acct")
spark.sql("truncate table all_all_r_all_bkp.ods_child_acct_t")

-- removing duplicates on basis of  child_record_type_vod__c and parent_record_type_vod__c & NAME

spark.sql("""with dup_rec as (select child_account,parent_account,rel_type,count(1) from all_all_b_usa_crmods.ods_child_acct group by child_account,parent_account,rel_type having count(1) > 1), sfid as (select  ods.* from all_all_b_usa_crmods.ods_child_acct ods join dup_rec d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type where  ods.child_acc_sf_id is not null and ods.parent_acc_sf_id is not null),name_rec as ( select * from  sfid where name is not null)

insert into all_all_r_all_bkp.ods_child_acct_t (select a.* from sfid a left anti join name_rec d on trim(a.child_account) = trim(d.child_account) and trim(a.parent_account) = trim(d.parent_account) and a.rel_type=d.rel_type union select * from name_rec )""")

spark.sql("insert into all_all_r_all_bkp.ods_child_acct_t select ods.* from all_all_b_usa_crmods.ods_child_acct ods left anti join all_all_r_all_bkp.ods_child_acct_t d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type")


spark.sql("truncate table all_all_b_usa_crmods.ods_child_acct")

spark.sql("insert into all_all_b_usa_crmods.ods_child_acct select * from  all_all_r_all_bkp.ods_child_acct_t")

