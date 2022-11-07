--backup

spark.sql("""create table all_all_r_all_bkp.ods_child_acct_10thOct_3 like all_all_b_usa_crmods.ods_child_acct""")
spark.sql("""insert into all_all_r_all_bkp.ods_child_acct_10thOct_3 select * from all_all_b_usa_crmods.ods_child_acct""")


-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_child_acct_t like all_all_b_usa_crmods.ods_child_acct")
spark.sql("truncate table all_all_r_all_bkp.ods_child_acct_t")

-- removing duplicates on rec_modify_date

spark.sql("""with dup_rec as (select *,row_number() over(partition by child_account,parent_account,rel_type order by rec_modify_date desc) as rn from all_all_b_usa_crmods.ods_child_acct), final_data as (select  name,child_account,child_acc_sf_id,parent_account,parent_acc_sf_id,primary,role,start_date,end_date,batch_id_insert,batch_id_update,rec_insert_by,rec_insert_date,rec_modify_by,rec_modify_date,sf_id,rel_type,status,child_record_type_vod__c,parent_record_type_vod__c,primary_vod__c from dup_rec where rn = 1)

insert into all_all_r_all_bkp.ods_child_acct_t select * from final_data""")

spark.sql("insert into all_all_r_all_bkp.ods_child_acct_t select ods.* from all_all_b_usa_crmods.ods_child_acct ods left anti join all_all_r_all_bkp.ods_child_acct_t d on trim(ods.child_account) = trim(d.child_account) and trim(ods.parent_account) = trim(d.parent_account) and ods.rel_type=d.rel_type")


spark.sql("truncate table all_all_b_usa_crmods.ods_child_acct")

spark.sql("insert into all_all_b_usa_crmods.ods_child_acct select * from  all_all_r_all_bkp.ods_child_acct_t")



