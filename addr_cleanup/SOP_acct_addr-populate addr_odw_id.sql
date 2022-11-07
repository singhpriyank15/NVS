

spark.sparkContext.setLogLevel("ERROR")

spark.sql("""create table all_all_r_all_bkp.ods_acct_addr_10thOct like all_all_b_usa_crmods.ods_acct_addr""")
spark.sql("""insert into all_all_r_all_bkp.ods_acct_addr_10thOct select * from all_all_b_usa_crmods.ods_acct_addr""")


-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_acct_addr_t like ALL_ALL_B_USA_CRMODS.ods_acct_addr")
spark.sql("truncate table all_all_r_all_bkp.ods_acct_addr_t")


spark.sql("""with req_data as (select *,row_number() over(order by rec_modify_date) as rn from all_all_b_usa_crmods.ods_acct_addr where acct_addr_odw_id is null), max_odw_id as (select max(acct_addr_odw_id) as max_id,'1' as dummy from all_all_b_usa_crmods.ods_acct_addr), final_data as (select acct_phys_addr_line_3,acct_phys_addr_line_2,acct_phys_addr_line_1,mdm_valid_addr_ind,home,inc_in_terr_asgnmt,primary,shipping,mailing,acct_name,acct_odw_id,cam_acct_key,sf_owner_id,sf_is_deleted,sf_id,(rn + b.max_id) acct_addr_odw_id,best_times,cass_certified,mdm_src_addr_type,mdm_cust_skey,mdm_addr_src_sys,edge_id,mdm_addr_id,billing,sonic_id,business,cust_addr_skey,acct_phys_zip2,acct_phys_zip1,batch_id_insert,rec_insert_by,rec_modify_date,rec_insert_date,inactive,office_notes,staff_notes,source,acct_fax_2,acct_fax_1,acct_phn_2,acct_phn_1,zip_4,cntry_code,acct_post_zip2,acct_post_zip1,acct_post_state,acct_post_county,acct_post_country,acct_post_city,acct_post_addr_line3,acct_post_addr_line2,acct_post_addr_line1,acct_phys_zip,acct_phys_state,acct_phys_county,acct_phys_country,acct_phys_city,siebel_id,batch_id_update,rec_modify_by,appt_required_vod__c,nvs_core_secondary_license__c,maps,sample_status_vod__c,sample_send_status_vod__c,nvs_core_novartis_unique_id__c from req_data a,max_odw_id b where a.sf_id<>b.dummy )

insert into all_all_r_all_bkp.ods_acct_addr_t select * from final_data""")


spark.sql("insert into all_all_r_all_bkp.ods_acct_addr_t select ods.* from all_all_b_usa_crmods.ods_acct_addr ods left anti join all_all_r_all_bkp.ods_acct_addr_t tmp on ods.sf_id=tmp.sf_id")

spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_acct_addr")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_acct_addr select * from  all_all_r_all_bkp.ods_acct_addr_t")




-- validation  (NO NEED TO EXECUTE, JUST FOR TESTING)

'a015Y00001CL9eIQAT','a011O00000m5fGxQAI'
select acct_addr_odw_id from all_all_b_usa_crmods.ods_acct_addr where sf_id in ('a015Y00001CL9eIQAT','a011O00000m5fGxQAI')


with null_id as (select sf_id from all_all_b_usa_crmods.ods_acct_addr where acct_addr_odw_id is null), veeva_data as ( select veeva.isdeleted,address_odw__c from ph_com_p_usa_veeva.address_vod__c veeva join null_id d on d.sf_id=veeva.id)

select * from veeva_data 






