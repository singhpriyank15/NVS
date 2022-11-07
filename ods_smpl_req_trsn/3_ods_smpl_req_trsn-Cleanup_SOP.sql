-- Remove duplicate on basis of sample_order_transaction_id,ship_date



spark2-shell --queue atlas --driver-memory 20G --num-executors 20 --executor-memory 20G --conf spark.driver.maxResultSize=3G   
spark.sparkContext.setLogLevel("ERROR")
spark2-shell --queue atlas --driver-memory 15G --executor-memory 15G --conf spark.driver.maxResultSize=10G 

-- backup

spark.sql(" create table all_all_r_all_bkp.ods_smpl_req_trsn_bkp_3 like all_all_b_usa_crmods.ods_smpl_req_trsn")
spark.sql("insert into all_all_r_all_bkp.ods_smpl_req_trsn_bkp_3 select * from all_all_b_usa_crmods.ods_smpl_req_trsn")

spark.sql("create table if not exists all_all_r_all_bkp.ods_smpl_req_trsn_t  like all_all_b_usa_crmods.ods_smpl_req_trsn")
spark.sql("truncate table all_all_r_all_bkp.ods_smpl_req_trsn_t")

spark.sql("""with dup_rec as (select sample_order_transaction_id,count(1) from all_all_b_usa_crmods.ods_smpl_req_trsn group by sample_order_transaction_id having count(1)>1),dice_ods as (select ods.sample_order_transaction_id,ods.ship_date,ods.item_status,ods.rec_modify_date,veeva.id,veeva.shipment_date__c,veeva.item_status__c,ods.odw_id,ods.track_no,ods.batch_id_update from all_all_b_usa_crmods.ods_smpl_req_trsn ods join ph_com_p_usa_veeva.sample_order_transaction_vod__c veeva on veeva.id=ods.sample_order_transaction_id where ods.sample_order_transaction_id in (select sample_order_transaction_id from dup_rec)), id_ship as (select sample_order_transaction_id,odw_id,batch_id_update,row_number() over(partition by sample_order_transaction_id order by odw_id desc) as rn from dice_ods ),ods_data as ( select ods.* from all_all_b_usa_crmods.ods_smpl_req_trsn ods join id_ship tmp on ods.sample_order_transaction_id=tmp.sample_order_transaction_id and ods.odw_id=tmp.odw_id and nvl(ods.batch_id_update,0)=nvl(tmp.batch_id_update,0) where tmp.rn=1)

insert into all_all_r_all_bkp.ods_smpl_req_trsn_t select od.sample_request_order_name,od.call_date,od.acct_id,od.mdm_id,od.ship_phone,od.degree,od.specialty,od.owner_id,od.rep_phone,od.product,od.shppng_addr,od.rec_typ_id,od.ndc_code,od.admin_stat,od.product_id,od.odw_id,od.sample_order_transaction_id,od.order_name,od.nov_id,od.hcp_first_name,od.hcp_last_name,od.terr_id,od.emp_no,od.rep_fnm,od.rep_lnm,od.requested_samples,od.quantity,od.ship_addr_line1_hcp,od.ship_addr_line2_hcp,od.ship_city_hcp,od.ship_state_hcp,od.ship_zip,od.ship_addr_line1_rep,od.ship_addr_line2_rep,od.ship_city_rep,od.ship_state_rep,od.ship_zip_rep,od.sln_no,od.ship_to_hcp,od.call_sample_id,veeva.item_status__c item_status,veeva.item_status_reason__c status_reason,veeva.tracking_number__c track_no,od.return_quantity,od.create_date,od.last_modified_date,od.created_by_id,od.last_modified_by_id,od.batch_id_insert,od.rec_insert_by,od.rec_insert_date,od.rec_modify_date,od.ship_date,od.internal_order_no,od.sample_request_id,od.ship_to_pick,od.batch_id_update,od.rec_modify_by from ods_data od left join ph_com_p_usa_veeva.sample_order_transaction_vod__c veeva on veeva.id=od.sample_order_transaction_id""")


spark.sql("insert into all_all_r_all_bkp.ods_smpl_req_trsn_t select ods.* from all_all_b_usa_crmods.ods_smpl_req_trsn ods left anti join all_all_r_all_bkp.ods_smpl_req_trsn_t tmp on ods.sample_order_transaction_id=tmp.sample_order_transaction_id")

spark.sql("truncate table all_all_b_usa_crmods.ods_smpl_req_trsn")

spark.sql("insert into all_all_b_usa_crmods.ods_smpl_req_trsn select distinct * from all_all_r_all_bkp.ods_smpl_req_trsn_t")



