-- Removing duplicates on basis of sample_order_transaction_id,ship_date,item_status,track_no

spark2-shell --queue atlas --driver-memory 20G --num-executors 20 --executor-memory 20G --conf spark.driver.maxResultSize=3G   
spark.sparkContext.setLogLevel("ERROR")
spark2-shell --queue atlas --driver-memory 15G --executor-memory 15G --conf spark.driver.maxResultSize=10G 

-- backup

spark.sql(" create table all_all_r_all_bkp.ods_smpl_req_trsn_bkp_16oct2022 like all_all_b_usa_crmods.ods_smpl_req_trsn")
spark.sql("insert into all_all_r_all_bkp.ods_smpl_req_trsn_bkp_16oct2022 select * from all_all_b_usa_crmods.ods_smpl_req_trsn")

spark.sql("create table if not exists all_all_r_all_bkp.ods_smpl_req_trsn_t  like all_all_b_usa_crmods.ods_smpl_req_trsn")
spark.sql("truncate table all_all_r_all_bkp.ods_smpl_req_trsn_t")

spark.sql("""with dup_rec as (select sample_order_transaction_id,count(1) from all_all_b_usa_crmods.ods_smpl_req_trsn group by sample_order_transaction_id having count(1)>1), dice_ods as (select ods.sample_order_transaction_id,ods.ship_date,ods.item_status,ods.rec_modify_date,veeva.id,veeva.shipment_date__c,veeva.item_status__c,ods.odw_id,ods.track_no,ods.batch_id_update from all_all_b_usa_crmods.ods_smpl_req_trsn ods  join ph_com_p_usa_veeva.sample_order_transaction_vod__c veeva on veeva.id=ods.sample_order_transaction_id and nvl(veeva.shipment_date__c,'@')=nvl(ods.ship_date,'@') and nvl(upper(ods.item_status),'@')=nvl(upper(veeva.item_status__c),'@') and nvl(ods.track_no,'@')=nvl(veeva.tracking_number__c,'@') where ods.sample_order_transaction_id in (select sample_order_transaction_id from dup_rec)),id_ship as (select sample_order_transaction_id,odw_id,batch_id_update,ship_date,item_status,track_no,row_number() over(partition by sample_order_transaction_id,ship_date,item_status,track_no order by odw_id desc) as rn from dice_ods), ods_data as ( select ods.* from all_all_b_usa_crmods.ods_smpl_req_trsn ods join id_ship tmp on ods.sample_order_transaction_id=tmp.sample_order_transaction_id and nvl(ods.ship_date,'@')=tmp.ship_date and ods.item_status=tmp.item_status and ods.odw_id=tmp.odw_id and nvl(ods.track_no,'@')=nvl(tmp.track_no,'@') and nvl(ods.batch_id_update,0)=nvl(tmp.batch_id_update,0) and nvl(ods.item_status,'@')=nvl(tmp.item_status,'@') where tmp.rn=1)

insert into all_all_r_all_bkp.ods_smpl_req_trsn_t select * from ods_data""")



spark.sql("insert into all_all_r_all_bkp.ods_smpl_req_trsn_t select ods.* from all_all_b_usa_crmods.ods_smpl_req_trsn ods left anti join all_all_r_all_bkp.ods_smpl_req_trsn_t tmp on ods.sample_order_transaction_id=tmp.sample_order_transaction_id")

spark.sql("truncate table all_all_b_usa_crmods.ods_smpl_req_trsn")

spark.sql("insert into all_all_b_usa_crmods.ods_smpl_req_trsn select * from all_all_r_all_bkp.ods_smpl_req_trsn_t")

