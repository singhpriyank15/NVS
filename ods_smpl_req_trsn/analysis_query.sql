sample_order_transaction_id,ship_date,item_status,track_no
id,shipment_date__c,item_status__c,tracking_number__c

select * from  ph_com_p_usa_veeva.sample_order_transaction_vod__c where id = 'a1m5Y0000041C9PQAU'

select sample_order_transaction_id,ship_date,* from all_all_b_usa_crmods.ods_smpl_req_trsn where sample_order_transaction_id = 'a1m5Y0000041ClsQAE'

select sample_order_transaction_id,ship_date,count(1) from all_all_b_usa_crmods.ods_smpl_req_trsn group by sample_order_transaction_id,ship_date having count(1) > 1

with dup_rec as (select sample_order_transaction_id,count(1) from all_all_b_usa_crmods.ods_smpl_req_trsn group by sample_order_transaction_id having count(1)>1), dice_ods as (select ods.sample_order_transaction_id,ods.ship_date,ods.item_status,ods.rec_modify_date,veeva.id,veeva.shipment_date__c,veeva.item_status__c,ods.odw_id,ods.track_no,ods.batch_id_update from all_all_b_usa_crmods.ods_smpl_req_trsn ods left join ph_com_p_usa_veeva.sample_order_transaction_vod__c veeva on veeva.id=ods.sample_order_transaction_id where ods.sample_order_transaction_id in (select sample_order_transaction_id from dup_rec)),id_ship as (select od.sample_order_transaction_id,od.ship_date,od.item_status,od.track_no,max(od.batch_id_update) batch_id,max(od.odw_id) dis_odw_id from dice_ods od join ph_com_p_usa_veeva.sample_order_transaction_vod__c veeva   on veeva.id=od.sample_order_transaction_id and nvl(veeva.shipment_date__c,'@')=nvl(od.ship_date,'@') and nvl(od.item_status,'@')=nvl(veeva.item_status__c,'@') and nvl(od.track_no,'@')=nvl(veeva.tracking_number__c,'@') group by od.sample_order_transaction_id,od.ship_date,od.item_status,od.track_no), ods_data as ( select ods.* from all_all_b_usa_crmods.ods_smpl_req_trsn ods join id_ship tmp on ods.sample_order_transaction_id=tmp.sample_order_transaction_id and nvl(ods.ship_date,'@')=tmp.ship_date and ods.item_status=tmp.item_status and ods.odw_id=tmp.dis_odw_id and ods.batch_id_update=tmp.batch_id and nvl(ods.item_status,'@')=nvl(tmp.item_status,'@'))

select * from ods_data

select sample_order_transaction_id,ship_date,item_status,rec_modify_date,odw_id,row_number over(partition by sample_order_transaction_id,ship_date,item_status order by rec_modify_date desc) as rn)




od.sample_request_order_name,od.call_date,od.acct_id,od.mdm_id,od.ship_phone,od.degree,od.specialty,od.owner_id,od.rep_phone,od.product,od.shppng_addr,od.rec_typ_id,od.ndc_code,od.admin_stat,od.product_id,od.odw_id,od.sample_order_transaction_id,od.order_name,od.nov_id,od.hcp_first_name,od.hcp_last_name,od.terr_id,od.emp_no,od.rep_fnm,od.rep_lnm,od.requested_samples,od.quantity,od.ship_addr_line1_hcp,od.ship_addr_line2_hcp,od.ship_city_hcp,od.ship_state_hcp,od.ship_zip,od.ship_addr_line1_rep,od.ship_addr_line2_rep,od.ship_city_rep,od.ship_state_rep,od.ship_zip_rep,od.sln_no,od.ship_to_hcp,od.call_sample_id,od.item_status,od.status_reason,od.track_no,od.return_quantity,od.create_date,od.last_modified_date,od.created_by_id,od.last_modified_by_id,od.batch_id_insert,od.rec_insert_by,od.rec_insert_date,od.rec_modify_date,od.ship_date,od.internal_order_no,od.sample_request_id,od.ship_to_pick,od.batch_id_update,od.rec_modify_by
