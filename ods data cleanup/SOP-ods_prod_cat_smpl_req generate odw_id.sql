-- BACKUP


spark.sql("""create table all_all_r_all_bkp.ods_prod_cat_smpl_req_12ndOct22 like all_all_b_usa_crmods.ods_prod_cat_smpl_req""")
spark.sql("""insert into all_all_r_all_bkp.ods_prod_cat_smpl_req_12ndOct22 select * from all_all_b_usa_crmods.ods_prod_cat_smpl_req""")


spark.sql("""create table if not exists singhp2j.ods_prod_cat_smpl_req_t like all_all_b_usa_crmods.ods_prod_cat_smpl_req""")
spark.sql(""" truncate table all_all_r_all_bkp.ods_prod_cat_smpl_req_t""")


spark.sql(""" with odwid as (select *,row_number() over(order by id) as rnk from all_all_b_usa_crmods.ods_prod_cat_smpl_req),final_data as (select ods.id,ods.owner_id,ods.is_deleted,ods.name,ods.create_date,ods.created_by_id,ods.last_modified_date,ods.last_modified_by_id,ods.system_mod_stamp,ods.may_edit,ods.is_locked,ods.consumer_site__c,ods.product_info__c,ods.therapeutic_class_vod__c,ods.parent_product_vod__c,ods.therapeutic_area_vod__c,ods.product_type_vod__c,ods.no_metrics_vod__c,ods.cost_vod__c,ods.external_id_vod__c,ods.manufacturer_vod__c,ods.company_product_vod__c,ods.controlled_substance_vod__c,ods.description_vod__c,ods.sample_qty_picklist_vod__c,ods.display_order_vod__c,ods.require_key_message_vod__c,ods.product_catalog_odw__c,ods.distributor_vod__c,ods.sample_quantity_bound_vod__c,ods.sample_u_m_vod__c,o.rnk odw_id,ods.product_description__c,ods.dtp__c,ods.batch_id_insert,ods.batch_id_update,ods.rec_insert_by,ods.rec_insert_date,ods.rec_modify_by,ods.rec_modify_date from all_all_b_usa_crmods.ods_prod_cat_smpl_req ods  join odwid o on o.id=ods.id)

insert into singhp2j.ods_prod_cat_smpl_req_t select * from final_data""")


spark.sql("""truncate table all_all_b_usa_crmods.ods_prod_cat_smpl_req""")
spark.sql("""insert into all_all_b_usa_crmods.ods_prod_cat_smpl_req select * from all_all_r_all_bkp.ods_prod_cat_smpl_req_t""")







