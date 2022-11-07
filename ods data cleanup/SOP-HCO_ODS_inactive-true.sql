
spark.sql("""create table all_all_r_all_bkp.ods_site_2ndOct22 like all_all_b_usa_crmods.ods_site""")
spark.sql("""insert into all_all_r_all_bkp.ods_site_2ndOct22 select * from all_all_b_usa_crmods.ods_site""")


spark.sql("""create table if not exists all_all_r_all_bkp.ods_site_t like all_all_b_usa_crmods.ods_site""")
spark.sql(""" truncate table all_all_r_all_bkp.ods_site_t""")



-

spark.sql("""insert into all_all_r_all_bkp.ods_site_t select ff_department,cpf_elgbl,ms_kam_acct_tier,payer_id,description,cln_rsrch_capable,heor_rsrch_capable,cln_rsrch_active,mangd_market_segmnt,heor_rsrch_active,org_strata_level,center_of_excell,mangd_market_phy_grp,created_by_id,last_modified_id,create_date,last_modified_date,siebel_id,midb_site_row_id,addr_row_id,sonic_create_date,sonic_org_type,comments,site_resident_program_flag,site_resident_count,site_mpn,site_mpn_order,site_physician_key,site_main_location_flag,mdm_party_id,edge_id,owner_dept,usmm_kam,tot_rx_lives,pbm_crv_out,spclty_phrmcy,case_mgr,num_us_emp,ebc,chn_fclty_phrmcy_prvdr,tot_fclts,med_d_beds,parta_beds,phrmcy_type,lvl,comm_cntrct_date,comm_cntrct_stat,medcr_partd_cntrt_date,medcr_partd_cntrt_stat,medcd_partd_cntrt_date,medcd_partd_cntrt_stat,tot_covd_lives,self_insurd,ful_insurd,trust,top5_geo,comm_drug_trnd_prcnt,spclty_pmpm,spclty_drug_cntr_prcnt,tot_sls,spclty_tot_prcnt_sls,rx_sls,tot_nvs_sls,nvs_rtns,nvs_rtns_prcnt,top5_nvs_prod,invtry_mnths_on_hnd,invtry_as_of_date,ex_dspns_rt_prcnt,medcr_prcnt,medcd_prcnt,oth_thrd_prty_prcnt,cthmnts,departments,rsdncy_prgms,tot_rx_bdgt,add_degs,job_ttl,plntrk_id,hsg_id,hlth_ldrs_id,dea_no,ftf_parent_mdm_id,onc_cntrt_date,onc_cntrt_stat,pbm_tot_lives,emplr,hmo_ppo,bcbs,medcr_mm_pbm,oth_fed,other,tot_of_phrm,cntr_date,cntrt_stat,spclty_drug_trnd,onc_kam,tot_rev,net_inc_loss,medc_loss_ratio,comm_prem_pmpm,ftf_prvdr_mdm_id,mdm_src,cust_excptn_flag,cust_key,mdm_src_id,mdm_cust_subtype,mdm_cust_type,mm_kam_acct_tier,exec_sum,fully_insured_rx_lives,comm_insured_rx_lives,comm_self_insured_rx_lives,med_caid,med_care,unos_id,alias_name,future_coe__c,attst_prod,msl_site_name,group_s,portfolio_msl_contact,lcz_attstn,lcz_attst_prod,xolair_attst_prod,prod_attstn,compliance_attstn,stt_and_fed_law_attstn,derm_rheum_clinic__c,site_odw_id,site_vendor_org_id,cam_key,sf_id,sf_is_deleted,sf_owner_id,acct_odw_id,cam_acct_key,site_name,site_dba_name,formatted_nm,hin,ddd_no,ddd_sub_cat_code,cust_type,aha,birth_date,bus,bus_desc,contracts_prcs,credentials,do_not_sync_sales_data,employees,excld_from_terr_asgnmt_rules,excld_from_zip_to_terr_prcsg,fax,fax_2,fellowship_location,include_in_terr_asgnmt,may_edit,me,modl,net_income_loss,npi,oth_rel_info,ownership,pager,phm_24_hr_flag,phm_disp_cls_code,phm_disp_type_code,phm_store_no,site_phn_1,site_phn_2,programs,regional_strategy,site_census_beds,site_cot_class,site_cot_facility_type,site_fips_county,site_fips_state,site_license_beds,site_msa,site_or_surgeries,site_procedures,site_profit_status,site_segment,site_staffed_beds,site_status_indicator,site_surgeries,site_web_site_url,spend_cap,src,staff_notes,tax_stat,teach_hospital_flag,territories,totl_bed_cnt,totl_lives,totl_mds_dos,totl_orgc_surg,totl_phrmts,totl_phys_enrolled,totl_revenue,tumor_types,veterans_affairs_hospital,visn,ytd_calls,ytd_stt_spend,'true' inactive,rec_insert_by,rec_insert_date,rec_modify_date,batch_id_insert,centre_no,sonic_id,dea,is_deleted,sonic_org_ext_row_id,phone,email,dept_no,totl_site,site_cmi,site_cot_specialty,hem_msl_contact,onc_msl_contact,rd_msl_contact,academic_community,pc_kam_key_account,pam_key_account,request_code,patnt_intr,site_agmt_revw_date,scz_vldt_stat,sec_scz_vldt_stat,brd_cert_in,key_oso_mm_contact_nov,contacts_identifier,tobi_validation_status,site_agmt_revw_with,rec_modify_by,batch_id_update,nvs_core_profit_status__c,beds__c,country_vod__c,external_id_vod__c,nvs_core_account_category__c,nvs_core_account_subtype__c,nvs_core_credentialing_required__c,nvs_core_credentialing_service_company__c,nvs_core_digital_id__c,nvs_core_emr_system__c,nvs_core_fiscal_id__c,nvs_core_key_account__c,nvs_core_legal_name__c,nvs_core_national_code__c,nvs_core_pricing_program__c,nvs_core_salesforce_id__c,nvs_core_sap_id__c,nvs_core_status__c,nvs_core_top_account_name__c,nvs_core_vendor_id__c,primary_parent_vod__c,recordtype,specialty_1_vod__c,specialty_2_vod__c,hco_phone from all_all_b_usa_crmods.ods_site where sf_id in ('0013900001e8nEEAAY','0013900001hUHcYAAW','0011O00002S0LxwQAF','0013900001e8n9FAAQ','0013900001hUHe8AAG','0013900001e8n9gAAA','0013900001hUHcIAAW','0013900001hUHhYAAW','0011O00002S0LxsQAF','0013900001hUHeTAAW','0013900001hUHbuAAG','0013900001hUHcMAAW','0013900001e8nOaAAI','0013900001hUHcLAAW','0013900001e8nCtAAI','0013900001hUHeuAAG','0011O00002S0LxrQAF','0013900001e8nPVAAY','0013900001hUHcpAAG','0013900001e8nCzAAI','0013900001hUHgUAAW','0013900001hUHe5AAG','0013900001hUHeAAAW','0013900001hUHexAAG','0011O00002S0LxxQAF','0013900001hUHgLAAW','0011O00002S0LxvQAF','0013900001e8nRuAAI','0013900001e8nKlAAI','0013900001hUJ9pAAG','0013900001hUHf0AAG','0013900001hUHgYAAW','0013900001hUHgkAAG','0013900001e8nKHAAY','0013900001e8nUAAAY','0013900001e8nLeAAI','0013900001hUHcZAAW','0013900001hUHgnAAG','0013900001e8nKJAAY','0011O00002QcebuQAB','0013900001hUHggAAG','0013900001hUHhdAAG','0013900001hUHcnAAG','0011O00002Qd1UNQAZ','0013900001hUHceAAG','0013900001e8nFEAAY','0013900001hUHcGAAW','0013900001e8n7kAAA','0011O00002RJGNkQAP','0011O00002RJIJtQAP')""")

spark.sql("""insert into all_all_r_all_bkp.ods_site_t select ods.* from all_all_b_usa_crmods.ods_site ods left anti join all_all_r_all_bkp.ods_site_t fd on fd.sf_id = ods.sf_id""")


spark.sql(""" truncate table all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_b_usa_crmods.ods_site select distinct * from all_all_r_all_bkp.ods_site_t """)
