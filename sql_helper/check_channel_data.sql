
----------
----OK
-- Initial catalogs
select * from rhnproductname where id in (select product_name_id from rhnchannel where id = 118);
select * from rhnchannelproduct where id in (select channel_product_id from rhnchannel where id = 118);
select * from rhnarchtype  where id in (select arch_type_id from rhnchannelarch where id in (select channel_arch_id from rhnchannel where id = 118)); -- can be incomplete
select * from rhnchecksumtype where id in (select checksum_type_id from rhnchannel where id = 118); -- can be incomplete
select * from rhnpackagearch where id in (select package_arch_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from web_customer where id in (select org_id from rhnchannel where id = 118);
select * from rhnchannelarch where id in (select channel_arch_id from rhnchannel where id = 118);
select * from rhnerrataseverity where id in (select severity_id from rhnerrata where id in (select errata_id from rhnchannelerrata where channel_id = 118))
--- step 2
select * from rhnchannel where id = 118;
select * from rhnchannelfamilymembers where channel_id = 118;
select * from rhnchannelfamily where id in (select channel_family_id from rhnchannelfamilymembers where channel_id = 118);

select * from rhnerrata where id in (select errata_id from rhnchannelerrata where channel_id = 118);
select * from rhnchannelerrata where channel_id = 118;

--- step 3
select * from rhnpackagename where id in (select name_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnpackagegroup where id in (select package_group from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnsourcerpm where id in (select source_rpm_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnpackageevr where id in (select evr_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118));

select * from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnchannelpackage where channel_id = 118;
select * from rhnerratapackage where errata_id in (select errata_id from rhnchannelerrata where channel_id = 118);

select * from rhnpackageprovider where id in (select provider_id from rhnpackagekey where id in (select rhnpackagekeyassociation.key_id from rhnpackagekeyassociation where package_id in (select package_id from rhnchannelpackage where channel_id = 118)));
select * from rhnpackagekeytype where id in (select key_type_id from rhnpackagekey where id in (select rhnpackagekeyassociation.key_id from rhnpackagekeyassociation where package_id in (select package_id from rhnchannelpackage where channel_id = 118)));
select * from rhnpackagekey where id in (select rhnpackagekeyassociation.key_id from rhnpackagekeyassociation where package_id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnpackagekeyassociation where package_id in (select package_id from rhnchannelpackage where channel_id = 118);

select * from rhnerratabuglist where errata_id in (select errata_id from rhnchannelerrata where channel_id = 118);

select * from rhnpackagecapability where id in (select capability_id from rhnpackagebreaks where package_id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnpackagebreaks where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackagechangelogdata where id in (select changelog_data_id from rhnpackagechangelogrec where package_id in (select package_id from rhnchannelpackage where channel_id = 118));
select * from rhnpackagechangelogrec where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackageconflicts where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackageenhances where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackagefile where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackageobsoletes  where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackagepredepends  where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackageprovides where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackagerequires where package_id in (select package_id from rhnchannelpackage where channel_id = 118);
select * from rhnpackagesuggests where package_id in (select package_id from rhnchannelpackage where channel_id = 118);

select * from rhnsourcerpm where id in (
    select source_rpm_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118) union all
    select source_rpm_id from rhnpackagesource
);

select * from rhnpackagerecommends where package_id in (select package_id from rhnchannelpackage where channel_id = 118);

select * from rhnchecksum
where id in
      (select checksum_id from rhnpackage where id in (select package_id from rhnchannelpackage where channel_id = 118) union all
       select checksum_id from rhnpackagefile where package_id in (select package_id from rhnchannelpackage where channel_id = 118));

------
---- NOT tested
-- rhnpackagesource
-- rhnpackagesuggests


