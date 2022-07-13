
{# SQL  #}
	
merge crm_salesforce.d_account_contact as tgt
using 
(
select C.ID as ACCOUNT_CONTACT_ID,
C.NAME as CONTACT_NAME,
A.NAME as ACCOUNT_NAME,
A.TYPE as ACOUNT_TYPE,
A.ACCOUNT_NUMBER,
C.MOBILE_PHONE as CONTACT_MOBILE,
C.EMAIL as CONTACT_EMAIL
from crm_salesforce.account a
join crm_salesforce.contact c
on a.id = c.account_id
) as src
on tgt.ACCOUNT_CONTACT_ID = src.ACCOUNT_CONTACT_ID
when matched then update set
tgt.CONTACT_NAME = src.CONTACT_NAME ,
tgt.ACCOUNT_NAME = src.ACCOUNT_NAME,
tgt.ACOUNT_TYPE = src.ACOUNT_TYPE, 
tgt.ACCOUNT_NUMBER = src.ACCOUNT_NUMBER,
tgt.CONTACT_MOBILE = src.CONTACT_MOBILE, 
tgt.CONTACT_EMAIL = src.CONTACT_EMAIL, 
tgt.DW_UPDATE_TS = current_timestamp, 
when not matched by tgt then
insert (ACCOUNT_CONTACT_ID,CONTACT_NAME,ACCOUNT_NAME,ACOUNT_TYPE,ACCOUNT_NUMBER,CONTACT_MOBILE,CONTACT_EMAIL,DW_CREATE_TS,DW_UPDATE_TS)
values (src.ACCOUNT_CONTACT_ID,src.CONTACT_NAME,src.ACCOUNT_NAME,src.ACOUNT_TYPE,src.ACCOUNT_NUMBER,src.CONTACT_MOBILE,src.CONTACT_EMAIL,current_timestamp,current_timestamp);

{#  end sql #}