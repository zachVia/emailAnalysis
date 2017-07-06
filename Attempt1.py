from psycopg2 import connect as psy_connect
import pandas as pd

connection = psy_connect(
    database='adwmain', user='prog_rw', password='gg6liDAOP6jF4KUP', host='10.0.11.128', port=5439
)

# alooma_read = pd.read_sql("SELECT adwmain.alooma.campaign_eligible.campaign_group, adwmain.alooma.campaign_eligible.end_date, adwmain.alooma.campaign_eligible.inapp, adwmain.alooma.campaign_eligible.campaign_type, adwmain.alooma.campaign_eligible.push, adwmain.alooma.campaign_eligible.variant_group, adwmain.alooma.campaign_eligible.lifecycle, adwmain.alooma.campaign_eligible.campaign_name, adwmain.alooma.campaign_eligible.rider_state, adwmain.alooma.campaign_eligible.variant_desc, adwmain.alooma.campaign_eligible.start_date, adwmain.alooma.campaign_eligible.offer_type, adwmain.alooma.campaign_eligible.email, adwmain.alooma.campaign_eligible.rider_id, adwmain.alooma.campaign_eligible.sms, adwmain.alooma.campaign_eligible.est_cost_cents, adwmain.alooma.campaign_eligible.metadata_timestamp, adwmain.alooma.campaign_eligible.city_id, adwmain.alooma.campaign_eligible.promo_code FROM adwmain.alooma.campaign_eligible", connection)
# sandbox_read = pd.read_sql("SELECT adwmain.sandbox.campaign_eligibility.rider_id, adwmain.sandbox.campaign_eligibility.campaign_group, adwmain.sandbox.campaign_eligibility.campaign_name FROM adwmain.sandbox.campaign_eligibility", connection)
#
# alooma_read.merge(sandbox_read, how='inner', left_on=['rider_id', 'campaign_name'], right_on=['rider_id', 'campaign_name'])
# pd.merge(alooma_read, sandbox_read, how='inner', on='rider_id',)

sandbox_alooma = pd.read_sql("SELECT adwmain.alooma.* FROM adwmain.sandbox.campaign_eligibility "
                             "RIGHT OUTER JOIN adwmain.alooma ON adwmain.alooma.rider_id = adwmain.sandbox.rider_id"
                             "AND adwmain.alooma.campaign_group = adwmain.sandbox.campaign_group"
                             "AND adwmain.alooma.campaign_name = adwmain.sandbox.campaign_name")
print("okay")