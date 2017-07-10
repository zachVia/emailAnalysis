from psycopg2 import connect as psy_connect
import pandas as pd
from datetime import date

connection = psy_connect(
    database='adwmain', user='prog_rw', password='gg6liDAOP6jF4KUP', host='10.0.11.128', port=5439
)

sandbox_alooma = pd.read_sql(
                            "SELECT alooma.campaign_eligible.* FROM sandbox.campaign_eligibility "
                             "RIGHT OUTER JOIN alooma.campaign_eligible ON alooma.campaign_eligible.rider_id = sandbox.campaign_eligibility.rider_id"
                             " AND adwmain.alooma.campaign_eligible.campaign_name = adwmain.sandbox.campaign_eligibility.campaign_name"
                             " WHERE sandbox.campaign_eligibility.rider_id IS NULL"
                             " AND sandbox.campaign_eligibility.campaign_group IS NULL"
                             " AND alooma.campaign_eligible.rider_id IS NOT NULL"
                             " AND sandbox.campaign_eligibility.campaign_name IS NULL"
                             " AND sandbox.campaign_eligibility.lifecycle IS NULL"
                             " AND sandbox.campaign_eligibility.rider_state IS NULL"
                             " AND sandbox.campaign_eligibility.start_date IS NULL"
                             " AND sandbox.campaign_eligibility.end_date IS NULL"
                             " AND sandbox.campaign_eligibility.variant_group IS NULL"
                             " AND sandbox.campaign_eligibility.promo_code IS NULL"
                             " AND sandbox.campaign_eligibility.offer_type IS NULL"
                             " AND sandbox.campaign_eligibility.campaign_type IS NULL"
                             " AND sandbox.campaign_eligibility.email IS NULL"
                             " AND sandbox.campaign_eligibility.push IS NULL"
                             " AND sandbox.campaign_eligibility.sms IS NULL"
                             " AND sandbox.campaign_eligibility.inapp IS NULL"
                             " AND sandbox.campaign_eligibility.est_cost_cents IS NULL"
                             " AND sandbox.campaign_eligibility.trigger_ts IS NULL"
                             " AND sandbox.campaign_eligibility.variant_desc IS NULL"
                             " AND sandbox.campaign_eligibility.city_id IS NULL"
                             " AND adwmain.sandbox.campaign_eligibility.promo_campaign_id IS NULL"
                             " AND adwmain.sandbox.campaign_eligibility.promo_code_id IS NULL"
                             " AND adwmain.sandbox.campaign_eligibility.segmentation IS NULL"
                             " AND adwmain.sandbox.campaign_eligibility.campaign_id IS NULL"
                             " AND sandbox.campaign_eligibility.owner IS NULL;", connection)

#rename columns:
sandbox_alooma.rename(columns={'metadata_timestamp': 'trigger_ts'}, inplace=True)

#re-type columns:
sandbox_alooma['start_date'] = sandbox_alooma.start_date.astype(date)
sandbox_alooma['end_date'] = sandbox_alooma.end_date.astype(date)

#cast float columns as ints, replace NaNs with special value:
magicNumber = -492934732.0
sandbox_alooma['inapp'] = sandbox_alooma.inapp.fillna(magicNumber).astype(int)
sandbox_alooma['push'] = sandbox_alooma.push.fillna(magicNumber).astype(int)
sandbox_alooma['variant_group'] = sandbox_alooma.variant_group.fillna(magicNumber).astype(int)
sandbox_alooma['est_cost_cents'] = sandbox_alooma.est_cost_cents.fillna(magicNumber).astype(int)
sandbox_alooma['sms'] = sandbox_alooma.sms.fillna(magicNumber).astype(int)
sandbox_alooma['rider_id'] = sandbox_alooma.rider_id.fillna(magicNumber).astype(int)
sandbox_alooma['email'] = sandbox_alooma.email.fillna(magicNumber).astype(int)
sandbox_alooma['metadata_version'] = sandbox_alooma.metadata_version.fillna(magicNumber).astype(int)
sandbox_alooma['offer'] = sandbox_alooma.offer.fillna(magicNumber).astype(int)

#replace special value with None:
sandbox_alooma = sandbox_alooma.where(sandbox_alooma != magicNumber, None)

#sandbox_alooma.tocsv('~/path')