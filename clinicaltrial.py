from api import fetch_studies_raw, studies_to_flat_df

raw = fetch_studies_raw(
    filter_overall_status=["RECRUITING", "ACTIVE_NOT_RECRUITING"],
    last_update_from="2020-01-01",   
    last_update_to="MAX",            
    page_size=200,
    max_records=6000,
    sort="LastUpdatePostDate:desc",
)

df = studies_to_flat_df(raw)
print(df.shape)
print(df.head())
df.to_csv("trials_last_5_years.csv", index=False)
