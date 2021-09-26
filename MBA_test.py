from google.cloud import bigquery
from mlxtend.frequent_patterns import apriori, fpmax, fpgrowth, association_rules

# TODO: Uncomment the line below to set the project variable.
# project = 'user-project-id'
#
# The project variable defines the project to be billed for query
# processing. The user must have the bigquery.jobs.create permission on
# this project to run a query. See:
# https://cloud.google.com/bigquery/docs/access-control#permissions

client = bigquery.Client()

query_string = """
SELECT
date, 
fullVisitorId, -- Cookie or Visitor Id
transaction.transactionId, 
p.productSKU,
p.v2ProductName,
p.v2ProductCategory,
p.productRevenue,
(p.productPrice/100000) AS price,
p.productBrand,	
p.productQuantity	
FROM 
`bigquery-public-data.google_analytics_sample.ga_sessions_*` AS T,
UNNEST(T.hits) AS h, UNNEST(h.product) AS p
WHERE T._TABLE_SUFFIX BETWEEN "20170101" AND "20170301"
AND transaction.transactionId IS NOT NULL
"""
query_job = client.query(query_string)
#safe_query_job=client.query(query_job)
summary_df = query_job.to_dataframe()
print("This is the summary", summary_df.tail(10))

# Defining the hot encoding function t make the data suitable
# for the concerned libraries
def hot_encode(x):
    if(x<= 0):
        return 0
    if(x>= 1):
        return 1

df1 = (summary_df.groupby(['transactionId', 'v2ProductName'])['productQuantity'].sum().unstack().reset_index().fillna(0).set_index('transactionId'))
print("This is the unstacked dataframe", df1.head())
basket_encoded = df1.applymap(hot_encode)
basket_2 = basket_encoded
min_support = 0.01
print("This is the basket set", basket_2.head())
#frq_items = apriori(basket_2, min_support=min_support, use_colnames=True)
frq_items = fpgrowth(basket_2, min_support=min_support, use_colnames=True)
print(frq_items)

frq_items['length'] = frq_items['itemsets'].apply(lambda x: len(x))

cell_hover = {  # for row hover use <tr> instead of <td>
    'selector': 'td:hover',
    'props': [('background-color', '#ffffb3')]
}
index_names = {
    'selector': '.index_name',
    'props': 'font-style: italic; color: darkgrey; font-weight:normal;'
}
headers = {
    'selector': 'th:not(.index_name)',
    'props': 'background-color: #000066; color: white;'
}

#frq_items['length'] = frq_items['itemsets'].apply(lambda x: len(x))
#frq_items['itemsets'] = frq_items['itemsets'].apply(lambda x: list(x)).astype("unicode")
#frq_items = frq_items.sort_values(['support', 'length'], ascending=[False, True])
#frq_items = frq_items.style.set_caption("More Frequent Dimensions")

#frq_items.set_table_styles([cell_hover, index_names, headers])

#frq_items.set_table_styles([
#    {'selector': 'th.col_heading', 'props': 'text-align: center;'},
#    {'selector': 'th.col_heading.level0', 'props': 'font-size: 1.5em;'},
#    {'selector': 'td', 'props': 'text-align: center; font-weight: bold;'},
#], overwrite=False)

rules = association_rules(frq_items, metric='lift', min_threshold=3.0)
rules["antecedent_len"] = rules["antecedents"].apply(lambda x: len(x))
rules["antecedents"] = rules["antecedents"].apply(lambda x: list(x)).astype("unicode")
rules["consequents"] = rules["consequents"].apply(lambda x: list(x)).astype("unicode")
rules2 = rules[(rules['lift'] >= 2) & (rules['confidence'] >= 0.2)]
## are confidence and lift the best metrics to consider?
rules3 = rules2.rename(columns={'antecedent support': 'antecedent_supp', 'consequent support': 'consequent_supp'})
rules3 = rules3.sort_values(['lift', 'antecedent_len'], ascending=[False, True])
rules4 = rules3.drop('antecedent_supp', axis=1).drop('consequent_supp', axis=1).drop('support', axis=1).drop('leverage',
                                                                                                             axis=1).drop(
    'conviction', axis=1)
#rules4 = rules4.style.set_caption("Lift of Sets of Dimensions")

#rules4.set_table_styles([cell_hover, index_names, headers])

#rules4.set_table_styles([
#    {'selector': 'th.col_heading', 'props': 'text-align: center;'},
#    {'selector': 'th.col_heading.level0', 'props': 'font-size: 1.5em;'},
#    {'selector': 'td', 'props': 'text-align: center; font-weight: bold;'},
#], overwrite=False)

print("Frequent items", frq_items.head(25))

print("Rules", rules4.head(25))

#rules4.to_gbq(table_id, project_id, if_exists='append')
dataset_name = 'mba_test_data'
output_table_id = 'mba_results_public_google_dataset'
project_id = 'starklab'


rules4.to_gbq(dataset_name +'.'+ output_table_id, project_id, if_exists='append')




# Print the results.
#for row in query_job.result():  # Wait for the job to complete.
#    print(row)

