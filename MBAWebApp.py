#This is the main file of the MBAWebApp
from google.cloud import bigquery
from mlxtend.frequent_patterns import apriori, fpmax, fpgrowth, association_rules
from jinjasql import JinjaSql
import matplotlib.pyplot as plt
from utils import hot_encode

project_id = 'starklab'
project = project_id
client = bigquery.Client()


#####################################
##### Asociation rules using apriori algorithm. ########
#####################################

# Building the models and analyzing the results
  ## Building the model
  ## We will use Apriori to determine the frequent item sets of this dataset.
  ## To do this, we will say that an item is frequent if it appears in at least 5 transactions of the dataset.
  ## The value 5 is represented in percentage, and in this case is called min_support = 0.01 i.e., 1%
  ## Then the minimum support that the items and pairs of items should have is 1
  ## Collecting the inferred rules in a dataframe
  ## The association rules are going to be extracted from the frequent itemset. We are setting them based on lift and has minimum lift as 1.
    # as based business use case we can sort based on confidence and lift


##TODO ### Cambiar project_id for hard code ###
def association_rules_analysis(brand_name, initialDate, endingDate, min_support, output_table_id, output2_table_id):
  dataset_name = 'mba_test_data'
  project_id = 'starklab'
  params = {
    'initialDate': str(initialDate),
    'endingDate': str(endingDate),
  }
  query_config = bigquery.QueryJobConfig(use_legacy_sql=True)
  #query_template = """
  #SELECT
  #date,
  #fullVisitorId, -- Cookie or Visitor Id
  #transaction.transactionId,
  #p.productSKU,
  #p.v2ProductName,
  #p.v2ProductCategory,
  #p.productRevenue,
  #(p.productPrice/100000) AS price,
  #p.productBrand,
  #p.productQuantity
  #FROM
  #[bigquery-public-data.google_analytics_sample.ga_sessions_*] AS T,
  #UNNEST(T.hits) AS h, UNNEST(h.product) AS p
  #WHERE T._TABLE_SUFFIX BETWEEN {{ initialDate}} AND {{ endingDate }}
  #AND transaction.transactionId IS NOT NULL
  #"""
  query_template = """
  SELECT
  *
  FROM
  [starklab.mba_test_data.mba_ardene_dataset]
  WHERE date BETWEEN {{ initialDate }} AND {{ endingDate }}
  """
  j = JinjaSql(param_style='pyformat')
  query, bind_params = j.prepare_query(query_template, params)
  #dataset = get_sql_from_template(query, bind_params)
  dataset = query % bind_params
  print(dataset)
  safe_query_job=client.query(dataset, job_config=query_config)
  df = safe_query_job.to_dataframe()
  most_sold = df['productName'].value_counts().head(15)
  #plt.figure(figsize=(12,6))
  #plt.subplot(1,2,1)
  #most_sold.plot(kind='bar')
  #plt.title(brand_name + ' ' + 'Most Sold Items, from' + initialDate +  ' to ' + endingDate )
  #df2 = (df.groupby(['transactionId', 'productSKU'])['productQuantity'].sum().unstack().reset_index().fillna(0).set_index('transactionId'))
  df2 = (df.groupby(['transactionId', 'productName'])['productQuantity'].sum().unstack().reset_index().fillna(0).set_index('transactionId'))
  print(df2.head())
  basket_encoded = df2.applymap(hot_encode)
  basket = basket_encoded
  #frq_items = apriori(basket, min_support = min_support, use_colnames = True)
  frq_items = fpgrowth(basket, min_support=min_support, use_colnames=True)
  print(frq_items)
  frq_items_formatted = fpgrowth(basket, min_support = min_support, use_colnames = True)
  frq_items_formatted['length'] = frq_items['itemsets'].apply(lambda x: len(x))
  frq_items_formatted
  frq_items_formatted['itemsets'] = frq_items_formatted['itemsets'].apply(lambda x: list(x)).astype("unicode")
  print("This is the formatted dataset")
  print(frq_items_formatted)
  print("this is the unformatted dataset")
  print(frq_items)
  rules = association_rules(frq_items, metric='lift', min_threshold=0.5)
  rules["antecedents"] = rules["antecedents"].apply(lambda x: list(x)).astype("unicode")
  rules["consequents"] = rules["consequents"].apply(lambda x: list(x)).astype("unicode")
  support=rules['support'].tolist()
  support=[element*100 for element in support]
  support=np.array(support)
  confidence=rules['confidence'].tolist()
  confidence=np.array(confidence)
  plt.figure(figsize=(12,6))
  plt.subplot(1,2,1)
  plt.scatter(support, confidence,   alpha=0.5, marker="*")
  plt.xlabel('support')
  plt.ylabel('confidence')
  rules = rules.sort_values(['confidence', 'lift'], ascending = [False, False])
  print(len(rules))
  rules2 = rules[(rules['lift'] >= 2) & (rules['confidence'] >= 0.2)]
  rules2['firstdate'] = ([firstdate]*(len(rules2)))
  rules2['seconddate'] = ([seconddate]*(len(rules2)))
  rules3 = rules2.rename(columns={'antecedent support': 'antecedent_supp', 'consequent support': 'consequent_supp'})
  G1 = nx.Graph()
  G1 = nx.from_pandas_edgelist(rules2, 'antecedents', 'consequents', edge_attr='lift')
  lift1 = [i['lift'] for i in dict(G1.edges).values()]
  labels = [i for i in dict(G1.nodes).keys()]
  labels = {i:i for i in dict(G1.nodes).keys()}
  fig, ax = plt.subplots(figsize=(22,10))
  pos1 = nx.spring_layout(G1)
  plt.title(brand_name + ' ' + 'Associated Items, from ' + initialDate +  ' to ' + endingDate + ' ' +  'Min Support = ' + str(min_support))
  nx.draw_networkx_nodes(G1, pos1, ax = ax, label=True, node_size=700, node_color='g')
  #nx.draw_networkx_edges(G1, pos1, width=(confidences1*1000), ax=ax, edge_color="r")
  nx.draw_networkx_edges(G1, pos1, width=(lift1), ax=ax, edge_color="r")
  _ = nx.draw_networkx_labels(G1, pos1, labels, ax=ax)
  frq_items_formatted.to_gbq(dataset_name +'.'+ output2_table_id, project_id, if_exists='append')
  rules3.to_gbq(dataset_name +'.'+ output_table_id, project_id, if_exists='append')
  return df2.head(5), rules3


if __name__ == '__main__':
  association_rules_analysis('test', '20210101', '20210115', 0.002, 'ga_test_rules', 'ga_test_freq')