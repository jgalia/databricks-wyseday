

# create a query
* In sql editer create a new query and save it:

```
select sum (PRCP) as monthly_prcp, month(DATE) as month, year(DATE) as year
from wyseday_databricks.default.jg_noaa_workflow where NAME like '%PARIS%'  group by month(DATE), year(DATE) order by year, month ;
```

![save_query](images/save_query)


# create a dashboard
* go in the dashboard menu

![dash_menu](images/dash_menu)

* click on create a dashboard

![create_dash](images/create_dash)

* click on add visualization

![add_viz](images/add_viz)

* choose the query you have saved

![choose_query](images/choose_query)

* select "create new visualization" and click on "create visualization"

![create_vis](images/create_vis)

* make your graph as you wish, exemple : perso_vis

![perso_vis](images/perso_vis)

* save and share you dashboard

![share](images/share)

* choose the credentials to share

![policy](images/policy)










