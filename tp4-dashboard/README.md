

# create a query
* In sql editer create a new query and save it:

```
select sum (PRCP) as monthly_prcp, month(DATE) as month, year(DATE) as year
from wyseday_databricks.default.jg_noaa_workflow where NAME like '%PARIS%'  group by month(DATE), year(DATE) order by year, month ;
```

![save_query](images/save_query.png)


# create a dashboard
* go in the dashboard menu

![dash_menu](images/dash_menu.png)

* click on create a dashboard

![create_dash](images/create_dash.png)

* click on add visualization

![add_viz](images/add_viz.png)

* choose the query you have saved

![choose_query](images/choose_query.png)

* select "create new visualization" and click on "create visualization"

![create_vis](images/create_vis.png)

* make your graph as you wish, exemple : perso_vis

![perso_vis](images/perso_vis.png)

* save and share you dashboard

![share](images/share.png)

* choose the credentials to share

![policy](images/policy.png)










