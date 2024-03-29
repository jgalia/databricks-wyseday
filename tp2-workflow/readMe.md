# create workflow
documentation : https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html#create-a-job

* change the path at the end of the notebook workflow-job

* click on workflows > create job

![create](images/workflow_create.png)

* choose type : notebook

![job](images/job.png)

* select notebook, choose workflow-job that you have updated with your initials

![select1](images/select1.png)

![select2](images/select2.png)

* then click on "run now"

![run](images/run.png)

* monitore the run

![monitore](images/monitore.png)


# schedule workflow
documentation : https://docs.databricks.com/en/notebooks/schedule-notebook-jobs.html#schedule-a-notebook-job

* click on add trigger

![add_trigger](images/add_trigger.png)

* choose scheduled

![scheduled](images/scheduled.png)

* active and save

![active](images/active.png)

* you can pause the trigger

![pause](images/pause.png)

# Workflow with parameters
* create a new job or update your actual job to use the notebook : workflow-job-with-parameter

![run_param](images/run_param.png)

* click on run now with different parameters : 
  + set key : "initials"  
  + value : your initials

![kv](images/kv.png)



* monitor the job, you can see your parameters

![params](images/params.png)


* go to tp3 to see the result table written in the catalog 


