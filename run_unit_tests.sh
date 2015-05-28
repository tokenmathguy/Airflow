export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg
if [[ -d "airflow/www/static/coverage" ]]
then
  rm airflow/www/static/coverage/*
else
  mkdir -p "airflow/www/static/coverage"
fi
nosetests --with-doctest --with-coverage --cover-html --cover-package=airflow -v --cover-html-dir=airflow/www/static/coverage
# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
