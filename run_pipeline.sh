#!/usr/bin/env bash
set -e

python fetch_aemet_barcelona.py
python spark_etl_aemet.py
python rain_train_predict.py
python model_advanced_train_predict.py
streamlit run app_streamlit.py


echo "Pipeline + Predicción actualizado"
