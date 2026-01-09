#!/usr/bin/env bash
set -e
source .venv/bin/activate

python fetch_aemet_barcelona.py
python spark_etl_aemet.py
python model_train_predict.py

echo "Pipeline + Predicción actualizado"
