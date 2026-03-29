name=model_entity_metadata_mapping
fn=/Users/yevgeniy/algo/db/af/collaborations/nvda/${name}.csv
fn=/disk8/db/af/collaborations/nvda/${name}.csv

# echo $fn

# sudo add-apt-repository ppa:deadsnakes/ppa
# sudo apt install python3.14
# sudo apt install python3.14-venv
# python3.14 -m venv .venv
# source .venv/bin/activate
# python3.14 -m pip install -U pip
# python3.14 -m pip install -r requirements.txt

python3.14 convert.py $fn

# python3.14 app.py --fn $fn