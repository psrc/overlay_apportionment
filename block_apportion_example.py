from modules import get_data
from modules import apportion
import yaml
from pathlib import Path
from modules import configuration
import geopandas as gpd
import pandas as pd
file = Path().joinpath(configuration.args.configs_dir,"config.yaml")

config = yaml.safe_load(open(file))

buffers = gpd.read_file(config['overlay_path'])
buffers['id'] = buffers.index+1

data = {}

last_used_census_year = "0"

for census_year, years in config['years'].items():
     blocks_gdf = get_data.read_from_sde(
            config['server'], 
            config['elmer_geo_database'],
            "block" + census_year + "_nowater",
            config['version'],
            is_table=False,
        )
     geo_id = "geoid" + census_year[-2:]
     split_blocks = apportion.block_split(buffers, 'id', blocks_gdf, geo_id)
     

     for year in years:
         block_apportion, ofm_data = apportion.apportion_ofm_data(split_blocks, year, 'id', geo_id,  config['elmer_conn_string'])
         inside_data = block_apportion.sum().to_dict()
         keep_cols = [col for col in ofm_data.columns if col != 'block_geoid']
         ofm_data = ofm_data[keep_cols]
         total_data = ofm_data.sum().to_dict()
         row = {**inside_data, **total_data}
         data[year] = row


         #data[year] = {'inside' : block_apportion, ofm_data)

df = pd.DataFrame(data)
df.to_csv(config['output_file'])
print ('done')