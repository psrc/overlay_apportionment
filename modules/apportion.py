
import pandas as pd
import geopandas as gpd
from modules import get_data

def block_split(overlay, overlay_id_col, blocks, geo_id):
    blocks = blocks[[geo_id, "geometry"]]
    blocks["block_area"] = blocks.geometry.area
    blocks_overlay = gpd.overlay(blocks, overlay, how="intersection")
    blocks_overlay["split_area"] = blocks_overlay.geometry.area
    df = blocks_overlay.groupby([overlay_id_col, geo_id], as_index=False).agg(
        {"block_area": "first", "split_area": "sum"}
    )
    df["percent_area"] = df["split_area"] / df["block_area"]
    return df

def apportion_ofm_data(split_blocks, year, overlay_id_col, geo_id, elmer_conn_string):
    ofm_fields = [
        "household_population",
        "occupied_housing_units",
        "group_quarters_population",
    ]

    query = "exec ofm.block_pop_estimates '2020 vintage Data', %s" % (str(year))
    ofm_data = get_data.read_from_elmer_sqlalchemy(elmer_conn_string, query)
    df = split_blocks.merge(ofm_data, how="left", left_on=geo_id, right_on="block_geoid")
    #agg_dict = {k + '_inside':'sum' for k in ofm_fields}
    agg_dict = {}
    for field in ofm_fields:
        new_field = field + "_inside_"
        df[new_field] = df[field] * df["percent_area"]
        agg_dict[new_field] = 'sum'
    df = df.groupby([overlay_id_col], as_index=False).agg(agg_dict)

    return df, ofm_data


def block_split2(overlay, overlay_id_col, blocks, geo_id, block_groups, block_data, year, block_cols=None):
    if not block_cols:
        block_cols = [col for col in block_data.columns if col != geo_id]
    new_block_cols = []
    for col in block_cols:
        block_data[col + '_' + str(year)] = block_data[col]
        new_block_cols.append(col + '_' + str(year))
    block_data.drop(columns=block_cols, inplace = True)
    block_cols= new_block_cols

        


    
    #block_cols = [col for col in block_data.columns if col != geo_id]

    # {'household_population':'sum', 'occupied_housing_units' : 'sum', 'group_quarters_population' : 'sum'}
    ofm_block_group_agg_dict = {}
    total_agg_dict = {column: 'sum' for column in block_cols}
    inside_agg_dict = {column + '_inside': 'sum' for column in block_cols}

    overlay = overlay[[overlay_id_col, "geometry"]]
    blocks = blocks[[geo_id, "geometry"]]
    blocks["block_area"] = blocks.geometry.area
    block_groups["block_group_area"] = block_groups.geometry.area
    block_groups = block_groups[[geo_id, "block_group_area"]]

    gdf = gpd.overlay(blocks, overlay, how="union")
    gdf["split_area"] = gdf.geometry.area
    df = gdf.groupby([overlay_id_col, geo_id], as_index=False).agg(
        {"block_area": "first", "split_area": "sum"}
    )
    df["percent_area"] = df["split_area"] / df["block_area"]
    block_data["block_group_id"] = block_data[geo_id].str[:-3]
    df = df.merge(block_data, how="left", on=geo_id)
    for column in block_cols:
        new_column = column + "_inside"
        df[new_column] = df[column] * df["percent_area"]
        # ofm_block_group_agg_dict[new_key] = 'sum'

    overlay_blocks = df.groupby([overlay_id_col, 'COUNTY'], ).agg(inside_agg_dict)

    # now do block groups
    # need ofm totals at bg level
    bg_totals = df.groupby(["block_group_id"], as_index=False).agg(total_agg_dict)
    # need split_area
    #ofm_block_group_agg_dict["split_area"] = "sum"
    inside_agg_dict['split_area']= "sum"
    overlay_block_groups = df.groupby(
        [overlay_id_col, "block_group_id"], as_index=False
    ).agg(inside_agg_dict)
    overlay_block_groups = overlay_block_groups.merge(
        bg_totals, how="left", on="block_group_id"
    )
    # now get area aggregated for all block groups
    overlay_block_groups = overlay_block_groups.merge(
        block_groups, how="left", left_on="block_group_id", right_on=geo_id
    )

    

    return overlay_blocks, df, overlay_block_groups