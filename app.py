# TBD:
# X Confirmation when changing groups
# X Confirmation when saving changes
# X Edit modal
# X Show changes in configurations
# X Select changes to be promoted
# - Delete rows
# - Integrate with CI/CD
# - Search
# - Partial search
# - Input panel with accordions and validations
# - Bulk upload

# TESTS:
# - Select ingestion group
# - Insert
# - Update
# - Warning: Incomplete
# - Error: Duplicate
# - Error: Invalid SQL
# - 100's of rows

import os
import datetime
from databricks import sql
from databricks.sdk.core import Config
import gradio as gr
import pandas as pd

# Ensure environment variables are set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."
assert os.getenv('CONFIG_TABLE'), "CONFIG_TABLE must be set in app.yaml."
CONFIG_TABLE = os.getenv('CONFIG_TABLE')
KEYS = ['ingestion_group', 'target_catalog', 'target_database', 'target_table']
COLUMNS = ["active", "source_type", "source_file_path", "source_file_aux_path", "source_kafka_topic", "source_kafka_schema", "silver_transformations", "silver_time_key", "silver_merge_keys", "silver_clustering_keys"]

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
            except Exception as e:
                print(e)
                raise e
            return cursor.fetchall_arrow().to_pandas()

def getIngestionGroups() -> list:
    return sqlQuery(f"""
        select distinct ingestion_group 
        from {CONFIG_TABLE} 
        order by ingestion_group
    """)["ingestion_group"].to_list()

def getConfigs(group: str) -> pd.DataFrame:
    return sqlQuery(f"""
        select * except (time_last_modified, time_last_promoted)
        from {CONFIG_TABLE} 
        where ingestion_group = '{group}'
        order by target_catalog, target_database, target_table
    """)

def getConfigsForPromotion(group: str) -> pd.DataFrame:
    return sqlQuery(f"""
        select * except (time_last_modified, time_last_promoted)
        from {CONFIG_TABLE} 
        where 
            ingestion_group = '{group}'
            and time_last_modified is not null 
            and ((time_last_modified > time_last_promoted) or (time_last_promoted is null))
        order by target_catalog, target_database, target_table
    """)

def getTableChoices(init_promo: pd.DataFrame) -> list:
    return [f"{cat}.{db}.{tbl}" for cat, db, tbl in zip(init_promo["target_catalog"], init_promo["target_database"], init_promo["target_table"])]

def updateDropdown(choices: list):
    return gr.Dropdown(choices=choices, value=choices)

def echoInput(input: pd.DataFrame) -> pd.DataFrame:
    return input

def saveConfigs(init_config: pd.DataFrame, new_config: pd.DataFrame):

    # Validate: duplicates
    if new_config.duplicated(subset=KEYS).any():
        raise gr.Error("Configuration update failed! Configuration has duplicates")

    # Validate: incomplete rows
    incomplete = new_config[
        (new_config["ingestion_group"].isnull()) | (new_config["ingestion_group"] == '')
        | (new_config["target_catalog"].isnull()) | (new_config["target_catalog"] == '')
        | (new_config["target_database"].isnull()) | (new_config["target_database"] == '')
        | (new_config["target_table"].isnull()) | (new_config["target_table"] == '')
    ]
    n_incomplete = len(incomplete)
    if n_incomplete > 0:
        gr.Warning(f"Skipped {n_incomplete} rows with missing ingestion_group, catalog, database and/or table. Make sure to fix them and try again to avoid loosing any changes")
    new_config = new_config[~new_config.index.isin(incomplete.index)]

    # Identify changes and set time_last_modified
    init_config = init_config.set_index(KEYS)
    new_config = new_config.set_index(KEYS)

    new_rows = new_config[~new_config.index.isin(init_config.index)]
    
    matching_init_config = init_config[init_config.index.isin(new_config.index)]
    matching_new_config = new_config[new_config.index.isin(init_config.index)]
    mod_rows = matching_new_config.filter(items=matching_new_config.compare(matching_init_config).index, axis=0)

    if len(new_rows) == 0 and len(mod_rows) == 0:
        gr.Warning("No changes or new configurations")
        return None

    new_config = pd.concat([new_rows, mod_rows]).reset_index().sort_values(KEYS)
    new_config["time_last_modified"] = str(datetime.datetime.now())

    # Prepare values
    values_list = []
    for row in new_config.to_dict(orient="records"):
        values_list.append(
            f"('{row['ingestion_group']}', '{row['target_catalog']}', '{row['target_database']}', '{row['target_table']}', {row['active']}, '{row['source_type']}', '{row['source_file_path']}', '{row['source_file_aux_path']}', '{row['source_kafka_topic']}', '{row['source_kafka_schema']}', '{row['silver_transformations']}', '{row['silver_time_key']}', '{row['silver_merge_keys']}', '{row['silver_clustering_keys']}', '{row['time_last_modified']}')"
        )
    values = ",".join(values_list)

    # Prepare statement
    cols = KEYS+COLUMNS+["time_last_modified"]
    schema = ", ".join(cols)
    update_clause = ", ".join([f"t.{col} = s.{col}" for col in cols])
    t = ", ".join([f"t.{col}" for col in cols])
    s = ", ".join([f"s.{col}" for col in cols])
    insert_clause = f"({t}) VALUES ({s})"
    on_clause = " AND ".join([f"t.{key} = s.{key}" for key in KEYS])
    query = f"""
        WITH s AS (VALUES {values} AS s({schema}))

        MERGE INTO {CONFIG_TABLE} t
        USING s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """

    try:
        # try to update config table
        sqlQuery(query)
        gr.Info("Configurations successfully updated")
    except:
        # otherwise, show error message
        raise gr.Error("Configuration update failed")

def savePromotions(group: str, choices: list):

    if len(choices) == 0:
        gr.Warning("No tables selected")
        return None

    all_choices = ', '.join([f"'{c}'" for c in choices])

    query = f"""
        update {CONFIG_TABLE} 
        set time_last_promoted = '{str(datetime.datetime.now())}' 
        where ingestion_group = '{group}' and concat(target_catalog, '.', target_database, '.', target_table) in ({all_choices})
    """

    try:
        # try to update config table
        sqlQuery(query)
        gr.Info("Tables successfully marked for promotion")
    except:
        # otherwise, show error message
        raise gr.Error("Promotion failed")

groups = getIngestionGroups()
init_group = groups[0]
init_config = getConfigs(init_group)
init_promo = getConfigsForPromotion(init_group)
init_promo_choices = getTableChoices(init_promo)
init_del_choices = getTableChoices(init_config)
            
# display the data with Gradio
with gr.Blocks(title="Meta Ingestion App", css="footer {visibility: hidden}") as app:
    
    # Header
    with gr.Row(height=84):
        with gr.Column(scale=0, min_width=84):
            gr.HTML('<img src=favicon.ico>')
        with gr.Column(scale=1):
            gr.HTML('<div style="height: 64px; line-height: 64px"><font size="6"><b>Meta Ingestion App</b></font></div>')
    
    # Selectors
    with gr.Row():
        with gr.Column(variant="panel"):
            gr.Markdown("## Ingestion Group:")
            gr.Markdown("Changing the ingestion group will discard any changes to the configuration table. Please make sure you have saved your configurations before changing the ingestion group.")
            group = gr.Dropdown(choices=groups, value=init_group, container=False)

    # Table
    with gr.Row():
        with gr.Tab("Configuration"):
            config_table = gr.Dataframe(value=init_config, label="Configure your ingestion parameters", col_count=(len(init_config.columns), "fixed"))
            config_button = gr.Button("Apply")
        with gr.Tab("Deletion"):
            del_dropdown = gr.Dropdown(init_del_choices, value=[], label="Select tables for deletion", multiselect=True, interactive=True)
            with gr.Row():
                del_clear_button = gr.Button("Clear")
                del_all_button = gr.Button("Select All")
                del_button = gr.Button("Apply")
        with gr.Tab("Promotion"):
            promo_dropdown = gr.Dropdown(init_promo_choices, value=init_promo_choices, label="Select tables to promote to production", multiselect=True, interactive=True)
            promo_table = gr.Dataframe(value=init_promo, interactive=False)
            with gr.Row():
                promo_clear_button = gr.Button("Clear")
                promo_all_button = gr.Button("Select All")
                promo_button = gr.Button("Apply")

    # State
    init_config_state = gr.State(init_config)
    del_choices = gr.State(init_del_choices)
    promo_choices = gr.State(init_promo_choices)
    
    # Event: change ingestion group
    group.input(getConfigs, inputs=group, outputs=init_config_state) \
        .success(echoInput, inputs=init_config_state, outputs=config_table) \
        .success(getConfigsForPromotion, inputs=group, outputs=promo_table) \
        .success(getTableChoices, inputs=promo_table, outputs=promo_choices) \
        .success(updateDropdown, inputs=promo_choices, outputs=promo_dropdown)


    # Event: click on apply configs button
    config_button.click(saveConfigs, inputs=[init_config_state, config_table]) \
        .success(getConfigs, inputs=group, outputs=init_config_state) \
        .success(getConfigsForPromotion, inputs=group, outputs=promo_table) \
        .success(getTableChoices, inputs=promo_table, outputs=promo_choices) \
        .success(updateDropdown, inputs=promo_choices, outputs=promo_dropdown)

    # Event: click on del clear button
    del_clear_button.click(lambda: gr.Dropdown(value=[]), outputs=del_dropdown)

    # Event: click on del select all button
    del_all_button.click(echoInput, inputs=del_choices, outputs=del_dropdown)

    #TBD: Event: click on del apply button
    
    # Event: click on promo clear button
    promo_clear_button.click(lambda: gr.Dropdown(value=[]), outputs=promo_dropdown)

    # Event: click on promo select all button
    promo_all_button.click(echoInput, inputs=promo_choices, outputs=promo_dropdown)

    # Event: click on promo apply button
    promo_button.click(savePromotions, inputs=[group, promo_dropdown]) \
        .success(getConfigsForPromotion, inputs=group, outputs=promo_table) \
        .success(getTableChoices, inputs=promo_table, outputs=promo_choices) \
        .success(updateDropdown, inputs=promo_choices, outputs=promo_dropdown)

if __name__ == "__main__":
    app.launch(debug=True, favicon_path="img/favicon.ico")