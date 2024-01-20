def test_find_all_upstream_orphaned_models(airflow_dag_validator, expected_orphaned_all_models_with_no_upstream):
    """
    Test method to find unexpected orphaned models with no upstream across all layers

    Pass Case (i.e. 'staging' is expecting upstream 'source' task):
    e.g. 'staging_task_a2': {'source_task_a1'}

    Failed Case (i.e. empty value, no upstream model found): 
    e.g. 'staging_task_a2': {}
    """
    validator = airflow_dag_validator  # Use the fixture instance
    results, layer_dependencies = validator.process_dag_folder()

    # Find orphaned models
    print("\nStarted test: Find unexpected upstream orphaned models across all layers")

    for layer, dag_group in results.items():
        for dag, orphaned_models in dag_group.items():
            expected_orphaned = expected_orphaned_all_models_with_no_upstream.get(layer, {}).get(dag, [])
            unexpected_orphaned_models = set(orphaned_models) - set(expected_orphaned)
            assert (
                not unexpected_orphaned_models
            ), f"Unexpected orphaned models for DAG {dag}, Layer {layer}: {unexpected_orphaned_models}"

    print("\nFinished test: No unexpected orphaned models found across all layers\n")


def test_find_all_incorrect_mapped_upstream_layer(
    airflow_dag_validator, expected_prefix_mapping
):
    """
    Test method to find incorrectly mapped upstream layers across all layers

    Pass Case:
    e.g. 'landing_task_c1': {'staging_task_b4', 'staging_task_a2'}

    Failed Case (i.e. 'landing' mapped to 'source' instead of 'staging'): 
    e.g. 'landing_task_c1': {'source_task_a1', 'source_task_b1'}
    """
    validator = airflow_dag_validator
    results, layer_dependencies = validator.process_dag_folder()

    # Find incorrect layer mapping in dependencies
    print(
        "\nStarted test: Find incorrect upstream layer mapping in the dependencies generated across all layers"
    )

    for key_type, dependencies in layer_dependencies.items():
        for dag, dependency_map in dependencies.items():
            for key, value in dependency_map.items():
                key_parts = key.split("_")

                # Check if the set is not empty before extracting the first element
                if value:
                    for value_str in value:
                        value_parts = value_str.split("_")
                        expected_value_prefix = expected_prefix_mapping.get(
                            key_parts[0], []
                        )

                    # Print information for debugging
                    print(
                        f"*** Key: {key}, Expected Value Prefix: {expected_value_prefix}, Actual Value Prefix: {value_parts[0]}"
                    )

                    # Perform the check
                    assert value_str.startswith(
                        expected_value_prefix
                    ), f"Unexpected value prefix for {value_str}: {expected_value_prefix}"

                else:
                    # Skip the assertion if the set is empty
                    continue

    print("\nFinised test: No incorrect layer mapping found\n")


# def test_find_raw_to_glue_one_to_one_incorrect_mapped_upstream_model(
#     airflow_dag_validator, expected_non_matching_raw_glue_models
# ):
#     """
#     Test method to find unexpected upstream 1:1 mapping from 'raw' to 'glue'
    
#     Pass Case (i.e. c_udkey_5 correctly mapped):
#     e.g. {raw_aln_dbo_c_udkey_5': {'glue_c_udkey_5'}

#     Failed Case: 
#     e.g. {raw_aln_dbo_c_udkey_5': {'glue_c_udkey_2'}
#     """
#     print("\nStarted test: Find unexpected 1:1 mapping from 'raw' to 'glue'\n")
#     validator = airflow_dag_validator

#     results, layer_dependencies = validator.process_dag_folder()
#     raw_glue_pairs = validator._raw_glue_pairs_generator(layer_dependencies)

#     for dag, raw_key, glue_key in raw_glue_pairs:
#         raw_key_split = raw_key.split("_")
#         glue_key_split = glue_key.split("_")

#         if glue_key != "stream_validation":
#             if dag.split("_")[0] == "Repertoire":
#                 raw_parts = validator._join_parts(raw_key_split[2:])
#             else:
#                 raw_parts = validator._join_parts(raw_key_split[3:])

#             glue_parts = validator._join_parts(glue_key_split[1:])

#             result = validator._find_unmatched_models(
#                 dag,
#                 raw_key,
#                 glue_key,
#                 raw_parts,
#                 glue_parts,
#                 expected_non_matching_raw_glue_models,
#             )
#             print(f"*** {result}: {raw_key} and {glue_key}")
#             assert (
#                 result == "Match found"
#             ), f"Unexpected result for {raw_key} and {glue_key}"

#     print("\nFinished test: All raw models and glue jobs mapped correctly\n")


# def test_find_curated_to_raw_one_to_one_incorrect_mapped_upstream_model(
#     airflow_dag_validator, expected_non_matching_curated_raw_models
# ):
#     """
#     Test method to find unexpected upstream 1:1 mapping from 'curated' to 'raw'

#     Pass Case (i.e. c_udkey_9 correctly mapped):
#     e.g. {cur_aln_dbo_c_udkey_9': {'raw_aln_dbo_c_udkey_9'}

#     Failed Case (i.e. c_udkey_9 incorrectly mapped to c_udkey_3):
#     e.g. {cur_aln_dbo_c_udkey_9': {'raw_aln_dbo_c_udkey_3'}
#     """
#     print("\nStarted test: Find unexpected 1:1 mapping from 'curated' to 'raw'\n")

#     layer_dependencies = airflow_dag_validator.process_dag_folder()[1]

#     curated_raw_pairs = airflow_dag_validator._curated_raw_pairs_generator(
#         layer_dependencies
#     )

#     for dag, curated_key, raw_key in curated_raw_pairs:
#         curated_key_split = curated_key.split("_")
#         raw_key_split = raw_key.split("_")

#         # Rest of your logic for curated to raw mapping test
#         if dag.split("_")[0] == "Repertoire":
#             curated_parts = airflow_dag_validator._join_parts(curated_key_split[2:])
#         else:
#             curated_parts = airflow_dag_validator._join_parts(curated_key_split[1:])
#             raw_parts = airflow_dag_validator._join_parts(raw_key_split[1:])

#             result = airflow_dag_validator._find_unmatched_models(
#                 dag,
#                 curated_key,
#                 raw_key,
#                 curated_parts,
#                 raw_parts,
#                 expected_non_matching_curated_raw_models,
#             )
#             print(f"*** {result}: {curated_key} and {raw_key}")

#     print("\nFinished test: All curated models and raw models mapped correctly\n")


# def test_find_curated_to_vault_downstream_orphaned_models(
#     airflow_dag_validator, expected_orphaned_curated_models_with_no_downstream
# ):
#     """
#     Test method to find unexpected orphaned curated models with no downstream vault model

#     Pass Case:
#     - Every curated model should have at least 1 downstream vault model (apart from the expected ones)

#     Failed Case:
#     - Unexpected curated model with no downstream vault model
#     """
#     print("\nStarted test: Find unexpected orphaned curated models with no downstream vault model\n")

#     validator = airflow_dag_validator
#     results, layer_dependencies = validator.process_dag_folder()

#     # Identify curated models that are upstream of any vault model for each DAG
#     MODELS_WITH_DOWNSTREAM_MAP = {}

#     for layer, content in layer_dependencies.items():
#         if layer == "vault_to_curated":
#             for dag, dependency in content.items():
#                 MODELS_WITH_DOWNSTREAM_MAP[dag] = set()

#                 for model_downstream, model_upstream in dependency.items():
#                     for model in model_upstream:
#                         MODELS_WITH_DOWNSTREAM_MAP[dag].add(model)

#     # Identify a set of all curated models for each DAG (whether or not they have any downstream model)
#     dag_files = list(validator.dag_folder_path.glob("*.py"))

#     ALL_CURATED_MODELS = {}

#     for dag_path in dag_files:

#         dag = dag_path.name

#         all_dependencies = validator.filter_dag_dependencies_from_file(dag_path)
#         stripped_curated_dependencies = validator._extract_curated_dependencies(
#             all_dependencies
#         )

#         ALL_CURATED_MODELS[dag] = set()

#         for item in stripped_curated_dependencies:
#             if ">> cur_" in item:
#                 model_downstream = validator._get_downstream_model(item)
#                 model_upstream = validator._get_upstream_model(item)

#                 if model_upstream.split("_")[0] == "cur":
#                     model_cur = model_upstream
#                     ALL_CURATED_MODELS[dag].add(model_cur)

#                 elif model_upstream.split("_")[0] == "raw":
#                     model_cur = model_downstream
#                     ALL_CURATED_MODELS[dag].add(model_cur)

#         # Use set difference to find unexpected models
#         unexpected_models = ALL_CURATED_MODELS[dag] - MODELS_WITH_DOWNSTREAM_MAP[dag]

#         # Use the expected_non_matching_curated_raw_models fixture for assertions
#         expected_models = expected_orphaned_curated_models_with_no_downstream.get(
#             dag, set()
#         )
#         assert (
#             unexpected_models == expected_models
#         ), f"Unexpected orphaned curated model with no downstream for {dag}: {unexpected_models}"

#     print("\nFinished test: No unexpected orphaned curated models with no downstream vault model found\n")
