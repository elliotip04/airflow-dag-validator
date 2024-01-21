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
