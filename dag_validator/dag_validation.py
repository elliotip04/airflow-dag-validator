import re
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Optional, Union


class AirflowDAGValidation:
    def __init__(self, dag_folder_path: str):
        self.dag_folder_path = Path(dag_folder_path)
        self.layer_functions = {
            "staging_to_source": self.generate_staging_to_source_upstream_dependencies,
            "landing_to_staging": self.generate_landing_to_staging_upstream_dependencies,
            # Add more layers and functions as needed
        }

    def parse_all_dependencies(self, dep):
        """
        Generate dependencies based on the conditions
        # i.e.
        Input: a >> [b, c] >> e
        Expected Output:
        a >> b
        a >> c
        b >> e
        c >> e
        """
        # Split the string by ' >> '
        parts = [s.strip() for s in dep.split(">>")]

        # List to store generated dependencies
        dependencies = []

        for i in range(len(parts) - 1):
            current_part = parts[i].strip()
            next_part = parts[i + 1].strip()

            # Check if either part is a list
            is_current_list = "[" in current_part
            is_next_list = "[" in next_part

            # ie. a >> [b, c]
            if not is_current_list and is_next_list:
                base_dependency = current_part.strip()
                inner_parts = re.findall(r"\[([^\]]+)\]", next_part)

                if inner_parts:
                    inner_parts = [
                        inner_part.strip() for inner_part in inner_parts[0].split(",")
                    ]
                    final_dependency = next_part

                    dependencies.extend(
                        [
                            f"\n    {base_dependency} >> {inner_part}"
                            for inner_part in inner_parts
                        ]
                    )

            # ie. a >> b
            elif not is_current_list and not is_next_list:
                dependencies.append(f"\n    {current_part} >> {next_part}")

            # ie. [a, b] >> [c, d]
            elif is_current_list and is_next_list:
                current_inner_parts = re.findall(r"\[([^\]]+)\]", current_part)
                next_inner_parts = re.findall(r"\[([^\]]+)\]", next_part)
                if current_inner_parts and next_inner_parts:
                    current_inner_parts = [
                        inner_part.strip()
                        for inner_part in current_inner_parts[0].split(",")
                    ]
                    next_inner_parts = [
                        inner_part.strip()
                        for inner_part in next_inner_parts[0].split(",")
                    ]

                    dependencies.extend(
                        [
                            f"\n    {current_inner} >> {next_inner}"
                            for current_inner in current_inner_parts
                            for next_inner in next_inner_parts
                        ]
                    )

            # ie. [a, b] >> c
            elif is_current_list and not is_next_list:
                current_inner_parts = re.findall(r"\[([^\]]+)\]", current_part)
                if current_inner_parts:
                    current_inner_parts = [
                        inner_part.strip()
                        for inner_part in current_inner_parts[0].split(",")
                    ]
                    final_dependency = next_part

                    dependencies.extend(
                        [
                            f"\n    {current_inner} >> {final_dependency}"
                            for current_inner in current_inner_parts
                        ]
                    )

        return dependencies

    def filter_dag_dependencies_from_file(self, file_path: str) -> str:
        try:
            with open(file_path, "r") as file:
                dag_string = file.read()

            # Define a regular expression pattern to match lines with '>>'
            pattern = re.compile(r"\s*.*>>.*")

            # Use the findall method to extract all matching lines
            dependencies = pattern.findall(dag_string)

            # List to store modified dependencies
            modified_dependencies = []

            for dep in dependencies:
                modified_dependencies.extend(self.parse_all_dependencies(dep))

            # Join the matching lines into a single string
            result_string = "".join(modified_dependencies)
            return result_string
        except FileNotFoundError:
            return f"Error: File not found - {file_path}"

    def _extract_dependencies_by_regex(
        self, dependencies: str, regex_pattern: re.Pattern
    ) -> List[str]:
        dependencies = regex_pattern.findall(dependencies)
        stripped_dependencies = [string.strip() for string in dependencies]
        return stripped_dependencies

    def _extract_source_dependencies(self, dependencies: str) -> List[str]:
        regex_pattern = re.compile(r"^\s*(?:source_)\S*.*$", re.MULTILINE)
        stripped_dependencies = self._extract_dependencies_by_regex(
            dependencies, regex_pattern
        )
        return stripped_dependencies

    def _extract_staging_dependencies(self, dependencies: str) -> List[str]:
        regex_pattern = re.compile(r"^.*>>\s*staging_\S+.*$", re.MULTILINE)
        stripped_dependencies = self._extract_dependencies_by_regex(
            dependencies, regex_pattern
        )
        stripped_dependencies = self._extract_dependencies_by_regex(
            dependencies, regex_pattern
        )
        return stripped_dependencies

    def _extract_landing_dependencies(self, dependencies: str) -> List[str]:
        regex_pattern = re.compile(r"^.*>>\s*landing_\S+.*$", re.MULTILINE)
        stripped_dependencies = self._extract_dependencies_by_regex(
            dependencies, regex_pattern
        )
        stripped_dependencies = self._extract_dependencies_by_regex(
            dependencies, regex_pattern
        )
        return stripped_dependencies

    def _get_upstream_model(self, model: str, separator: str = " >> ") -> str:
        return model.split(separator)[-2]

    def _get_downstream_model(self, model: str, separator: str = " >> ") -> str:
        return model.split(separator)[-1]

    def _join_parts(self, parts: List[str], separator: str = "_") -> str:
        return separator.join(parts)

    def _recursive_result_processing(
        self,
        base_model: str,
        dependency: str,
        layer_dependencies_map: Dict[str, List[str]],
        map_dict: Dict[str, set],
        from_layer: str,
        to_layer: str,
        output_layer: str,
    ) -> None:

        mapped_layer_dependencies = layer_dependencies_map[from_layer]
        result = list(
            filter(lambda x: x.endswith(f">> {dependency}"), mapped_layer_dependencies)
        )

        for upstream in result:
            while True:
                model_upstream = self._get_upstream_model(upstream)
                model_downstream = self._get_downstream_model(upstream)

                model_upstream_split = model_upstream.split("_")
                model_downstream_split = model_downstream.split("_")

                if model_upstream_split[0] == to_layer:

                    # 'output_layer': This is used to specify the layer you want to output
                    # as value in the dependency dictionary. Either upstream or downstream
                    # (e.g. raw_aln_dbo_c_udkey_18 >> cur_aln_dbo_c_udkey_18)
                    # In the vault-to-curated mapping, output cur_aln_dbo_c_udkey_18
                    # In the curated-to-raw mapping, output raw_aln_dbo_c_udkey_18
                    if model_upstream_split[0] == output_layer:
                        self._update_map_dict(map_dict, base_model, model_upstream)
                    elif model_downstream_split[0] == output_layer:
                        self._update_map_dict(map_dict, base_model, model_downstream)
                    break  # Exit the while loop when 'raw' is found

                model_upstream_parts = model_upstream.split("_")

                next_from_layer = (
                    model_upstream_parts[0]
                    if model_upstream_parts[0] != "trns"
                    else "_".join(model_upstream_parts[:2])
                )

                self._recursive_result_processing(
                    base_model,
                    model_upstream,
                    layer_dependencies_map,
                    map_dict,
                    next_from_layer,
                    to_layer,
                    output_layer,
                )  # Recursive call

                break  # Break the while loop as the recursive call handles further processing

    def generate_staging_to_source_upstream_dependencies(self, dependencies: str) -> Dict[str, set]:
        """
        Create a dictionary with upstream dependencies from 'staging' to 'source'
        """
        stripped_staging_dependencies = self._extract_staging_dependencies(dependencies)

        LAYER_DEPENDENCIES_MAP = {
            "staging": stripped_staging_dependencies,
        }

        STAGING_SOURCE_MAP = {}
        filtered_staging_dependencies = [
            item for item in stripped_staging_dependencies if ">> staging_" in item
        ]  # only include 'staging_*' model as base model
        for model in filtered_staging_dependencies:
            model_staging = self._get_downstream_model(model)
            STAGING_SOURCE_MAP[model_staging] = set()

            # Create a list of all upstream dependencies
            result = list(
                filter(
                    lambda x: x.endswith(f">> {model_staging}"),
                    stripped_staging_dependencies,
                )
            )

            # Check if result is empty, raise 'no upstream task' error
            if len(result) == 0:
                raise ValueError(
                    f"There is no upstream task found for '{model_staging}'. Please review DAG dependencies."
                )

            # Update the dictionary key (curated model) with the corresponding raw model
            for upstream in result:
                model_upstream = self._get_upstream_model(upstream)
                model_upstream_split = model_upstream.split("_")

                if (
                    model_upstream_split[0] == "source"
                ):  # (e.g. 'source_task_a1 >> staging_task_a2')
                    self._update_map_dict(
                        STAGING_SOURCE_MAP, model_staging, model_upstream
                    )

                # e.g. 'staging_task_b3 >> staging_task_b4'
                else:
                    self._recursive_result_processing(
                        model_staging,
                        model_upstream,
                        LAYER_DEPENDENCIES_MAP,
                        STAGING_SOURCE_MAP,
                        from_layer="staging",
                        to_layer="source",
                        output_layer="source",
                    )

        print(STAGING_SOURCE_MAP)
        return STAGING_SOURCE_MAP


    def generate_landing_to_staging_upstream_dependencies(self, dependencies: str) -> Dict[str, set]:
        """
        Create a dictionary with upstream dependencies from 'landing' to 'staging'
        """
        stripped_landing_dependencies = self._extract_landing_dependencies(dependencies)

        LAYER_DEPENDENCIES_MAP = {
            "landing": stripped_landing_dependencies,
        }

        LANDING_STAGING_MAP = {}
        filtered_landing_dependencies = [
            item for item in stripped_landing_dependencies if ">> landing_" in item
        ]  # only include 'landing_*' model as base model
        for model in filtered_landing_dependencies:
            model_landing = self._get_downstream_model(model)
            LANDING_STAGING_MAP[model_landing] = set()

            # Create a list of all upstream dependencies
            result = list(
                filter(
                    lambda x: x.endswith(f">> {model_landing}"),
                    stripped_landing_dependencies,
                )
            )

            # Check if result is empty, raise 'no upstream task' error
            if len(result) == 0:
                raise ValueError(
                    f"There is no upstream task found for '{model_landing}'. Please review DAG dependencies."
                )

            # Update the dictionary key (curated model) with the corresponding raw model
            for upstream in result:
                model_upstream = self._get_upstream_model(upstream)
                model_upstream_split = model_upstream.split("_")

                if (
                    model_upstream_split[0] == "staging"
                ):  # (e.g. 'staging_task_b3 >> landing_task_b4')
                    self._update_map_dict(
                        LANDING_STAGING_MAP, model_landing, model_upstream
                    )

                # e.g. [landing_task_a3, landing_task_b5] >> landing_task_c1
                else:
                    self._recursive_result_processing(
                        model_landing,
                        model_upstream,
                        LAYER_DEPENDENCIES_MAP,
                        LANDING_STAGING_MAP,
                        from_layer="landing",
                        to_layer="staging",
                        output_layer="staging",
                    )

        print(LANDING_STAGING_MAP)
        return LANDING_STAGING_MAP

    def _update_map_dict(
        self, map_dict: Dict[str, set], base_model: str, model: str
    ) -> None:
        map_dict[base_model].add(model)

    def find_orphaned_models(self, input_dict: Dict[str, set]) -> List[str]:
        orphaned_keys = [key for key, value in input_dict.items() if not value]
        return orphaned_keys

    def process_dag_folder(
        self,
    ) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, Dict[str, set]]]:
        """
        Process all DAG files in a folder for different layers.
        """
        # Dictionary to store orphaned model (if any) and dependencies for each DAG and layer
        results = {layer: {} for layer in self.layer_functions.keys()}
        layer_dependencies = {layer: {} for layer in self.layer_functions.keys()}

        # Check if there are any .py files in the folder
        py_files = list(self.dag_folder_path.glob("*.py"))
        if not py_files:
            raise ValueError(
                f"{self.dag_folder_path} is empty. There is no valid DAG to validate."
            )

        for dag_path in py_files:
            dag_name = dag_path.name
            for layer, layer_function in self.layer_functions.items():
                result, layer_map = self.process_single_dag(
                    dag_path, layer, layer_function
                )
                results[layer][dag_name] = result
                layer_dependencies[layer][dag_name] = layer_map

        # print(results, layer_dependencies)
        return results, layer_dependencies

    def process_single_dag(
        self, dag_path: Path, layer: str, layer_function: Callable
    ) -> Tuple[List[str], Dict[str, set]]:
        """
        Process a single DAG file for a specific layer.
        """
        dag = dag_path.name
        print(f"\nValidating '{dag}': {layer}")

        all_dependencies = self.filter_dag_dependencies_from_file(dag_path)
        layer_dependencies = layer_function(all_dependencies)

        result = self.find_orphaned_models(layer_dependencies)
        print(f"*** Orphaned Models found in {dag}: {result}")

        return result, layer_dependencies

    def _generate_pairs(
        self,
        layer_dependencies: Dict[str, Dict[str, Dict[str, str]]],
        key_type: str,
    ) -> List:
        return (
            (dag, key1, key2)
            for dag, dag_dependencies in layer_dependencies.get(key_type, {}).items()
            for key1, keys2 in dag_dependencies.items()
            for key2 in keys2
        )

    def _curated_raw_pairs_generator(
        self, layer_dependencies: Dict[str, Dict[str, Dict[str, str]]]
    ) -> List:
        return self._generate_pairs(layer_dependencies, "curated_to_raw")

    def _raw_glue_pairs_generator(
        self, layer_dependencies: Dict[str, Dict[str, Dict[str, str]]]
    ) -> List:
        return self._generate_pairs(layer_dependencies, "raw_to_glue")

    def _find_unmatched_models(
        self,
        dag: str,
        key1: str,
        key2: str,
        split_key1: str,
        split_key2: str,
        expected_non_matching_models: Dict[
            str, Union[Dict[str, List[str]], Optional[str]]
        ],
    ) -> str:
        result = "Match found" if split_key1 in split_key2 else "No match"

        # Check against the expected non-matching pairs only when there is no match
        if result == "No match":
            expected_non_matching = expected_non_matching_models.get(dag, {}).get(
                key1, []
            )
            assert (
                key2 in expected_non_matching
            ), f"Assertion failed: {key2} and {key1} is not a correct match"

            result = "Match found" if key2 in expected_non_matching else "No match"
        return result

# test = AirflowDAGValidation("dags/")
# print(test._extract_landing_dependencies(test.filter_dag_dependencies_from_file("dags/example_dag.py")))
# test.process_dag_folder()