import json
import os
import shutil
from pathlib import Path

import typer
import yaml

from dbt_af.builder.dbt_model_path_graph_builder import DbtModelPathGraph

cli = typer.Typer()


@cli.command()
def generate_mini_dbt_projects(manifest_path: str, dbt_project_path: str, result_path: str):
    generate_mini_dbt_template(dbt_project_path, result_path, 'mini_dbt_template')

    with open(manifest_path) as fin:
        manifest = json.load(fin)

    graph = DbtModelPathGraph.from_manifest(manifest)

    for node in graph.dbt_nodes:
        # copy template

        shutil.copytree(
            Path(result_path, 'mini_dbt_template'),
            Path(result_path, node.resource_name),
        )

        # create list of dirs we should copy for model
        list_of_directories = []

        # model path
        list_of_directories.append(graph.node_to_path[node.unique_id])
        # model deps pathes
        list_of_directories.extend(graph.full_path_to_path[graph.node_to_path[node.unique_id]])

        for path in list_of_directories:
            from_path = Path(dbt_project_path, path)
            to_path = Path(result_path, node.resource_name, path)

            # create destination dir
            to_path.mkdir(parents=True, exist_ok=True)

            # copy all sql
            for file in from_path.glob('*.sql'):
                shutil.copy(file, to_path)

            # copy all py
            for file in from_path.glob('*.py'):
                shutil.copy(file, to_path)

            # copy all yml
            for file in from_path.glob('*.yml'):
                shutil.copy(file, to_path)

    return graph


def generate_mini_dbt_template(dbt_project_path, result_path, mini_dbt_node_template):
    with open(Path(dbt_project_path, 'dbt_project.yml')) as fin:
        dbt_project = yaml.safe_load(fin)

    Path(result_path, mini_dbt_node_template).mkdir(parents=True, exist_ok=True)
    # copy core files
    shutil.copyfile(
        Path(dbt_project_path, 'dbt_project.yml'),
        Path(result_path, mini_dbt_node_template, 'dbt_project.yml'),
    )
    shutil.copyfile(
        Path(dbt_project_path, 'packages.yml'),
        Path(result_path, mini_dbt_node_template, 'packages.yml'),
    )

    # copy any md file into one md per folder
    for path in dbt_project['model-paths']:
        from_path = Path(dbt_project_path, path)
        to_path = Path(result_path, mini_dbt_node_template, path)

        to_path.mkdir(parents=True, exist_ok=True)
        md_pathlist = from_path.glob('**/*.md')
        with open(Path(to_path, 'doc.md'), 'w') as outfile:
            for md_path in md_pathlist:
                with open(md_path) as infile:
                    for line in infile:
                        outfile.write(line)

        for yml_file in from_path.glob('**/*.yml'):
            file_to_path = Path(str(yml_file).replace(str(from_path), str(to_path)))
            with open(yml_file) as fin:
                some_yml = yaml.safe_load(fin)
                if 'groups' in some_yml:
                    os.makedirs(os.path.dirname(file_to_path), exist_ok=True)
                    shutil.copy(yml_file, file_to_path)

    # all macro
    for path in dbt_project['macro-paths']:
        from_path = Path(dbt_project_path, path)
        to_path = Path(result_path, mini_dbt_node_template, path)
        if os.path.exists(to_path):
            shutil.rmtree(to_path)
        shutil.copytree(from_path, to_path)


if __name__ == '__main__':
    cli()
