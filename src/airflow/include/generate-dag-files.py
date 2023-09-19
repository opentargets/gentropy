"""Generate DAG files from templates."""
# import fileinput
# import json
# import os
# import shutil

from __future__ import annotations

# import jinja2
# config_filepath = "include/dag-config/"
# dag_template_filename = "include/dag-template.py"

# environment = jinja2.Environment(
#     loader=jinja2.FileSystemLoader(searchpath="src/airflow/include/templates/")
# )
# template = environment.get_template("test.txt.j2")

# template.render(name="John Doe")

# # for filename in os.listdir(config_filepath):
# #     f = open(config_filepath + filename)
# #     config = json.load(f)

# #     new_filename = "dags/" + config["dag_id"] + ".py"
# #     shutil.copyfile(dag_template_filename, new_filename)

# #     for line in fileinput.input(new_filename, inplace=True):
# #         line = line.replace("dag_id_to_replace", "'" + config["dag_id"] + "'")
# #         line = line.replace("schedule_to_replace", config["schedule"])
# #         line = line.replace("bash_command_to_replace", config["bash_command"])
# #         line = line.replace("env_var_to_replace", config["env_var"])
# #         print(line, end="")
