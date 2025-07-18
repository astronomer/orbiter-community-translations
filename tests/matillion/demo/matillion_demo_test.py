from orbiter_translations.matillion.matillion_demo import translation_ruleset

def test_matillion_demo(project_root):
    # Point to the folder containing the input YAML workflow
    input_dir = project_root / "tests/matillion/demo/workflow/"
    actual = translation_ruleset.translate_fn(translation_ruleset, input_dir)

    # Only one DAG should be generated, named 'matillion_pipeline'
    assert list(actual.dags.keys()) == ["matillion_pipeline"]

    dag = list(actual.dags.values())[0]
    task_names = sorted(list(dag.tasks.keys()))

    # All three tasks from the YAML should be present
    assert task_names == sorted([
        "start",
        "print_hello_world",
        "print_pipeline_finished",
    ])

    # Check the downstream dependencies for "print_hello_world"
    assert sorted(list(dag.tasks["print_hello_world"].downstream)) == ["print_pipeline_finished"]

    # The "start" task should have "print_hello_world" as downstream
    assert sorted(list(dag.tasks["start"].downstream)) == ["print_hello_world"]
