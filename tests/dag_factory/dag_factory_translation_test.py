import sys

from orbiter_translations.dag_factory.yaml_base import translation_ruleset


def test_dag_factory_translation(change_test_dir):
    sys.path += [".", "workflow"]
    actual = translation_ruleset.translate_fn(translation_ruleset, "workflow/")
    assert list(actual.dags.keys()) == [
        "example_dag",
        "example_dag2",
        "example_dag3",
        "example_dag4",
        "test_expand",
    ]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["task_1", "task_2", "task_3"]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["task_1"].downstream)
    ) == sorted(["task_2", "task_3"])
