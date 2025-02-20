from orbiter_translations.kestra.yaml_demo import translation_ruleset


def test_kestra_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/kestra/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["company.team.getting_started"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["dag", "hello_world"]
    )

    assert sorted(actual.dags['company.team.getting_started'].tasks['dag'].tasks.keys()) == ['python', 'task1', 'task2']
