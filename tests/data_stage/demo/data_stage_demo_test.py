from orbiter_translations.data_stage.xml_demo import translation_ruleset


def test_data_stage_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/data_stage/demo/workflow/")
    )

    assert list(actual.dags.keys()) == ["ph_insert_employee_dim"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "build",
            "test",
            "deploy",
        ]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["build"].downstream)
    ) == sorted(["test"])
