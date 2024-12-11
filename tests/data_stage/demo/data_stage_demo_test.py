from orbiter_translations.data_stage.xml_demo import translation_ruleset


def test_data_stage_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/data_stage/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["data_stage_job"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["jdbc_sf_insert_table", "unknown"]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["jdbc_sf_insert_table"].downstream)
    ) == ["unknown"]
