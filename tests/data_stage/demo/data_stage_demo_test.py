from orbiter_translations.data_stage.xml_demo import translation_ruleset


def test_data_stage_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/data_stage/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["data_stage_job"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["peek_sf_ld", "select_table"]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["select_table"].downstream)
    ) == ["peek_sf_ld"]
