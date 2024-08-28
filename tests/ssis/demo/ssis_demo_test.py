from orbiter_translations.ssis.xml_demo import translation_ruleset


def test_ssis_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/ssis/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["demo.extract_sample_currency_data"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "extract_sample_currency_data",
            "lookup_currency_key",
            "lookup_date_key",
            "sample_ole__db_destination",
        ]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["lookup_date_key"].downstream)
    ) == sorted(["sample_ole__db_destination"])
